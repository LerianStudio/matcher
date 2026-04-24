// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package audit provides adapters for publishing configuration audit events.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
	"github.com/LerianStudio/matcher/internal/shared/adapters/outboxtelemetry"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ErrNilOutboxRepository is returned when the outbox repository is nil.
var ErrNilOutboxRepository = errors.New("outbox repository is required")

// auditConfigEntityType labels truncation metrics emitted from this
// publisher so operators can distinguish config-diff truncation from the
// exception-context publisher.
const auditConfigEntityType = "audit_config"

// OutboxPublisher publishes configuration audit events to the outbox for asynchronous processing.
type OutboxPublisher struct {
	outboxRepo sharedPorts.OutboxRepository
	now        func() time.Time
}

// PublisherOption configures NewOutboxPublisher.
type PublisherOption func(*OutboxPublisher)

// WithClock injects a time source for the envelope Timestamp. Intended for
// tests that need deterministic serialized envelope widths; production
// callers should omit this option and rely on the default time.Now().UTC().
func WithClock(now func() time.Time) PublisherOption {
	return func(pub *OutboxPublisher) {
		pub.now = now
	}
}

// NewOutboxPublisher creates a new outbox-based audit publisher.
func NewOutboxPublisher(repo sharedPorts.OutboxRepository, opts ...PublisherOption) (*OutboxPublisher, error) {
	if repo == nil {
		return nil, ErrNilOutboxRepository
	}

	pub := &OutboxPublisher{
		outboxRepo: repo,
		now:        func() time.Time { return time.Now().UTC() },
	}

	for _, opt := range opts {
		if opt != nil {
			opt(pub)
		}
	}

	return pub, nil
}

// Publish enqueues a configuration audit event on the outbox.
//
// The publisher uses a single-marshal gating strategy: the complete
// AuditLogCreatedEvent is serialized once, and only if the resulting
// payload exceeds the broker's per-event cap does Publish swap Changes
// for a truncation marker and re-marshal. This replaces the prior
// measure-then-marshal approach (which marshaled Changes separately for
// a heuristic size check and then marshaled the envelope again) with a
// cheaper and more accurate gate that measures the exact bytes the broker
// will receive.
func (pub *OutboxPublisher) Publish(ctx context.Context, event ports.AuditEvent) error {
	if pub == nil || pub.outboxRepo == nil {
		return ErrNilOutboxRepository
	}

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return fmt.Errorf("parse tenant id: %w", err)
	}

	var actor *string
	if event.Actor != "" {
		actor = &event.Actor
	}

	auditEvent := sharedDomain.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   tenantID,
		EntityType: event.EntityType,
		EntityID:   event.EntityID,
		Action:     event.Action,
		Actor:      actor,
		Changes:    event.Changes,
		OccurredAt: event.OccurredAt,
		Timestamp:  pub.now(),
	}

	payload, err := marshalOrTruncate(ctx, &auditEvent)
	if err != nil {
		return err
	}

	outboxEvent, err := sharedDomain.NewOutboxEvent(
		ctx,
		sharedDomain.EventTypeAuditLogCreated,
		event.EntityID,
		payload,
	)
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	if _, err := pub.outboxRepo.Create(ctx, outboxEvent); err != nil {
		return fmt.Errorf("persist outbox event: %w", err)
	}

	return nil
}

var _ ports.AuditPublisher = (*OutboxPublisher)(nil)

// marshalOrTruncate serializes the audit event and, if the resulting
// payload exceeds the broker's cap, replaces Changes with a truncation
// marker and re-marshals. The final payload is returned or a marshal
// error wrapped for the caller.
//
// This is the single-marshal counterpart of the prior measure-first
// helper: it gates on the actual outgoing bytes rather than on a
// Changes-only heuristic.
func marshalOrTruncate(
	ctx context.Context,
	event *sharedDomain.AuditLogCreatedEvent,
) ([]byte, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal audit event: %w", err)
	}

	if len(payload) <= sharedDomain.DefaultOutboxMaxPayloadBytes {
		return payload, nil
	}

	originalSize := len(payload)

	outboxtelemetry.RecordAuditChangesTruncated(
		ctx,
		auditConfigEntityType,
		event.EntityID,
		originalSize,
		sharedDomain.DefaultOutboxMaxPayloadBytes,
	)

	event.Changes = sharedDomain.BuildAuditChangesTruncationMarker(
		originalSize,
		sharedDomain.DefaultOutboxMaxPayloadBytes,
	)

	payload, err = json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal truncated audit event: %w", err)
	}

	return payload, nil
}
