// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package audit provides adapters for consuming audit events.
package audit

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	streaming "github.com/LerianStudio/lib-streaming"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

// defaultDedupWindow is the default time window used to detect duplicate event deliveries.
// The outbox pattern may deliver the same event twice on retry; if an audit log
// with the same entity_type + entity_id + action exists within this window,
// the duplicate is silently skipped.
//
// TUNING: This value should be greater than the outbox dispatcher's retry interval
// (currently configured via IDEMPOTENCY_RETRY_WINDOW). If you change the retry
// configuration, review this constant. The fail-open design ensures that if the
// window is too short, the worst case is a duplicate audit entry (harmless for
// compliance) rather than a missed event.
const defaultDedupWindow = 5 * time.Second

const dedupScanLimit = 10

// Sentinel errors for audit consumer validation.
var (
	ErrNilAuditRepository        = errors.New("audit log repository is required")
	ErrAuditEventRequired        = errors.New("audit event is required")
	ErrNilInfrastructureProvider = errors.New("infrastructure provider is required")
	ErrAuditTenantMismatch       = errors.New("audit event tenant does not match dispatch context tenant")
	ErrAuditTenantContextMissing = errors.New("audit dispatch tenant missing from tenant-manager context")
)

// Consumer processes audit events and persists them to the audit log.
type Consumer struct {
	auditLogRepo  repositories.AuditLogRepository
	dedupWindow   time.Duration
	infraProvider sharedPorts.InfrastructureProvider
	streamEmitter streaming.Emitter
}

// ConsumerConfig controls consumer behavior.
type ConsumerConfig struct {
	DedupWindow      time.Duration
	Infrastructure   sharedPorts.InfrastructureProvider
	StreamingEmitter streaming.Emitter
}

// NewConsumer creates a new audit event consumer.
func NewConsumer(repo repositories.AuditLogRepository, cfgOpt ...ConsumerConfig) (*Consumer, error) {
	if repo == nil {
		return nil, ErrNilAuditRepository
	}

	cfg := ConsumerConfig{DedupWindow: defaultDedupWindow}
	if len(cfgOpt) > 0 {
		cfg = cfgOpt[0]
		if cfg.DedupWindow <= 0 {
			cfg.DedupWindow = defaultDedupWindow
		}
	}

	return &Consumer{
		auditLogRepo:  repo,
		dedupWindow:   cfg.DedupWindow,
		infraProvider: cfg.Infrastructure,
		streamEmitter: cfg.StreamingEmitter,
	}, nil
}

// PublishAuditLogCreated processes an audit log created event and persists it.
// This implements sharedDomain.AuditEventPublisher.
func (consumer *Consumer) PublishAuditLogCreated(
	ctx context.Context,
	event *sharedDomain.AuditLogCreatedEvent,
) error {
	if consumer == nil || consumer.auditLogRepo == nil {
		return ErrNilAuditRepository
	}

	if event == nil {
		return ErrAuditEventRequired
	}

	var err error

	ctx, err = auditTenantContext(ctx, event)
	if err != nil {
		return err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("commons.noop")
	}

	ctx, span := tracer.Start(ctx, "governance.audit.publish_created")
	defer span.End()

	// Idempotency guard: the outbox pattern may deliver the same event twice on retry.
	// Check if an audit log with the same entity + action was recently created.
	// This uses a small time window to avoid false positives from legitimate rapid operations.
	if consumer.isDuplicateDelivery(ctx, logger, event) {
		logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("skipping duplicate audit event for entity %s/%s action=%s",
			event.EntityType, event.EntityID, event.Action))

		return nil
	}

	changes, err := buildChangesPayload(event)
	if err != nil {
		wrappedErr := fmt.Errorf("build changes payload: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to build changes payload", wrappedErr)

		return wrappedErr
	}

	auditLog, err := entities.NewAuditLog(
		ctx,
		event.TenantID,
		event.EntityType,
		event.EntityID,
		event.Action,
		event.Actor,
		changes,
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create audit log entity: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create audit log entity", wrappedErr)

		return wrappedErr
	}

	// Streaming-disabled fast-path: when the configured emitter is the
	// canonical NoopEmitter from lib-streaming (or interface-nil),
	// streaming emission is a documented soft-disable. Skip the
	// BeginTx → CreateWithTx → Emit → Commit dance and persist via the
	// repository's autocommit Create — pre-streaming semantics. This
	// removes 2 round-trips per audit log on the disabled hot path
	// without changing the persisted entity. Compliance posture is
	// preserved: every audit row still lands; only the broker emission
	// (which would no-op anyway) is skipped along with its tx envelope.
	if emission.IsNilEmitter(consumer.streamEmitter) || isNoopEmitter(consumer.streamEmitter) {
		return consumer.persistAuditLogAutocommit(ctx, span, auditLog)
	}

	if consumer.infraProvider == nil {
		return fmt.Errorf("audit streaming transaction: %w", ErrNilInfrastructureProvider)
	}

	return consumer.createAuditLogAndEmit(ctx, span, auditLog)
}

// persistAuditLogAutocommit persists an audit log using the repository's
// autocommit Create, used on the streaming-disabled fast-path. Extracted from
// PublishAuditLogCreated to keep its cyclomatic complexity within the cyclop
// threshold while preserving identical error-wrapping semantics.
func (consumer *Consumer) persistAuditLogAutocommit(ctx context.Context, span trace.Span, auditLog *entities.AuditLog) error {
	if _, err := consumer.auditLogRepo.Create(ctx, auditLog); err != nil {
		wrappedErr := fmt.Errorf("persist audit log (streaming disabled): %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to persist audit log without streaming", wrappedErr)

		return wrappedErr
	}

	return nil
}

// isNoopEmitter reports whether emitter is the canonical
// lib-streaming NoopEmitter — the value returned by
// streaming.NewEmitterWithCatalog when STREAMING_ENABLED=false. The
// type is exported as streaming.NoopEmitter, so a direct type
// assertion is safe and cheap (no reflection). Combined with
// emission.IsNilEmitter, this covers both flavours of "streaming is
// off": interface-nil and typed-noop.
func isNoopEmitter(emitter streaming.Emitter) bool {
	if emitter == nil {
		return false
	}

	_, ok := emitter.(*streaming.NoopEmitter)

	return ok
}

func auditTenantContext(ctx context.Context, event *sharedDomain.AuditLogCreatedEvent) (context.Context, error) {
	// Defensive symmetry. PublishAuditLogCreated already short-circuits on
	// nil event, but auditTenantContext is package-private and reachable
	// from future callers (tests, fixtures, other consumers). A nil event
	// would dereference event.TenantID below and panic.
	if event == nil {
		return ctx, ErrAuditEventRequired
	}

	eventTenantID := event.TenantID.String()

	ctxTenantID := tmcore.GetTenantIDContext(ctx)
	if ctxTenantID == "" {
		return ctx, fmt.Errorf("%w: %w", ErrAuditTenantContextMissing, emission.ErrTenantIDMissing)
	}

	if ctxTenantID != eventTenantID {
		return ctx, fmt.Errorf("%w: context=%s payload=%s", ErrAuditTenantMismatch, ctxTenantID, eventTenantID)
	}

	return tmcore.ContextWithTenantID(context.WithValue(ctx, auth.TenantIDKey, ctxTenantID), ctxTenantID), nil
}

func (consumer *Consumer) createAuditLogAndEmit(ctx context.Context, span trace.Span, auditLog *entities.AuditLog) error {
	txLease, err := consumer.infraProvider.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin audit log streaming transaction: %w", err)
	}

	if txLease == nil || txLease.SQLTx() == nil {
		return fmt.Errorf("begin audit log streaming transaction: %w", emission.ErrCriticalOutboxTxRequired)
	}

	defer func() { _ = txLease.Rollback() }()

	created, err := consumer.auditLogRepo.CreateWithTx(ctx, txLease.SQLTx(), auditLog)
	if err != nil {
		return fmt.Errorf("persist audit log with tx: %w", err)
	}

	if err := consumer.emitAuditLogCreated(ctx, span, txLease.SQLTx(), created); err != nil {
		return fmt.Errorf("emit audit log created: %w", err)
	}

	if err := txLease.Commit(); err != nil {
		return fmt.Errorf("commit audit log streaming transaction: %w", err)
	}

	return nil
}

func (consumer *Consumer) emitAuditLogCreated(ctx context.Context, span trace.Span, tx *sql.Tx, auditLog *entities.AuditLog) error {
	if auditLog == nil {
		return nil
	}

	options := []emission.Option{emission.RequireOutboxTx()}
	if tx != nil {
		options = append(options, emission.WithOutboxTx(tx))
	}

	payload := map[string]any{
		"audit_log_id": auditLog.ID.String(),
		"tenant_id":    auditLog.TenantID.String(),
		"entity_type":  auditLog.EntityType,
		"entity_id":    auditLog.EntityID.String(),
		"action":       auditLog.Action,
		"tenant_seq":   auditLog.TenantSeq,
		"hash_version": auditLog.HashVersion,
		"record_hash":  hex.EncodeToString(auditLog.RecordHash),
		"created_at":   auditLog.CreatedAt.UTC().Format(time.RFC3339Nano),
	}

	if err := emission.Emit(ctx, consumer.streamEmitter, "audit_log.created", auditLog.ID.String(), payload, options...); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event audit_log.created", err)
		return fmt.Errorf("emit audit log streaming event: %w", err)
	}

	return nil
}

// isDuplicateDelivery checks whether a matching audit log was recently persisted.
// It queries the most recent audit log for the same entity and compares the action
// and timestamp. If the latest entry matches within the dedup window, the event is
// considered a duplicate delivery and should be skipped.
//
// Errors from the query are intentionally swallowed — a failed dedup check must not
// prevent a legitimate audit event from being persisted.
func (consumer *Consumer) isDuplicateDelivery(
	ctx context.Context,
	logger libLog.Logger,
	event *sharedDomain.AuditLogCreatedEvent,
) bool {
	recent, _, err := consumer.auditLogRepo.ListByEntity(
		ctx, event.EntityType, event.EntityID, nil, dedupScanLimit,
	)
	if err != nil {
		if logger != nil {
			logger.Log(
				ctx,
				libLog.LevelWarn,
				fmt.Sprintf("dedup check failed for entity %s/%s action=%s: %v", event.EntityType, event.EntityID, event.Action, err),
			)
		}

		return false
	}

	if len(recent) == 0 {
		return false
	}

	now := time.Now().UTC()

	for _, candidate := range recent {
		if candidate == nil {
			continue
		}

		if now.Sub(candidate.CreatedAt) >= consumer.dedupWindow {
			// Results are sorted descending by CreatedAt, so once we are outside
			// the dedup window all subsequent rows are also outside the window.
			break
		}

		if candidate.Action == event.Action {
			return true
		}
	}

	return false
}

func buildChangesPayload(event *sharedDomain.AuditLogCreatedEvent) ([]byte, error) {
	// Defensive symmetry. The caller (PublishAuditLogCreated) pre-validates
	// event != nil, but buildChangesPayload is package-private and could be
	// reused. A nil event would dereference EntityType / EntityID below.
	// Returning (nil, nil) instead of an error keeps the contract simple
	// for hypothetical callers: a nil event yields a nil payload, not a
	// hard failure that surfaces deep in marshalling.
	if event == nil {
		return nil, nil
	}

	payload := map[string]any{
		"entity_type": event.EntityType,
		"entity_id":   event.EntityID.String(),
		"action":      event.Action,
		"occurred_at": event.OccurredAt,
	}

	if event.Actor != nil && *event.Actor != "" {
		payload["actor"] = *event.Actor
	}

	if event.Changes != nil {
		payload["changes"] = event.Changes
	}

	return json.Marshal(payload)
}

var _ sharedDomain.AuditEventPublisher = (*Consumer)(nil)
