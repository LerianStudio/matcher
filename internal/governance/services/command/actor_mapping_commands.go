// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"
	"fmt"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	streaming "github.com/LerianStudio/lib-streaming/v2"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

// TODO(telemetry): governance/adapters/http/handlers.go — logSpanError uses HandleSpanError for
// business outcomes (badRequest, notFound, writeNotFound). Add logSpanBusinessEvent using
// HandleSpanBusinessErrorEvent and create business-aware variants for 400/404 responses.
// See reporting/adapters/http/handlers_export_job.go for the reference implementation.

// Sentinel errors for actor-mapping command operations.
var (
	ErrNilActorMappingRepository = entities.ErrNilActorMappingRepository
	ErrNilPersistedActorMapping  = errors.New("actor mapping repository returned nil mapping")
	ErrNilInfraProvider          = errors.New("infrastructure provider is required")
)

// ActorMappingUseCase handles command operations for actor mappings.
type ActorMappingUseCase struct {
	repo          repositories.ActorMappingRepository
	infraProvider sharedPorts.InfrastructureProvider
	streamEmitter streaming.Emitter
}

// Functional options for streaming.Emitter injection follow the convention:
// - Bare WithStreamingEmitter when this package owns one emitter consumer
// - With<ReceiverName>StreamingEmitter when multiple consumers coexist in the same package
//
// Multiple use cases coexist in this package (ActorMappingUseCase, plus
// partition/audit-log commands), so the receiver-prefixed form
// WithActorMappingStreamingEmitter avoids name collisions.

// ActorMappingOption configures optional actor mapping use-case dependencies.
type ActorMappingOption func(*ActorMappingUseCase)

// WithActorMappingInfrastructure sets the infrastructure provider used for transactional emissions.
func WithActorMappingInfrastructure(provider sharedPorts.InfrastructureProvider) ActorMappingOption {
	return func(uc *ActorMappingUseCase) {
		if provider != nil {
			uc.infraProvider = provider
		}
	}
}

// WithActorMappingStreamingEmitter sets the emitter used for actor mapping streaming events.
// Use emission.IsNilEmitter() to defend against typed-nil interface values
// (e.g., a (*MockEmitter)(nil) hiding behind a streaming.Emitter interface).
func WithActorMappingStreamingEmitter(emitter streaming.Emitter) ActorMappingOption {
	return func(uc *ActorMappingUseCase) {
		if !emission.IsNilEmitter(emitter) {
			uc.streamEmitter = emitter
		}
	}
}

// NewActorMappingUseCase creates a new actor mapping command use case.
func NewActorMappingUseCase(repo repositories.ActorMappingRepository, options ...ActorMappingOption) (*ActorMappingUseCase, error) {
	if repo == nil {
		return nil, ErrNilActorMappingRepository
	}

	uc := &ActorMappingUseCase{repo: repo}

	for _, option := range options {
		if option != nil {
			option(uc)
		}
	}

	return uc, nil
}

// UpsertActorMapping creates or updates an actor mapping.
// Returns the persisted entity (including DB-generated timestamps) so the handler
// can respond without a separate read query, avoiding read-replica lag issues.
func (uc *ActorMappingUseCase) UpsertActorMapping(ctx context.Context, actorID string, displayName, email *string) (*entities.ActorMapping, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "service.governance.upsert_actor_mapping")

	defer span.End()

	mapping, err := entities.NewActorMapping(ctx, actorID, displayName, email)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid actor mapping input", err)

		libLog.SafeError(logger, ctx, "invalid actor mapping input", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("create actor mapping entity: %w", err)
	}

	result, err := uc.repo.Upsert(ctx, mapping)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to upsert actor mapping", err)

		libLog.SafeError(logger, ctx, "failed to upsert actor mapping", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("upsert actor mapping: %w", err)
	}

	if result == nil {
		libOpentelemetry.HandleSpanError(span, "actor mapping repository returned nil mapping", ErrNilPersistedActorMapping)

		logger.Log(ctx, libLog.LevelError, ErrNilPersistedActorMapping.Error())

		return nil, ErrNilPersistedActorMapping
	}

	return result, nil
}

// PseudonymizeActor replaces PII fields with [REDACTED] for GDPR compliance.
func (uc *ActorMappingUseCase) PseudonymizeActor(ctx context.Context, actorID string) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "service.governance.pseudonymize_actor")

	defer span.End()

	if emission.IsNilEmitter(uc.streamEmitter) {
		return fmt.Errorf("actor pseudonymized streaming emitter: %w", emission.ErrCriticalOutboxTxRequired)
	}

	if uc.infraProvider == nil {
		return ErrNilInfraProvider
	}

	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin pseudonymize actor transaction: %w", err)
	}

	if txLease == nil || txLease.SQLTx() == nil {
		return fmt.Errorf("begin pseudonymize actor transaction: %w", emission.ErrCriticalOutboxTxRequired)
	}
	defer func() { _ = txLease.Rollback() }()

	if err := uc.repo.PseudonymizeWithTx(ctx, txLease.SQLTx(), actorID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to pseudonymize actor", err)
		libLog.SafeError(logger, ctx, fmt.Sprintf("failed to pseudonymize actor [id_prefix=%s]", entities.SafeActorIDPrefix(actorID)), err, runtime.IsProductionMode())

		return fmt.Errorf("pseudonymize actor: %w", err)
	}

	payload, err := emission.AddTenantID(ctx, map[string]any{
		"actor_id":            actorID,
		"pseudonymized":       true,
		"display_name_status": "REDACTED",
		"email_status":        "REDACTED",
		"updated_at":          time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build actor pseudonymized payload", err)
		return fmt.Errorf("build actor pseudonymized payload: %w", err)
	}

	if err := emission.Emit(ctx, uc.streamEmitter, "actor.pseudonymized", actorID, payload, emission.RequireOutboxTx(), emission.WithOutboxTx(txLease.SQLTx())); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event actor.pseudonymized", err)
		return fmt.Errorf("emit actor pseudonymized: %w", err)
	}

	if err := txLease.Commit(); err != nil {
		return fmt.Errorf("commit pseudonymize actor transaction: %w", err)
	}

	return nil
}

// DeleteActorMapping permanently removes an actor mapping (right-to-erasure).
func (uc *ActorMappingUseCase) DeleteActorMapping(ctx context.Context, actorID string) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "service.governance.delete_actor_mapping")

	defer span.End()

	if err := uc.repo.Delete(ctx, actorID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete actor mapping", err)

		libLog.SafeError(logger, ctx, fmt.Sprintf("failed to delete actor mapping [id_prefix=%s]", entities.SafeActorIDPrefix(actorID)), err, runtime.IsProductionMode())

		return fmt.Errorf("delete actor mapping: %w", err)
	}

	return nil
}
