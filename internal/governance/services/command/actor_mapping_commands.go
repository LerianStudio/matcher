package command

import (
	"context"
	"errors"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
)

// TODO(telemetry): governance/adapters/http/handlers.go — logSpanError uses HandleSpanError for
// business outcomes (badRequest, notFound, writeNotFound). Add logSpanBusinessEvent using
// HandleSpanBusinessErrorEvent and create business-aware variants for 400/404 responses.
// See reporting/adapters/http/handlers_export_job.go for the reference implementation.

// Sentinel errors for actor-mapping command operations.
var (
	ErrNilActorMappingRepository = entities.ErrNilActorMappingRepository
	ErrNilPersistedActorMapping  = errors.New("actor mapping repository returned nil mapping")
)

// ActorMappingUseCase handles command operations for actor mappings.
type ActorMappingUseCase struct {
	repo repositories.ActorMappingRepository
}

// NewActorMappingUseCase creates a new actor mapping command use case.
func NewActorMappingUseCase(repo repositories.ActorMappingRepository) (*ActorMappingUseCase, error) {
	if repo == nil {
		return nil, ErrNilActorMappingRepository
	}

	return &ActorMappingUseCase{repo: repo}, nil
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

	if err := uc.repo.Pseudonymize(ctx, actorID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to pseudonymize actor", err)

		libLog.SafeError(logger, ctx, fmt.Sprintf("failed to pseudonymize actor [id_prefix=%s]", entities.SafeActorIDPrefix(actorID)), err, runtime.IsProductionMode())

		return fmt.Errorf("pseudonymize actor: %w", err)
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
