package command

import (
	"context"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
)

// ErrNilActorMappingRepository is an alias for the domain sentinel error.
var ErrNilActorMappingRepository = entities.ErrNilActorMappingRepository

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
func (uc *ActorMappingUseCase) UpsertActorMapping(ctx context.Context, actorID string, displayName, email *string) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "service.governance.upsert_actor_mapping")

	defer span.End()

	mapping, err := entities.NewActorMapping(ctx, actorID, displayName, email)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid actor mapping input", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("invalid actor mapping input: %v", err))

		return fmt.Errorf("create actor mapping entity: %w", err)
	}

	if err := uc.repo.Upsert(ctx, mapping); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to upsert actor mapping", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to upsert actor mapping: %v", err))

		return fmt.Errorf("upsert actor mapping: %w", err)
	}

	return nil
}

// PseudonymizeActor replaces PII fields with [REDACTED] for GDPR compliance.
func (uc *ActorMappingUseCase) PseudonymizeActor(ctx context.Context, actorID string) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "service.governance.pseudonymize_actor")

	defer span.End()

	if err := uc.repo.Pseudonymize(ctx, actorID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to pseudonymize actor", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to pseudonymize actor [id_prefix=%s]: %v", entities.SafeActorIDPrefix(actorID), err))

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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to delete actor mapping [id_prefix=%s]: %v", entities.SafeActorIDPrefix(actorID), err))

		return fmt.Errorf("delete actor mapping: %w", err)
	}

	return nil
}
