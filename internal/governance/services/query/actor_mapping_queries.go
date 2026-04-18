// Package query provides governance query use cases.
package query

import (
	"context"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
)

// ErrNilActorMappingRepository is an alias for the domain sentinel error.
var ErrNilActorMappingRepository = entities.ErrNilActorMappingRepository

// ActorMappingQueryUseCase handles query operations for actor mappings.
type ActorMappingQueryUseCase struct {
	repo repositories.ActorMappingRepository
}

// NewActorMappingQueryUseCase creates a new actor mapping query use case.
func NewActorMappingQueryUseCase(repo repositories.ActorMappingRepository) (*ActorMappingQueryUseCase, error) {
	if repo == nil {
		return nil, ErrNilActorMappingRepository
	}

	return &ActorMappingQueryUseCase{repo: repo}, nil
}

// GetActorMapping retrieves an actor mapping by its actor ID.
func (uc *ActorMappingQueryUseCase) GetActorMapping(ctx context.Context, actorID string) (*entities.ActorMapping, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "service.governance.get_actor_mapping")

	defer span.End()

	mapping, err := uc.repo.GetByActorID(ctx, actorID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get actor mapping", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get actor mapping [id_prefix=%s]: %v", entities.SafeActorIDPrefix(actorID), err))

		return nil, fmt.Errorf("get actor mapping: %w", err)
	}

	return mapping, nil
}
