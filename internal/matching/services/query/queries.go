// Package query provides read operations for matching domain entities.
package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
)

// Sentinel errors for use case validation.
var (
	ErrNilMatchRunRepository   = errors.New("match run repository is required")
	ErrNilMatchGroupRepository = errors.New("match group repository is required")
	ErrNilMatchItemRepository  = errors.New("match item repository is required")
)

// UseCase provides query operations for matching domain entities.
type UseCase struct {
	matchRunRepo   matchingRepos.MatchRunRepository
	matchGroupRepo matchingRepos.MatchGroupRepository
	matchItemRepo  matchingRepos.MatchItemRepository
}

// NewUseCase creates a new query use case with the required repositories.
func NewUseCase(
	matchRunRepo matchingRepos.MatchRunRepository,
	matchGroupRepo matchingRepos.MatchGroupRepository,
	matchItemRepo matchingRepos.MatchItemRepository,
) (*UseCase, error) {
	if matchRunRepo == nil {
		return nil, ErrNilMatchRunRepository
	}

	if matchGroupRepo == nil {
		return nil, ErrNilMatchGroupRepository
	}

	if matchItemRepo == nil {
		return nil, ErrNilMatchItemRepository
	}

	return &UseCase{
		matchRunRepo:   matchRunRepo,
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
	}, nil
}

// GetMatchRun retrieves a match run by ID within a context.
func (uc *UseCase) GetMatchRun(
	ctx context.Context,
	contextID, runID uuid.UUID,
) (*matchingEntities.MatchRun, error) {
	if uc.matchRunRepo == nil {
		return nil, ErrNilMatchRunRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.matching.get_match_run")
	defer span.End()

	result, err := uc.matchRunRepo.FindByID(ctx, contextID, runID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get match run", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to get match run")

		return nil, fmt.Errorf("finding match run: %w", err)
	}

	return result, nil
}

// ListMatchRuns retrieves match runs for a context with cursor pagination.
func (uc *UseCase) ListMatchRuns(
	ctx context.Context,
	contextID uuid.UUID,
	filter matchingRepos.CursorFilter,
) ([]*matchingEntities.MatchRun, libHTTP.CursorPagination, error) {
	if uc.matchRunRepo == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilMatchRunRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.matching.list_match_runs")
	defer span.End()

	result, pagination, err := uc.matchRunRepo.ListByContextID(ctx, contextID, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list match runs", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list match runs")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing match runs: %w", err)
	}

	return result, pagination, nil
}

// ListMatchRunGroups retrieves match groups for a run within a context.
// Items for each group are batch-loaded in a single query to avoid N+1.
func (uc *UseCase) ListMatchRunGroups(
	ctx context.Context,
	contextID, runID uuid.UUID,
	filter matchingRepos.CursorFilter,
) ([]*matchingEntities.MatchGroup, libHTTP.CursorPagination, error) {
	if uc.matchGroupRepo == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilMatchGroupRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.matching.list_match_run_groups")
	defer span.End()

	result, pagination, err := uc.matchGroupRepo.ListByRunID(ctx, contextID, runID, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list match run groups", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list match run groups")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing match run groups: %w", err)
	}

	// Batch-load items for all groups in a single query.
	uc.enrichGroupsWithItems(ctx, logger, result)

	return result, pagination, nil
}

// enrichGroupsWithItems batch-loads match items for the given groups
// and attaches them in place. Failures are logged but never propagated
// because items are enrichment data, not critical for the response.
func (uc *UseCase) enrichGroupsWithItems(
	ctx context.Context,
	logger libLog.Logger,
	groups []*matchingEntities.MatchGroup,
) {
	if len(groups) == 0 || uc.matchItemRepo == nil {
		return
	}

	groupIDs := make([]uuid.UUID, len(groups))
	for i, g := range groups {
		groupIDs[i] = g.ID
	}

	itemsByGroup, err := uc.matchItemRepo.ListByMatchGroupIDs(ctx, groupIDs)
	if err != nil {
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelWarn, "failed to batch-load match items")

		return
	}

	for _, g := range groups {
		if items, ok := itemsByGroup[g.ID]; ok {
			g.Items = items
		}
	}
}

// FindMatchGroupByID retrieves a match group by ID within a context.
func (uc *UseCase) FindMatchGroupByID(
	ctx context.Context,
	contextID, groupID uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	if uc.matchGroupRepo == nil {
		return nil, ErrNilMatchGroupRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.matching.find_match_group_by_id")
	defer span.End()

	result, err := uc.matchGroupRepo.FindByID(ctx, contextID, groupID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find match group", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find match group")

		return nil, fmt.Errorf("finding match group: %w", err)
	}

	return result, nil
}
