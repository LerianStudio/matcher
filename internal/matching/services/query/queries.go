// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package query provides read operations for matching domain entities.
package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

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

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list match run groups")

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
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelWarn, "failed to batch-load match items")

		return
	}

	for _, g := range groups {
		if items, ok := itemsByGroup[g.ID]; ok {
			g.Items = items
		}
	}
}
