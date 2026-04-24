// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package repositories provides persistence interfaces and implementations
// for matching domain entities such as match runs and related records.
package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

//go:generate mockgen -imports=libHTTP=github.com/LerianStudio/lib-commons/v5/commons/net/http -destination=mocks/match_run_repository_mock.go -package=mocks . MatchRunRepository

// MatchRunRepository defines persistence operations for match runs.
type MatchRunRepository interface {
	Create(ctx context.Context, entity *entities.MatchRun) (*entities.MatchRun, error)
	CreateWithTx(ctx context.Context, tx Tx, entity *entities.MatchRun) (*entities.MatchRun, error)
	Update(ctx context.Context, entity *entities.MatchRun) (*entities.MatchRun, error)
	UpdateWithTx(ctx context.Context, tx Tx, entity *entities.MatchRun) (*entities.MatchRun, error)
	FindByID(ctx context.Context, contextID, runID uuid.UUID) (*entities.MatchRun, error)
	WithTx(ctx context.Context, fn func(Tx) error) error
	ListByContextID(
		ctx context.Context,
		contextID uuid.UUID,
		filter CursorFilter,
	) ([]*entities.MatchRun, libHTTP.CursorPagination, error)
}
