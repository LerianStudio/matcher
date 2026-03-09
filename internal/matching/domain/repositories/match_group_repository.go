// Package repositories provides persistence abstractions for matching domain
// repositories, including MatchGroupRepository and related repository
// interfaces.
package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// CursorFilter contains pagination and sorting parameters.
type CursorFilter struct {
	Limit     int
	Cursor    string
	SortBy    string
	SortOrder string
}

//go:generate mockgen -destination=mocks/match_group_repository_mock.go -package=mocks . MatchGroupRepository

// MatchGroupRepository defines persistence operations for match groups.
type MatchGroupRepository interface {
	CreateBatch(ctx context.Context, groups []*entities.MatchGroup) ([]*entities.MatchGroup, error)
	CreateBatchWithTx(
		ctx context.Context,
		tx Tx,
		groups []*entities.MatchGroup,
	) ([]*entities.MatchGroup, error)
	ListByRunID(
		ctx context.Context,
		contextID, runID uuid.UUID,
		filter CursorFilter,
	) ([]*entities.MatchGroup, libHTTP.CursorPagination, error)
	FindByID(ctx context.Context, contextID, id uuid.UUID) (*entities.MatchGroup, error)
	Update(ctx context.Context, group *entities.MatchGroup) (*entities.MatchGroup, error)
	UpdateWithTx(
		ctx context.Context,
		tx Tx,
		group *entities.MatchGroup,
	) (*entities.MatchGroup, error)
}
