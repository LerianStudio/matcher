// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package repositories

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

//go:generate mockgen -destination=mocks/match_item_repository_mock.go -package=mocks . MatchItemRepository

// MatchItemRepository defines persistence operations for match items.
type MatchItemRepository interface {
	CreateBatch(ctx context.Context, items []*entities.MatchItem) ([]*entities.MatchItem, error)
	CreateBatchWithTx(
		ctx context.Context,
		tx Tx,
		items []*entities.MatchItem,
	) ([]*entities.MatchItem, error)
	ListByMatchGroupID(ctx context.Context, matchGroupID uuid.UUID) ([]*entities.MatchItem, error)
	// ListByMatchGroupIDs returns all match items for the given group IDs in a single query.
	// Results are grouped by match_group_id for efficient association with parent groups.
	ListByMatchGroupIDs(ctx context.Context, matchGroupIDs []uuid.UUID) (map[uuid.UUID][]*entities.MatchItem, error)
}
