// Package repositories provides matching persistence abstractions.
package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

//go:generate mockgen -destination=mocks/adjustment_repository_mock.go -package=mocks . AdjustmentRepository

// AdjustmentRepository defines persistence operations for adjustments.
type AdjustmentRepository interface {
	Create(ctx context.Context, adjustment *entities.Adjustment) (*entities.Adjustment, error)
	// CreateWithTx creates an adjustment within the provided transaction.
	// This enables atomic operations where adjustment creation and audit logging
	// must succeed or fail together (SOX compliance).
	CreateWithTx(
		ctx context.Context,
		tx any,
		adjustment *entities.Adjustment,
	) (*entities.Adjustment, error)
	FindByID(ctx context.Context, contextID, id uuid.UUID) (*entities.Adjustment, error)
	ListByContextID(
		ctx context.Context,
		contextID uuid.UUID,
		filter CursorFilter,
	) ([]*entities.Adjustment, libHTTP.CursorPagination, error)
	ListByMatchGroupID(
		ctx context.Context,
		contextID, matchGroupID uuid.UUID,
	) ([]*entities.Adjustment, error)
}
