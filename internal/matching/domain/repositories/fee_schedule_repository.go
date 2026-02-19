// Package repositories provides matching persistence abstractions.
package repositories

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// FeeScheduleRepository defines persistence operations for fee schedules.
type FeeScheduleRepository interface {
	Create(ctx context.Context, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error)
	GetByID(ctx context.Context, id uuid.UUID) (*fee.FeeSchedule, error)
	Update(ctx context.Context, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error)
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, limit int) ([]*fee.FeeSchedule, error)
	GetByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error)
}
