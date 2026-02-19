// Package repositories provides matching persistence abstractions.
package repositories

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// RateRepository defines persistence operations for fee rates.
type RateRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*fee.Rate, error)
}
