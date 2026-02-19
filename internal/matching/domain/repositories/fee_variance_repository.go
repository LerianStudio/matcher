// Package repositories provides matching persistence abstractions.
package repositories

import (
	"context"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// FeeVarianceRepository defines persistence operations for fee variance records.
type FeeVarianceRepository interface {
	CreateBatchWithTx(
		ctx context.Context,
		tx Tx,
		rows []*entities.FeeVariance,
	) ([]*entities.FeeVariance, error)
}
