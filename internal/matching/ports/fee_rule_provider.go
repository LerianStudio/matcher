package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// FeeRuleProvider provides fee rule information for the matching pipeline.
// This abstracts the Configuration context's FeeRuleRepository.
type FeeRuleProvider interface {
	FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*fee.FeeRule, error)
}
