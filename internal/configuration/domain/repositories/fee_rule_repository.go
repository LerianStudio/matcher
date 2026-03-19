package repositories

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

//go:generate mockgen -source=fee_rule_repository.go -destination=mocks/fee_rule_repository_mock.go -package=mocks

// FeeRuleRepository defines persistence operations for fee rules.
type FeeRuleRepository interface {
	Create(ctx context.Context, rule *fee.FeeRule) error
	CreateWithTx(ctx context.Context, tx *sql.Tx, rule *fee.FeeRule) error
	FindByID(ctx context.Context, id uuid.UUID) (*fee.FeeRule, error)
	FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*fee.FeeRule, error)
	Update(ctx context.Context, rule *fee.FeeRule) error
	UpdateWithTx(ctx context.Context, tx *sql.Tx, rule *fee.FeeRule) error
	Delete(ctx context.Context, id uuid.UUID) error
	DeleteWithTx(ctx context.Context, tx *sql.Tx, id uuid.UUID) error
}
