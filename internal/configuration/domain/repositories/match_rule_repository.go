package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

//go:generate mockgen -source=match_rule_repository.go -destination=mocks/match_rule_repository_mock.go -package=mocks

// MatchRuleRepository defines persistence operations for match rules.
type MatchRuleRepository interface {
	Create(ctx context.Context, entity *entities.MatchRule) (*entities.MatchRule, error)
	FindByID(ctx context.Context, contextID, id uuid.UUID) (*entities.MatchRule, error)
	FindByContextID(
		ctx context.Context,
		contextID uuid.UUID,
		cursor string,
		limit int,
	) (entities.MatchRules, libHTTP.CursorPagination, error)
	FindByContextIDAndType(
		ctx context.Context,
		contextID uuid.UUID,
		ruleType value_objects.RuleType,
		cursor string,
		limit int,
	) (entities.MatchRules, libHTTP.CursorPagination, error)
	FindByPriority(
		ctx context.Context,
		contextID uuid.UUID,
		priority int,
	) (*entities.MatchRule, error)
	Update(ctx context.Context, entity *entities.MatchRule) (*entities.MatchRule, error)
	Delete(ctx context.Context, contextID, id uuid.UUID) error
	ReorderPriorities(ctx context.Context, contextID uuid.UUID, ruleIDs []uuid.UUID) error
}
