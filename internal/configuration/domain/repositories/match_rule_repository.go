// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
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
		ruleType shared.RuleType,
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
