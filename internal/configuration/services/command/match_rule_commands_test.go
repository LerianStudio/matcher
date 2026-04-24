// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestCreateMatchRule_PriorityConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	existingRule, err := entities.NewMatchRule(ctx, contextID, entities.CreateMatchRuleInput{
		Priority: 1,
		Type:     shared.RuleTypeExact,
		Config:   map[string]any{"matchCurrency": true},
	})
	require.NoError(t, err)

	repo := &matchRuleRepoStub{
		findByPriorityFn: func(ctx context.Context, contextIDValue uuid.UUID, priority int) (*entities.MatchRule, error) {
			return existingRule, nil
		},
	}
	useCase, err := NewUseCase(&contextRepoStub{}, &sourceRepoStub{}, &fieldMapRepoStub{}, repo)
	require.NoError(t, err)

	input := entities.CreateMatchRuleInput{
		Priority: 1,
		Type:     shared.RuleTypeExact,
		Config:   map[string]any{"matchCurrency": true},
	}
	_, err = useCase.CreateMatchRule(context.Background(), contextID, input)
	require.Error(t, err)
	assert.Equal(t, entities.ErrRulePriorityConflict, err)
}

func TestUpdateMatchRule_ConfigRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	rule, err := entities.NewMatchRule(ctx, contextID, entities.CreateMatchRuleInput{
		Priority: 2,
		Type:     shared.RuleTypeTolerance,
		Config:   map[string]any{"absTolerance": "1.00"},
	})
	require.NoError(t, err)

	repo := &matchRuleRepoStub{
		findByIDFn: func(ctx context.Context, contextIDValue, identifier uuid.UUID) (*entities.MatchRule, error) {
			return rule, nil
		},
		updateFn: func(ctx context.Context, entity *entities.MatchRule) (*entities.MatchRule, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(&contextRepoStub{}, &sourceRepoStub{}, &fieldMapRepoStub{}, repo)
	require.NoError(t, err)

	_, err = useCase.UpdateMatchRule(
		context.Background(),
		contextID,
		rule.ID,
		entities.UpdateMatchRuleInput{Config: map[string]any{}},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, entities.ErrRuleConfigRequired)
}

func TestUpdateMatchRule_PriorityConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	primaryRule, err := entities.NewMatchRule(ctx, contextID, entities.CreateMatchRuleInput{
		Priority: 1,
		Type:     shared.RuleTypeExact,
		Config:   map[string]any{"matchCurrency": true},
	})
	require.NoError(t, err)

	conflictRule := &entities.MatchRule{ID: uuid.New(), ContextID: contextID, Priority: 2}

	repo := &matchRuleRepoStub{
		findByIDFn: func(ctx context.Context, contextIDValue, identifier uuid.UUID) (*entities.MatchRule, error) {
			return primaryRule, nil
		},
		findByPriorityFn: func(ctx context.Context, contextIDValue uuid.UUID, priority int) (*entities.MatchRule, error) {
			return conflictRule, nil
		},
	}
	useCase, err := NewUseCase(&contextRepoStub{}, &sourceRepoStub{}, &fieldMapRepoStub{}, repo)
	require.NoError(t, err)

	newPriority := 2
	_, err = useCase.UpdateMatchRule(
		ctx,
		contextID,
		primaryRule.ID,
		entities.UpdateMatchRuleInput{Priority: &newPriority},
	)
	require.Error(t, err)
	assert.Equal(t, entities.ErrRulePriorityConflict, err)
}

func TestUpdateMatchRule_NotFoundReturnsNoRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	repo := &matchRuleRepoStub{
		findByIDFn: func(ctx context.Context, contextIDValue, identifier uuid.UUID) (*entities.MatchRule, error) {
			return nil, nil
		},
	}
	useCase, err := NewUseCase(&contextRepoStub{}, &sourceRepoStub{}, &fieldMapRepoStub{}, repo)
	require.NoError(t, err)

	priority := 1
	_, err = useCase.UpdateMatchRule(
		ctx,
		contextID,
		uuid.New(),
		entities.UpdateMatchRuleInput{Priority: &priority},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
}
