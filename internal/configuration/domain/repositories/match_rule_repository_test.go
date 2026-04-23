//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestMatchRuleRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ MatchRuleRepository = (*mockMatchRuleRepository)(nil)
}

type mockMatchRuleRepository struct {
	rules map[uuid.UUID]*entities.MatchRule
}

func (m *mockMatchRuleRepository) Create(
	_ context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if m.rules == nil {
		m.rules = make(map[uuid.UUID]*entities.MatchRule)
	}

	m.rules[entity.ID] = entity

	return entity, nil
}

func (m *mockMatchRuleRepository) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*entities.MatchRule, error) {
	if rule, ok := m.rules[id]; ok {
		return rule, nil
	}

	return nil, nil
}

func (m *mockMatchRuleRepository) FindByContextID(
	_ context.Context,
	contextID uuid.UUID,
	_ string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	result := make(entities.MatchRules, 0, len(m.rules))

	for _, rule := range m.rules {
		if rule.ContextID == contextID {
			result = append(result, rule)
		}
	}

	if limit > 0 && limit < len(result) {
		return result[:limit], libHTTP.CursorPagination{}, nil
	}

	return result, libHTTP.CursorPagination{}, nil
}

func (m *mockMatchRuleRepository) FindByContextIDAndType(
	_ context.Context,
	contextID uuid.UUID,
	ruleType shared.RuleType,
	_ string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	result := make(entities.MatchRules, 0, len(m.rules))

	for _, rule := range m.rules {
		if rule.ContextID == contextID && rule.Type == ruleType {
			result = append(result, rule)
		}
	}

	if limit > 0 && limit < len(result) {
		return result[:limit], libHTTP.CursorPagination{}, nil
	}

	return result, libHTTP.CursorPagination{}, nil
}

func (m *mockMatchRuleRepository) FindByPriority(
	_ context.Context,
	contextID uuid.UUID,
	priority int,
) (*entities.MatchRule, error) {
	for _, rule := range m.rules {
		if rule.ContextID == contextID && rule.Priority == priority {
			return rule, nil
		}
	}

	return nil, nil
}

func (m *mockMatchRuleRepository) Update(
	_ context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if m.rules == nil {
		m.rules = make(map[uuid.UUID]*entities.MatchRule)
	}

	m.rules[entity.ID] = entity

	return entity, nil
}

func (m *mockMatchRuleRepository) Delete(_ context.Context, _, id uuid.UUID) error {
	delete(m.rules, id)
	return nil
}

func (m *mockMatchRuleRepository) ReorderPriorities(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func TestMockMatchRuleRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("Create stores rule", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchRuleRepository{}
		ruleID := uuid.New()
		rule := &entities.MatchRule{
			ID:       ruleID,
			Priority: 1,
			Type:     shared.RuleTypeExact,
		}

		created, err := repo.Create(context.Background(), rule)

		require.NoError(t, err)
		assert.Equal(t, ruleID, created.ID)
	})

	t.Run("FindByID retrieves rule", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchRuleRepository{}
		ruleID := uuid.New()
		contextID := uuid.New()
		rule := &entities.MatchRule{
			ID:        ruleID,
			ContextID: contextID,
			Priority:  1,
			Type:      shared.RuleTypeExact,
		}

		_, err := repo.Create(context.Background(), rule)

		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), contextID, ruleID)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, ruleID, found.ID)
	})

	t.Run("FindByPriority retrieves rule by priority", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchRuleRepository{}
		ruleID := uuid.New()
		contextID := uuid.New()
		rule := &entities.MatchRule{
			ID:        ruleID,
			ContextID: contextID,
			Priority:  1,
			Type:      shared.RuleTypeExact,
		}

		_, err := repo.Create(context.Background(), rule)

		require.NoError(t, err)

		found, err := repo.FindByPriority(context.Background(), contextID, 1)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, 1, found.Priority)
	})

	t.Run("Delete removes rule", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchRuleRepository{}
		ruleID := uuid.New()
		contextID := uuid.New()
		rule := &entities.MatchRule{ID: ruleID, ContextID: contextID}

		_, err := repo.Create(context.Background(), rule)

		require.NoError(t, err)

		err = repo.Delete(context.Background(), contextID, ruleID)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), contextID, ruleID)
		require.NoError(t, err)
		assert.Nil(t, found)
	})
}
