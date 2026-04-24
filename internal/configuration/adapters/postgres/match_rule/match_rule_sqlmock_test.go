// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package match_rule

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestRepository_Create_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"field": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.Create(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.Create(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.Create(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrMatchRuleEntityRequired)
	})
}

func TestRepository_CreateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"field": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.CreateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.CreateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.CreateWithTx(ctx, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrMatchRuleEntityRequired)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.CreateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_FindByID_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByID(ctx, testContextID, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindByID(ctx, testContextID, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindByContextID_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, _, err := repo.FindByContextID(ctx, testContextID, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, _, err := repo.FindByContextID(ctx, testContextID, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindByContextIDAndType_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	ruleType := shared.RuleTypeExact

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, _, err := repo.FindByContextIDAndType(ctx, testContextID, ruleType, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, _, err := repo.FindByContextIDAndType(ctx, testContextID, ruleType, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_Update_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"field": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.Update(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.Update(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.Update(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrMatchRuleEntityRequired)
	})
}

func TestRepository_UpdateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"field": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.UpdateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.UpdateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.UpdateWithTx(ctx, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrMatchRuleEntityRequired)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.UpdateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_Delete_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.Delete(ctx, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.Delete(ctx, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_DeleteWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.DeleteWithTx(ctx, nil, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.DeleteWithTx(ctx, nil, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.DeleteWithTx(ctx, nil, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_ReorderPriorities_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	ruleIDs := []uuid.UUID{uuid.New(), uuid.New()}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.ReorderPriorities(ctx, testContextID, ruleIDs)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.ReorderPriorities(ctx, testContextID, ruleIDs)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("empty rule IDs returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.ReorderPriorities(ctx, testContextID, []uuid.UUID{})

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRuleIDsRequired)
	})

	t.Run("nil rule IDs returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.ReorderPriorities(ctx, testContextID, nil)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRuleIDsRequired)
	})
}

func TestRepository_ProviderConnectionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connectionErr := errors.New("connection failed")

	t.Run("FindByID returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindByContextID returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, _, err := repo.FindByContextID(ctx, uuid.New(), "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindByContextIDAndType returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, _, err := repo.FindByContextIDAndType(ctx, uuid.New(), shared.RuleTypeExact, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("ReorderPriorities returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		err := repo.ReorderPriorities(ctx, uuid.New(), []uuid.UUID{uuid.New()})

		require.Error(t, err)
		require.ErrorIs(t, err, connectionErr)
	})
}

func TestBuildReorderQuery(t *testing.T) {
	t.Parallel()

	t.Run("single rule ID", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		ruleID := uuid.New()
		ruleIDs := []uuid.UUID{ruleID}

		query, args := buildReorderQuery(contextID, ruleIDs)

		require.NotEmpty(t, query)
		require.Len(t, args, 4)
		require.Equal(t, contextID.String(), args[0])
		require.Equal(t, ruleID.String(), args[1])
		require.Equal(t, 1, args[2])
		require.Equal(t, ruleID.String(), args[3])
	})

	t.Run("multiple rule IDs", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		ruleID1 := uuid.New()
		ruleID2 := uuid.New()
		ruleID3 := uuid.New()
		ruleIDs := []uuid.UUID{ruleID1, ruleID2, ruleID3}

		query, args := buildReorderQuery(contextID, ruleIDs)

		require.NotEmpty(t, query)
		require.Contains(t, query, "UPDATE match_rules SET priority = CASE id")
		require.Contains(t, query, "WHERE context_id = $1 AND id IN")
		require.Len(t, args, 10)
		require.Equal(t, contextID.String(), args[0])
	})

	t.Run("query contains all rule IDs in IN clause", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		ruleIDs := []uuid.UUID{uuid.New(), uuid.New()}

		query, _ := buildReorderQuery(contextID, ruleIDs)

		require.Contains(t, query, "IN")
	})

	t.Run("priority assignment is sequential", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		ruleIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

		_, args := buildReorderQuery(contextID, ruleIDs)

		require.Equal(t, 1, args[2])
		require.Equal(t, 2, args[4])
		require.Equal(t, 3, args[6])
	})
}

func TestModelConversion_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("model with empty config", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.MatchRule{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Priority:  1,
			Type:      shared.RuleTypeExact,
			Config:    map[string]any{},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewMatchRulePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Config)
		require.Empty(t, resultEntity.Config)
	})

	t.Run("model with complex config", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.MatchRule{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Priority:  1,
			Type:      shared.RuleTypeTolerance,
			Config: map[string]any{
				"tolerance":     0.05,
				"fields":        []string{"amount", "date", "reference"},
				"strictMode":    true,
				"caseSensitive": false,
				"nested": map[string]any{
					"threshold": 100,
					"enabled":   true,
				},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewMatchRulePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Config)
		require.InDelta(t, 0.05, resultEntity.Config["tolerance"], 0.001)
		require.Equal(t, true, resultEntity.Config["strictMode"])
	})

	t.Run("model with various priority values", func(t *testing.T) {
		t.Parallel()

		priorities := []int{0, 1, 10, 100, 1000, 9999}
		now := time.Now().UTC()

		for _, priority := range priorities {
			entity := &entities.MatchRule{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				Priority:  priority,
				Type:      shared.RuleTypeExact,
				Config:    map[string]any{},
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewMatchRulePostgreSQLModel(entity)
			require.NoError(t, err)

			resultEntity, err := model.ToEntity()
			require.NoError(t, err)
			require.Equal(t, priority, resultEntity.Priority)
		}
	})

	t.Run("model preserves timestamps", func(t *testing.T) {
		t.Parallel()

		createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		updatedAt := time.Date(2024, 6, 20, 14, 45, 0, 0, time.UTC)
		entity := &entities.MatchRule{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Priority:  1,
			Type:      shared.RuleTypeExact,
			Config:    map[string]any{},
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		}

		model, err := NewMatchRulePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, createdAt, resultEntity.CreatedAt)
		require.Equal(t, updatedAt, resultEntity.UpdatedAt)
	})
}

func TestConfigEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("config with numeric values", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.MatchRule{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Priority:  1,
			Type:      shared.RuleTypeTolerance,
			Config: map[string]any{
				"integer":   float64(42),
				"float":     3.14159,
				"negative":  -100.5,
				"zero":      float64(0),
				"large":     float64(999999999),
				"precision": 0.000001,
			},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewMatchRulePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.InDelta(t, 42.0, resultEntity.Config["integer"], 0.001)
		require.InDelta(t, 3.14159, resultEntity.Config["float"], 0.00001)
	})

	t.Run("config with boolean values", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.MatchRule{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Priority:  1,
			Type:      shared.RuleTypeExact,
			Config: map[string]any{
				"enabled":       true,
				"disabled":      false,
				"strictMode":    true,
				"caseSensitive": false,
			},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewMatchRulePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, true, resultEntity.Config["enabled"])
		require.Equal(t, false, resultEntity.Config["disabled"])
	})

	t.Run("config with array values", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.MatchRule{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Priority:  1,
			Type:      shared.RuleTypeExact,
			Config: map[string]any{
				"fields":  []any{"amount", "date", "reference"},
				"numbers": []any{float64(1), float64(2), float64(3)},
				"mixed":   []any{"string", float64(42), true},
				"empty":   []any{},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewMatchRulePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Config["fields"])
		require.NotNil(t, resultEntity.Config["empty"])
	})
}

func TestConfigWithUnicodeValues(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"japanese": "日本語",
			"emoji":    "🔥",
			"arabic":   "العربية",
			"cyrillic": "Кириллица",
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)
	require.Equal(t, "日本語", resultEntity.Config["japanese"])
	require.Equal(t, "🔥", resultEntity.Config["emoji"])
	require.Equal(t, "العربية", resultEntity.Config["arabic"])
	require.Equal(t, "Кириллица", resultEntity.Config["cyrillic"])
}

func TestConfigWithNullValues(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      "EXACT",
		Config:    []byte(`{"nullField":null,"nested":{"nullNested":null}}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Nil(t, entity.Config["nullField"])
	require.NotNil(t, entity.Config["nested"])
}

func createValidMatchRuleEntity() *entities.MatchRule {
	now := time.Now().UTC()

	return &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"field": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestScanMatchRule_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 1, "EXACT", []byte(`{"key":"value"}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(
		ctx,
		"SELECT id, context_id, priority, type, config, created_at, updated_at FROM match_rules",
	)
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)
	require.Equal(t, contextID, result.ContextID)
	require.Equal(t, 1, result.Priority)
	require.Equal(t, shared.RuleTypeExact, result.Type)
	require.Equal(t, "value", result.Config["key"])
}

func TestScanMatchRule_ScanError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow("invalid-uuid", uuid.New(), 1, "EXACT", []byte(`{}`), time.Now().UTC(), time.Now().UTC())
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

func TestScanMatchRule_InvalidType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(uuid.New(), uuid.New(), 1, "INVALID_TYPE", []byte(`{}`), time.Now().UTC(), time.Now().UTC())
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parse rule type")
}

func TestScanMatchRule_InvalidConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(uuid.New(), uuid.New(), 1, "EXACT", []byte(`{invalid-json`), time.Now().UTC(), time.Now().UTC())
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "unmarshal config")
}

func TestExecuteMatchRulesQuery_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id1 := uuid.New()
	id2 := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id1.String(), contextID.String(), 1, "EXACT", []byte(`{"field":"value1"}`), now, now).
		AddRow(id2.String(), contextID.String(), 2, "TOLERANCE", []byte(`{"tolerance":0.05}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(
		ctx,
		tx,
		"SELECT id, context_id, priority, type, config, created_at, updated_at FROM match_rules",
		[]any{},
	)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, id1, result[0].ID)
	require.Equal(t, id2, result[1].ID)
	require.Equal(t, 1, result[0].Priority)
	require.Equal(t, 2, result[1].Priority)
}

func TestExecuteMatchRulesQuery_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	rows := sqlmock.NewRows(
		[]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(
		ctx,
		tx,
		"SELECT id, context_id, priority, type, config, created_at, updated_at FROM match_rules",
		[]any{},
	)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestExecuteMatchRulesQuery_ScanError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow("invalid-uuid", uuid.New(), 1, "EXACT", []byte(`{}`), time.Now().UTC(), time.Now().UTC())
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(ctx, tx, "SELECT 1", []any{})
	require.Error(t, err)
	require.Nil(t, result)
}

func TestExecuteMatchRulesQuery_QueryError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query failed"))

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(ctx, tx, "SELECT 1", []any{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "query failed")
	require.Nil(t, result)
}

func TestFetchCursorPriority_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	cursorID := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"priority"}).AddRow(5)
	mock.ExpectQuery("SELECT priority FROM match_rules").
		WithArgs(cursorID.String(), contextID.String()).
		WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	priority, err := fetchCursorPriority(ctx, tx, cursorID, contextID)
	require.NoError(t, err)
	require.Equal(t, 5, priority)
}

func TestFetchCursorPriority_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	cursorID := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT priority FROM match_rules").
		WithArgs(cursorID.String(), contextID.String()).
		WillReturnError(sql.ErrNoRows)

	tx, err := db.Begin()
	require.NoError(t, err)

	priority, err := fetchCursorPriority(ctx, tx, cursorID, contextID)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCursorNotFound)
	require.Equal(t, 0, priority)
}

func TestFetchCursorPriority_QueryError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	cursorID := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT priority FROM match_rules").
		WithArgs(cursorID.String(), contextID.String()).
		WillReturnError(errors.New("database error"))

	tx, err := db.Begin()
	require.NoError(t, err)

	priority, err := fetchCursorPriority(ctx, tx, cursorID, contextID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validating cursor")
	require.Equal(t, 0, priority)
}

func TestRepository_FindByPriority_ConnectionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connectionErr := errors.New("connection failed")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: connectionErr,
	}
	repo := NewRepository(provider)
	result, err := repo.FindByPriority(ctx, uuid.New(), 1)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, connectionErr)
}

func TestScanMatchRule_AllRuleTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		ruleTypeStr  string
		expectedType shared.RuleType
	}{
		{"EXACT", "EXACT", shared.RuleTypeExact},
		{"TOLERANCE", "TOLERANCE", shared.RuleTypeTolerance},
		{"DATE_LAG", "DATE_LAG", shared.RuleTypeDateLag},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			id := uuid.New()
			contextID := uuid.New()
			now := time.Now().UTC()

			rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
				AddRow(id.String(), contextID.String(), 1, tc.ruleTypeStr, []byte(`{}`), now, now)
			mock.ExpectQuery("SELECT").WillReturnRows(rows)

			sqlRows, err := db.QueryContext(ctx, "SELECT 1")
			require.NoError(t, err)

			defer sqlRows.Close()

			require.True(t, sqlRows.Next())

			result, err := scanMatchRule(sqlRows)
			require.NoError(t, err)
			require.Equal(t, tc.expectedType, result.Type)
		})
	}
}

func TestScanMatchRule_WithComplexConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()
	config := []byte(
		`{"tolerance":0.05,"fields":["amount","date"],"nested":{"key":"value"},"enabled":true}`,
	)

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 10, "TOLERANCE", config, now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.InDelta(t, 0.05, result.Config["tolerance"], 0.001)
	require.Equal(t, true, result.Config["enabled"])
	require.NotNil(t, result.Config["fields"])
	require.NotNil(t, result.Config["nested"])
}

func TestExecuteMatchRulesQuery_RowsCloseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(uuid.New(), uuid.New(), 1, "EXACT", []byte(`{}`), time.Now().UTC(), time.Now().UTC()).
		CloseError(errors.New("close error"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(ctx, tx, "SELECT 1", []any{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "close error")
	require.Nil(t, result)
}

func TestExecuteMatchRulesQuery_RowsError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(uuid.New(), uuid.New(), 1, "EXACT", []byte(`{}`), time.Now().UTC(), time.Now().UTC()).
		RowError(0, errors.New("row iteration error"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(ctx, tx, "SELECT 1", []any{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "row iteration error")
	require.Nil(t, result)
}

func TestBuildReorderQuery_MultipleRuleIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ruleID1 := uuid.New()
	ruleID2 := uuid.New()
	ruleID3 := uuid.New()
	ruleIDs := []uuid.UUID{ruleID1, ruleID2, ruleID3}

	query, args := buildReorderQuery(contextID, ruleIDs)

	require.Contains(t, query, "UPDATE match_rules SET priority = CASE id")
	require.Contains(t, query, "WHEN $2 THEN $3::int")
	require.Contains(t, query, "WHEN $4 THEN $5::int")
	require.Contains(t, query, "WHEN $6 THEN $7::int")
	require.Contains(t, query, "WHERE context_id = $1 AND id IN ($8, $9, $10)")
	require.Len(t, args, 10)
	require.Equal(t, contextID.String(), args[0])
	require.Equal(t, ruleID1.String(), args[1])
	require.Equal(t, 1, args[2])
	require.Equal(t, ruleID2.String(), args[3])
	require.Equal(t, 2, args[4])
	require.Equal(t, ruleID3.String(), args[5])
	require.Equal(t, 3, args[6])
}

func TestScanMatchRule_EmptyConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 1, "EXACT", []byte(`{}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result.Config)
	require.Empty(t, result.Config)
}

func TestScanMatchRule_NullConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 1, "EXACT", []byte(nil), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result.Config)
	require.Empty(t, result.Config)
}

func TestScanMatchRule_PreservesTimestamps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 6, 20, 14, 45, 0, 0, time.UTC)

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 1, "EXACT", []byte(`{}`), createdAt, updatedAt)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.NoError(t, err)
	require.Equal(t, createdAt, result.CreatedAt)
	require.Equal(t, updatedAt, result.UpdatedAt)
}

func TestExecuteMatchRulesQuery_MultipleRulesWithDifferentTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(uuid.New(), contextID.String(), 1, "EXACT", []byte(`{"field":"amount"}`), now, now).
		AddRow(uuid.New(), contextID.String(), 2, "TOLERANCE", []byte(`{"tolerance":0.01}`), now, now).
		AddRow(uuid.New(), contextID.String(), 3, "DATE_LAG", []byte(`{"maxDays":5}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(ctx, tx, "SELECT 1", []any{})
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, shared.RuleTypeExact, result[0].Type)
	require.Equal(t, shared.RuleTypeTolerance, result[1].Type)
	require.Equal(t, shared.RuleTypeDateLag, result[2].Type)
}

func TestExecuteCreate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidMatchRuleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO match_rules").
		WithArgs(
			entity.ID.String(),
			entity.ContextID.String(),
			entity.Priority,
			"EXACT",
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeCreate(ctx, tx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.Equal(t, entity.ContextID, result.ContextID)
}

func TestExecuteCreate_ExecError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidMatchRuleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO match_rules").
		WillReturnError(errors.New("database error"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeCreate(ctx, tx, entity)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database error")
	require.Nil(t, result)
}

func TestExecuteCreate_InvalidEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.Nil,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{},
	}

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeCreate(ctx, tx, entity)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMatchRuleContextIDRequired)
	require.Nil(t, result)
}

func TestExecuteUpdate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidMatchRuleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_rules").
		WithArgs(
			entity.Priority,
			"EXACT",
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			entity.ContextID.String(),
			entity.ID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
}

func TestExecuteUpdate_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidMatchRuleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_rules").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)
	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.Nil(t, result)
}

func TestExecuteUpdate_ExecError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidMatchRuleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_rules").
		WillReturnError(errors.New("database error"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database error")
	require.Nil(t, result)
}

func TestExecuteUpdate_InvalidEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.Nil,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{},
	}

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMatchRuleContextIDRequired)
	require.Nil(t, result)
}

func TestExecuteUpdate_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidMatchRuleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_rules").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rows affected error")
	require.Nil(t, result)
}

func TestExecuteDelete_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM match_rules").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeDelete(ctx, tx, contextID, ruleID)
	require.NoError(t, err)
	require.True(t, result)
}

func TestExecuteDelete_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM match_rules").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeDelete(ctx, tx, contextID, ruleID)
	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.False(t, result)
}

func TestExecuteDelete_ExecError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM match_rules").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnError(errors.New("database error"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeDelete(ctx, tx, contextID, ruleID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database error")
	require.False(t, result)
}

func TestExecuteDelete_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM match_rules").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeDelete(ctx, tx, contextID, ruleID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rows affected error")
	require.False(t, result)
}

func TestScanMatchRule_InvalidContextID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(uuid.New(), "invalid-context-id", 1, "EXACT", []byte(`{}`), time.Now().UTC(), time.Now().UTC())
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

func TestFetchCursorPriority_VariousPriorityValues(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		priority int
	}{
		{"zero priority", 0},
		{"positive priority", 10},
		{"large priority", 9999},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			cursorID := uuid.New()
			contextID := uuid.New()

			mock.ExpectBegin()

			rows := sqlmock.NewRows([]string{"priority"}).AddRow(tc.priority)
			mock.ExpectQuery("SELECT priority FROM match_rules").
				WithArgs(cursorID.String(), contextID.String()).
				WillReturnRows(rows)

			tx, err := db.Begin()
			require.NoError(t, err)

			priority, err := fetchCursorPriority(ctx, tx, cursorID, contextID)
			require.NoError(t, err)
			require.Equal(t, tc.priority, priority)
		})
	}
}

func TestExecuteUpdate_UpdatesTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	originalUpdatedAt := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"field": "value"},
		CreatedAt: originalUpdatedAt,
		UpdatedAt: originalUpdatedAt,
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_rules").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.UpdatedAt.After(originalUpdatedAt))
}

func TestBuildReorderQuery_TwoRuleIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ruleID1 := uuid.New()
	ruleID2 := uuid.New()
	ruleIDs := []uuid.UUID{ruleID1, ruleID2}

	query, args := buildReorderQuery(contextID, ruleIDs)

	require.Contains(t, query, "UPDATE match_rules SET priority = CASE id")
	require.Contains(t, query, "WHEN $2 THEN $3::int")
	require.Contains(t, query, "WHEN $4 THEN $5::int")
	require.Contains(t, query, "WHERE context_id = $1 AND id IN ($6, $7)")
	require.Len(t, args, 7)
	require.Equal(t, contextID.String(), args[0])
	require.Equal(t, ruleID1.String(), args[1])
	require.Equal(t, 1, args[2])
	require.Equal(t, ruleID2.String(), args[3])
	require.Equal(t, 2, args[4])
	require.Equal(t, ruleID1.String(), args[5])
	require.Equal(t, ruleID2.String(), args[6])
}

func TestExecuteCreate_AllRuleTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		ruleType     shared.RuleType
		expectedType string
	}{
		{"EXACT", shared.RuleTypeExact, "EXACT"},
		{"TOLERANCE", shared.RuleTypeTolerance, "TOLERANCE"},
		{"DATE_LAG", shared.RuleTypeDateLag, "DATE_LAG"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			now := time.Now().UTC()
			entity := &entities.MatchRule{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				Priority:  1,
				Type:      tc.ruleType,
				Config:    map[string]any{},
				CreatedAt: now,
				UpdatedAt: now,
			}

			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO match_rules").
				WithArgs(
					entity.ID.String(),
					entity.ContextID.String(),
					entity.Priority,
					tc.expectedType,
					sqlmock.AnyArg(),
					sqlmock.AnyArg(),
					sqlmock.AnyArg(),
				).
				WillReturnResult(sqlmock.NewResult(1, 1))

			tx, err := db.Begin()
			require.NoError(t, err)

			provider := &testutil.MockInfrastructureProvider{}
			repo := NewRepository(provider)
			result, err := repo.executeCreate(ctx, tx, entity)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.ruleType, result.Type)
		})
	}
}

func TestExecuteCreate_WithComplexConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  5,
		Type:      shared.RuleTypeTolerance,
		Config: map[string]any{
			"tolerance":  0.05,
			"fields":     []string{"amount", "date"},
			"strictMode": true,
			"nested": map[string]any{
				"key": "value",
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO match_rules").
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeCreate(ctx, tx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.InDelta(t, 0.05, result.Config["tolerance"], 0.001)
	require.Equal(t, true, result.Config["strictMode"])
}

func TestExecuteUpdate_WithDifferentPriorities(t *testing.T) {
	t.Parallel()

	priorities := []int{0, 1, 10, 100, 1000}

	for _, priority := range priorities {
		t.Run(fmt.Sprintf("priority_%d", priority), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			now := time.Now().UTC()
			entity := &entities.MatchRule{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				Priority:  priority,
				Type:      shared.RuleTypeExact,
				Config:    map[string]any{},
				CreatedAt: now,
				UpdatedAt: now,
			}

			mock.ExpectBegin()
			mock.ExpectExec("UPDATE match_rules").
				WillReturnResult(sqlmock.NewResult(0, 1))

			tx, err := db.Begin()
			require.NoError(t, err)

			provider := &testutil.MockInfrastructureProvider{}
			repo := NewRepository(provider)
			result, err := repo.executeUpdate(ctx, tx, entity)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, priority, result.Priority)
		})
	}
}

func TestScanMatchRule_ScanFailsWithMissingColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id"}).
		AddRow("not-enough-columns")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_Create_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	providerErr := errors.New("provider connection error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	entity := createValidMatchRuleEntity()
	result, err := repo.Create(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to create match rule")
}

func TestRepository_Update_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	providerErr := errors.New("provider connection error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	entity := createValidMatchRuleEntity()
	result, err := repo.Update(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to update match rule")
}

func TestRepository_Delete_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	providerErr := errors.New("provider connection error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	err := repo.Delete(ctx, uuid.New(), uuid.New())

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to delete match rule")
}

func TestRepository_CreateWithTx_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	providerErr := errors.New("provider connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	entity := createValidMatchRuleEntity()
	result, err := repo.CreateWithTx(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to create match rule")
}

func TestRepository_UpdateWithTx_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	providerErr := errors.New("provider connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	entity := createValidMatchRuleEntity()
	result, err := repo.UpdateWithTx(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to update match rule")
}

func TestRepository_DeleteWithTx_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	providerErr := errors.New("provider connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	err = repo.DeleteWithTx(ctx, tx, uuid.New(), uuid.New())

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to delete match rule")
}

func TestRepository_ReorderPriorities_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	providerErr := errors.New("provider connection error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	ruleIDs := []uuid.UUID{uuid.New(), uuid.New()}
	err := repo.ReorderPriorities(ctx, uuid.New(), ruleIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, providerErr)
}

func TestRepository_FindByID_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbErr := errors.New("database query error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: dbErr,
	}
	repo := NewRepository(provider)

	result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, dbErr)
}

func TestRepository_FindByContextID_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbErr := errors.New("database query error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: dbErr,
	}
	repo := NewRepository(provider)

	result, _, err := repo.FindByContextID(ctx, uuid.New(), "", 10)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, dbErr)
}

func TestRepository_FindByContextIDAndType_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbErr := errors.New("database query error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: dbErr,
	}
	repo := NewRepository(provider)

	result, _, err := repo.FindByContextIDAndType(ctx, uuid.New(), shared.RuleTypeExact, "", 10)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, dbErr)
}

func TestRepository_FindByPriority_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbErr := errors.New("database query error")

	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: dbErr,
	}
	repo := NewRepository(provider)

	result, err := repo.FindByPriority(ctx, uuid.New(), 1)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, dbErr)
}

func TestScanMatchRule_WithZeroPriority(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 0, "EXACT", []byte(`{}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.NoError(t, err)
	require.Equal(t, 0, result.Priority)
}

func TestScanMatchRule_WithLargePriority(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 999999, "TOLERANCE", []byte(`{"tolerance":0.1}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanMatchRule(sqlRows)
	require.NoError(t, err)
	require.Equal(t, 999999, result.Priority)
	require.Equal(t, shared.RuleTypeTolerance, result.Type)
}

func TestExecuteMatchRulesQuery_WithArgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}).
		AddRow(id.String(), contextID.String(), 1, "EXACT", []byte(`{}`), now, now)
	mock.ExpectQuery("SELECT").WithArgs(contextID.String()).WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := executeMatchRulesQuery(
		ctx,
		tx,
		"SELECT id, context_id, priority, type, config, created_at, updated_at FROM match_rules WHERE context_id = $1",
		[]any{contextID.String()},
	)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, id, result[0].ID)
}

func TestExecuteCreate_WithNilConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    nil,
		CreatedAt: now,
		UpdatedAt: now,
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO match_rules").
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeCreate(ctx, tx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Config)
}

func TestExecuteUpdate_WithTypeChange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  5,
		Type:      shared.RuleTypeDateLag,
		Config:    map[string]any{"maxDays": 7},
		CreatedAt: now,
		UpdatedAt: now,
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_rules").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, shared.RuleTypeDateLag, result.Type)
	require.InDelta(t, 7.0, result.Config["maxDays"], 0.001)
}

func TestNewMatchRulePostgreSQLModel_ZeroTimestamps(t *testing.T) {
	t.Parallel()

	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{},
	}

	model, err := NewMatchRulePostgreSQLModel(entity)
	require.NoError(t, err)
	require.NotNil(t, model)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
	require.Equal(t, model.CreatedAt, model.UpdatedAt)
}

func TestRepository_ReorderPriorities_QueryError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo := NewRepository(provider)

	contextID := uuid.New()
	ruleIDs := []uuid.UUID{uuid.New(), uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE match_rules SET priority").WillReturnError(errors.New("database error"))
	mock.ExpectRollback()

	err = repo.ReorderPriorities(ctx, contextID, ruleIDs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to reorder match rule priorities")
}

func TestToEntity_WithAllFields(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 8, 20, 14, 45, 30, 0, time.UTC)

	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  42,
		Type:      "TOLERANCE",
		Config:    []byte(`{"tolerance":0.05,"strict":true,"fields":["amount"]}`),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	entity, err := model.ToEntity()
	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Equal(t, 42, entity.Priority)
	require.Equal(t, shared.RuleTypeTolerance, entity.Type)
	require.Equal(t, createdAt, entity.CreatedAt)
	require.Equal(t, updatedAt, entity.UpdatedAt)
	require.InDelta(t, 0.05, entity.Config["tolerance"], 0.001)
	require.Equal(t, true, entity.Config["strict"])
}

func TestParseCursor(t *testing.T) {
	t.Parallel()

	t.Run("empty cursor returns default values", func(t *testing.T) {
		t.Parallel()

		cursor, cursorID, err := parseCursor("")

		require.NoError(t, err)
		assert.Equal(t, pkgHTTP.CursorDirectionNext, cursor.Direction)
		assert.Equal(t, uuid.Nil, cursorID)
	})

	t.Run("invalid base64 cursor returns error", func(t *testing.T) {
		t.Parallel()

		_, _, err := parseCursor("not-valid-base64!!!")

		require.Error(t, err)
		require.ErrorIs(t, err, pkgHTTP.ErrInvalidCursor)
	})

	t.Run("valid base64 but invalid JSON cursor returns error", func(t *testing.T) {
		t.Parallel()

		invalidCursor := base64.StdEncoding.EncodeToString([]byte("not-json"))

		_, _, err := parseCursor(invalidCursor)

		require.Error(t, err)
		require.ErrorIs(t, err, pkgHTTP.ErrInvalidCursor)
	})

	t.Run("valid base64 JSON but invalid UUID returns error", func(t *testing.T) {
		t.Parallel()

		cursorJSON := `{"id":"invalid-uuid","direction":"next"}`
		invalidCursor := base64.StdEncoding.EncodeToString([]byte(cursorJSON))

		_, _, err := parseCursor(invalidCursor)

		require.Error(t, err)
		require.ErrorIs(t, err, pkgHTTP.ErrInvalidCursor)
	})

	t.Run("valid cursor with Direction next", func(t *testing.T) {
		t.Parallel()

		expectedID := testutil.DeterministicUUID("cursor-points-next-true")
		cursorJSON := `{"id":"` + expectedID.String() + `","direction":"next"}`
		validCursor := base64.StdEncoding.EncodeToString([]byte(cursorJSON))

		cursor, cursorID, err := parseCursor(validCursor)

		require.NoError(t, err)
		assert.Equal(t, pkgHTTP.CursorDirectionNext, cursor.Direction)
		assert.Equal(t, expectedID, cursorID)
		assert.Equal(t, expectedID.String(), cursor.ID)
	})

	t.Run("valid cursor with Direction prev", func(t *testing.T) {
		t.Parallel()

		expectedID := testutil.DeterministicUUID("cursor-points-next-false")
		cursorJSON := `{"id":"` + expectedID.String() + `","direction":"prev"}`
		validCursor := base64.StdEncoding.EncodeToString([]byte(cursorJSON))

		cursor, cursorID, err := parseCursor(validCursor)

		require.NoError(t, err)
		assert.Equal(t, pkgHTTP.CursorDirectionPrev, cursor.Direction)
		assert.Equal(t, expectedID, cursorID)
	})
}
