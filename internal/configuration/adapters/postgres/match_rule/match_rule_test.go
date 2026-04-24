// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package match_rule

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestMatchRulePostgreSQLModelRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"matchScore": 100},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)
	require.NoError(t, err)

	out, err := model.ToEntity()
	require.NoError(t, err)
	require.Equal(t, entity.ID, out.ID)
	require.Equal(t, entity.ContextID, out.ContextID)
	require.Equal(t, entity.Priority, out.Priority)
	require.Equal(t, entity.Type, out.Type)
	require.InDelta(t, float64(100), out.Config["matchScore"], 0.01)
}

func TestMatchRulePostgreSQLModelDefaults(t *testing.T) {
	t.Parallel()

	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"matchScore": 100},
	}

	model, err := NewMatchRulePostgreSQLModel(entity)
	require.NoError(t, err)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
}

func TestNewMatchRulePostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewMatchRulePostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchRuleEntityRequired)
}

func TestNewMatchRulePostgreSQLModel_GeneratesIDWhenNil(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.Nil,
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{"field": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEqual(t, uuid.Nil, model.ID)
}

func TestNewMatchRulePostgreSQLModel_NilContextID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.Nil,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchRuleContextIDRequired)
}

func TestNewMatchRulePostgreSQLModel_NilConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeTolerance,
		Config:    nil,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.Config)
}

func TestNewMatchRulePostgreSQLModel_EmptyConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  5,
		Type:      shared.RuleTypeDateLag,
		Config:    map[string]any{},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.Config)
	assert.Equal(t, "DATE_LAG", model.Type)
	assert.Equal(t, 5, model.Priority)
}

func TestNewMatchRulePostgreSQLModel_ComplexConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  10,
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

	model, err := NewMatchRulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.Config)
	assert.Equal(t, "TOLERANCE", model.Type)
}

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *MatchRulePostgreSQLModel
	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrMatchRuleModelRequired)
}

func TestToEntity_InvalidType(t *testing.T) {
	t.Parallel()

	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Type:      "INVALID_TYPE",
		Config:    []byte(`{}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse rule type")
}

func TestToEntity_InvalidConfigJSON(t *testing.T) {
	t.Parallel()

	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Type:      "EXACT",
		Config:    []byte(`{invalid json}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "unmarshal config")
}

func TestToEntity_EmptyConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      "TOLERANCE",
		Config:    []byte{},
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Config)
	require.Empty(t, entity.Config)
}

func TestToEntity_NilConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  2,
		Type:      "DATE_LAG",
		Config:    nil,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Config)
	require.Empty(t, entity.Config)
}

func TestToEntity_ValidWithComplexConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  5,
		Type:      "EXACT",
		Config:    []byte(`{"tolerance":0.1,"strictMode":true,"fields":["amount","date"]}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, 5, entity.Priority)
	assert.Equal(t, shared.RuleTypeExact, entity.Type)
	assert.Len(t, entity.Config, 3)
	assert.InDelta(t, 0.1, entity.Config["tolerance"], 0.001)
	assert.Equal(t, true, entity.Config["strictMode"])
}

func TestToEntity_AllRuleTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		typeStr      string
		expectedType shared.RuleType
	}{
		{"EXACT", "EXACT", shared.RuleTypeExact},
		{"TOLERANCE", "TOLERANCE", shared.RuleTypeTolerance},
		{"DATE_LAG", "DATE_LAG", shared.RuleTypeDateLag},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			model := &MatchRulePostgreSQLModel{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				Priority:  1,
				Type:      tt.typeStr,
				Config:    []byte(`{}`),
				CreatedAt: now,
				UpdatedAt: now,
			}

			entity, err := model.ToEntity()

			require.NoError(t, err)
			require.NotNil(t, entity)
			assert.Equal(t, tt.expectedType, entity.Type)
		})
	}
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrMatchRuleEntityRequired", ErrMatchRuleEntityRequired},
		{"ErrMatchRuleModelRequired", ErrMatchRuleModelRequired},
		{"ErrMatchRuleContextIDRequired", ErrMatchRuleContextIDRequired},
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
		{"ErrRuleIDsRequired", ErrRuleIDsRequired},
		{"ErrCursorNotFound", ErrCursorNotFound},
		{"ErrTransactionRequired", ErrTransactionRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestNewMatchRulePostgreSQLModel_ZeroUpdatedAt(t *testing.T) {
	t.Parallel()

	createdAt := time.Now().UTC()
	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    map[string]any{},
		CreatedAt: createdAt,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, createdAt, model.CreatedAt)
	require.Equal(t, createdAt, model.UpdatedAt)
}

func TestNewMatchRulePostgreSQLModel_AllRuleTypes(t *testing.T) {
	t.Parallel()

	ruleTypes := []struct {
		ruleType    shared.RuleType
		expectedStr string
	}{
		{shared.RuleTypeExact, "EXACT"},
		{shared.RuleTypeTolerance, "TOLERANCE"},
		{shared.RuleTypeDateLag, "DATE_LAG"},
	}

	for _, tt := range ruleTypes {
		t.Run(tt.expectedStr, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			entity := &entities.MatchRule{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				Priority:  1,
				Type:      tt.ruleType,
				Config:    map[string]any{},
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewMatchRulePostgreSQLModel(entity)

			require.NoError(t, err)
			require.NotNil(t, model)
			require.Equal(t, tt.expectedStr, model.Type)
		})
	}
}

func TestToEntity_ConfigWithNullValue(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      "EXACT",
		Config:    []byte(`{"key":null,"nested":{"value":null}}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Nil(t, entity.Config["key"])
}

func TestModelPreservesAllFields(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2023, 5, 15, 10, 30, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 8, 20, 14, 45, 30, 0, time.UTC)
	entityID := uuid.New()
	contextID := uuid.New()

	entity := &entities.MatchRule{
		ID:        entityID,
		ContextID: contextID,
		Priority:  42,
		Type:      shared.RuleTypeTolerance,
		Config: map[string]any{
			"tolerance": float64(0.05),
			"strict":    true,
		},
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)

	require.Equal(t, entityID, resultEntity.ID)
	require.Equal(t, contextID, resultEntity.ContextID)
	require.Equal(t, 42, resultEntity.Priority)
	require.Equal(t, shared.RuleTypeTolerance, resultEntity.Type)
	require.Equal(t, createdAt, resultEntity.CreatedAt)
	require.Equal(t, updatedAt, resultEntity.UpdatedAt)
	require.InDelta(t, 0.05, resultEntity.Config["tolerance"], 0.001)
	require.Equal(t, true, resultEntity.Config["strict"])
}

func TestToEntity_PriorityZero(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  0,
		Type:      "EXACT",
		Config:    []byte(`{}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Equal(t, 0, entity.Priority)
}

func TestToEntity_NegativePriority(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &MatchRulePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  -5,
		Type:      "TOLERANCE",
		Config:    []byte(`{}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Equal(t, -5, entity.Priority)
}

func TestToEntity_LargeConfigPayload(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	largeConfig := make(map[string]any)
	for i := 0; i < 100; i++ {
		key := "field_" + string(rune('a'+i%26)) + "_" + string(rune('0'+i/26))
		largeConfig[key] = "value_" + key
	}

	entity := &entities.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config:    largeConfig,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)
	require.Len(t, resultEntity.Config, 100)
}

func TestBuildReorderQuery_EmptyRuleIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ruleIDs := []uuid.UUID{}

	query, args := buildReorderQuery(contextID, ruleIDs)

	require.Contains(t, query, "UPDATE match_rules SET priority = CASE id")
	require.Len(t, args, 1)
	require.Equal(t, contextID.String(), args[0])
}

func TestBuildReorderQuery_SingleRuleID(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ruleID := uuid.New()
	ruleIDs := []uuid.UUID{ruleID}

	query, args := buildReorderQuery(contextID, ruleIDs)

	require.Contains(t, query, "UPDATE match_rules SET priority = CASE id")
	require.Contains(t, query, "WHEN $2 THEN $3::int")
	require.Contains(t, query, "WHERE context_id = $1 AND id IN ($4)")
	require.Len(t, args, 4)
	require.Equal(t, contextID.String(), args[0])
	require.Equal(t, ruleID.String(), args[1])
	require.Equal(t, 1, args[2])
	require.Equal(t, ruleID.String(), args[3])
}

func TestBuildReorderQuery_LargeNumberOfRuleIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ruleIDs := make([]uuid.UUID, 50)
	for i := range ruleIDs {
		ruleIDs[i] = uuid.New()
	}

	query, args := buildReorderQuery(contextID, ruleIDs)

	require.Contains(t, query, "UPDATE match_rules SET priority = CASE id")
	require.Len(t, args, 1+(50*3))

	for i := 0; i < 50; i++ {
		require.Equal(t, i+1, args[2+(i*2)])
	}
}
