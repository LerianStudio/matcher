// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fee_rule

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func validFeeRuleEntity(t *testing.T) *fee.FeeRule {
	t.Helper()

	now := time.Now().UTC()

	return &fee.FeeRule{
		ID:            uuid.New(),
		ContextID:     uuid.New(),
		Side:          fee.MatchingSideLeft,
		FeeScheduleID: uuid.New(),
		Name:          "flat-fee-usd",
		Priority:      10,
		Predicates: []fee.FieldPredicate{
			{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestNewPostgreSQLModel_Success(t *testing.T) {
	t.Parallel()

	entity := validFeeRuleEntity(t)

	model, err := NewPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, entity.ID, model.ID)
	assert.Equal(t, entity.ContextID, model.ContextID)
	assert.Equal(t, string(entity.Side), model.Side)
	assert.Equal(t, entity.FeeScheduleID, model.FeeScheduleID)
	assert.Equal(t, entity.Name, model.Name)
	assert.Equal(t, entity.Priority, model.Priority)
	assert.Equal(t, entity.CreatedAt, model.CreatedAt)
	assert.Equal(t, entity.UpdatedAt, model.UpdatedAt)

	// Predicates should be valid JSON.
	var predicates []fee.FieldPredicate
	require.NoError(t, json.Unmarshal(model.Predicates, &predicates))
	require.Len(t, predicates, 1)
	assert.Equal(t, "currency", predicates[0].Field)
	assert.Equal(t, fee.PredicateOperatorEquals, predicates[0].Operator)
	assert.Equal(t, "USD", predicates[0].Value)
}

func TestNewPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewPostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrFeeRuleEntityNil)
}

func TestNewPostgreSQLModel_NilID(t *testing.T) {
	t.Parallel()

	entity := validFeeRuleEntity(t)
	entity.ID = uuid.Nil

	model, err := NewPostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrFeeRuleEntityIDNil)
}

func TestNewPostgreSQLModel_EmptyPredicates(t *testing.T) {
	t.Parallel()

	entity := validFeeRuleEntity(t)
	entity.Predicates = nil

	model, err := NewPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, []byte("null"), model.Predicates)
}

func TestNewPostgreSQLModel_MultiplePredicates(t *testing.T) {
	t.Parallel()

	entity := validFeeRuleEntity(t)
	entity.Predicates = []fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
		{Field: "type", Operator: fee.PredicateOperatorIn, Values: []string{"WIRE", "ACH"}},
		{Field: "priority_flag", Operator: fee.PredicateOperatorExists},
	}

	model, err := NewPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)

	var predicates []fee.FieldPredicate
	require.NoError(t, json.Unmarshal(model.Predicates, &predicates))
	require.Len(t, predicates, 3)
}

func TestToEntity_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	feeScheduleID := uuid.New()
	predicatesJSON, err := json.Marshal([]fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "BRL"},
	})
	require.NoError(t, err)

	model := &PostgreSQLModel{
		ID:            id,
		ContextID:     contextID,
		Side:          "RIGHT",
		FeeScheduleID: feeScheduleID,
		Name:          "percentage-brl",
		Priority:      5,
		Predicates:    predicatesJSON,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, id, entity.ID)
	assert.Equal(t, contextID, entity.ContextID)
	assert.Equal(t, fee.MatchingSideRight, entity.Side)
	assert.Equal(t, feeScheduleID, entity.FeeScheduleID)
	assert.Equal(t, "percentage-brl", entity.Name)
	assert.Equal(t, 5, entity.Priority)
	require.Len(t, entity.Predicates, 1)
	assert.Equal(t, "currency", entity.Predicates[0].Field)
	assert.Equal(t, "BRL", entity.Predicates[0].Value)
	assert.Equal(t, now, entity.CreatedAt)
	assert.Equal(t, now, entity.UpdatedAt)
}

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *PostgreSQLModel

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrFeeRuleModelNeeded)
}

func TestToEntity_InvalidPredicatesJSON(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	contextID := uuid.New()
	feeScheduleID := uuid.New()
	now := time.Now().UTC()

	model := &PostgreSQLModel{
		ID:            id,
		ContextID:     contextID,
		Side:          "LEFT",
		FeeScheduleID: feeScheduleID,
		Name:          "broken-predicates",
		Priority:      1,
		Predicates:    []byte(`{not valid json`),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	assert.Contains(t, err.Error(), "unmarshal predicates")
}

func TestToEntity_EmptyPredicatesSlice(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	model := &PostgreSQLModel{
		ID:            uuid.New(),
		ContextID:     uuid.New(),
		Side:          "ANY",
		FeeScheduleID: uuid.New(),
		Name:          "no-predicates",
		Priority:      0,
		Predicates:    nil,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Empty(t, entity.Predicates)
}

func TestFromEntity_DelegatesToNewPostgreSQLModel(t *testing.T) {
	t.Parallel()

	entity := validFeeRuleEntity(t)

	model, err := FromEntity(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, entity.ID, model.ID)
}

func TestFromEntity_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := FromEntity(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrFeeRuleEntityNil)
}

func TestRoundTrip_EntityToModelToEntity(t *testing.T) {
	t.Parallel()

	original := validFeeRuleEntity(t)

	model, err := NewPostgreSQLModel(original)
	require.NoError(t, err)

	restored, err := model.ToEntity()
	require.NoError(t, err)

	assert.Equal(t, original.ID, restored.ID)
	assert.Equal(t, original.ContextID, restored.ContextID)
	assert.Equal(t, original.Side, restored.Side)
	assert.Equal(t, original.FeeScheduleID, restored.FeeScheduleID)
	assert.Equal(t, original.Name, restored.Name)
	assert.Equal(t, original.Priority, restored.Priority)
	assert.Equal(t, original.CreatedAt, restored.CreatedAt)
	assert.Equal(t, original.UpdatedAt, restored.UpdatedAt)
	require.Len(t, restored.Predicates, len(original.Predicates))
	assert.Equal(t, original.Predicates[0].Field, restored.Predicates[0].Field)
	assert.Equal(t, original.Predicates[0].Operator, restored.Predicates[0].Operator)
	assert.Equal(t, original.Predicates[0].Value, restored.Predicates[0].Value)
}

func TestRoundTrip_PreservesAllSides(t *testing.T) {
	t.Parallel()

	sides := []fee.MatchingSide{
		fee.MatchingSideLeft,
		fee.MatchingSideRight,
		fee.MatchingSideAny,
	}

	for _, side := range sides {
		t.Run(string(side), func(t *testing.T) {
			t.Parallel()

			entity := validFeeRuleEntity(t)
			entity.Side = side

			model, err := NewPostgreSQLModel(entity)
			require.NoError(t, err)

			restored, err := model.ToEntity()
			require.NoError(t, err)

			assert.Equal(t, side, restored.Side)
		})
	}
}

func TestRoundTrip_PreservesAllOperatorTypes(t *testing.T) {
	t.Parallel()

	entity := validFeeRuleEntity(t)
	entity.Predicates = []fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
		{Field: "type", Operator: fee.PredicateOperatorIn, Values: []string{"WIRE", "ACH", "SWIFT"}},
		{Field: "high_value", Operator: fee.PredicateOperatorExists},
	}

	model, err := NewPostgreSQLModel(entity)
	require.NoError(t, err)

	restored, err := model.ToEntity()
	require.NoError(t, err)

	require.Len(t, restored.Predicates, 3)
	assert.Equal(t, fee.PredicateOperatorEquals, restored.Predicates[0].Operator)
	assert.Equal(t, "USD", restored.Predicates[0].Value)
	assert.Equal(t, fee.PredicateOperatorIn, restored.Predicates[1].Operator)
	assert.Equal(t, []string{"WIRE", "ACH", "SWIFT"}, restored.Predicates[1].Values)
	assert.Equal(t, fee.PredicateOperatorExists, restored.Predicates[2].Operator)
}
