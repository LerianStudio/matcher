// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestFeeRuleToResponse_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ruleID := uuid.New()
	contextID := uuid.New()
	scheduleID := uuid.New()

	rule := &fee.FeeRule{
		ID:            ruleID,
		ContextID:     contextID,
		Side:          fee.MatchingSideRight,
		FeeScheduleID: scheduleID,
		Name:          "BB Right-Side Rule",
		Priority:      0,
		Predicates: []fee.FieldPredicate{
			{
				Field:    "institution",
				Operator: fee.PredicateOperatorEquals,
				Value:    "Banco do Brasil",
			},
			{
				Field:    "region",
				Operator: fee.PredicateOperatorIn,
				Values:   []string{"BR", "AR", "CL"},
			},
			{
				Field:    "flagged",
				Operator: fee.PredicateOperatorExists,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	resp := FeeRuleToResponse(rule)

	assert.Equal(t, ruleID.String(), resp.ID)
	assert.Equal(t, contextID.String(), resp.ContextID)
	assert.Equal(t, "RIGHT", resp.Side)
	assert.Equal(t, scheduleID.String(), resp.FeeScheduleID)
	assert.Equal(t, "BB Right-Side Rule", resp.Name)
	assert.Equal(t, 0, resp.Priority)
	assert.Equal(t, now.Format(time.RFC3339), resp.CreatedAt)
	assert.Equal(t, now.Format(time.RFC3339), resp.UpdatedAt)

	require.Len(t, resp.Predicates, 3)

	assert.Equal(t, "institution", resp.Predicates[0].Field)
	assert.Equal(t, "EQUALS", resp.Predicates[0].Operator)
	assert.Equal(t, "Banco do Brasil", resp.Predicates[0].Value)
	assert.Empty(t, resp.Predicates[0].Values)

	assert.Equal(t, "region", resp.Predicates[1].Field)
	assert.Equal(t, "IN", resp.Predicates[1].Operator)
	assert.Empty(t, resp.Predicates[1].Value)
	assert.Equal(t, []string{"BR", "AR", "CL"}, resp.Predicates[1].Values)

	assert.Equal(t, "flagged", resp.Predicates[2].Field)
	assert.Equal(t, "EXISTS", resp.Predicates[2].Operator)
}

func TestFeeRuleToResponse_NilRule(t *testing.T) {
	t.Parallel()

	resp := FeeRuleToResponse(nil)

	assert.Empty(t, resp.ID)
	assert.Empty(t, resp.ContextID)
	assert.Empty(t, resp.Side)
	assert.Empty(t, resp.FeeScheduleID)
	assert.Empty(t, resp.Name)
	assert.Equal(t, 0, resp.Priority)
	assert.Empty(t, resp.CreatedAt)
	assert.Empty(t, resp.UpdatedAt)
	assert.NotNil(t, resp.Predicates, "predicates should be initialized, not nil")
	assert.Empty(t, resp.Predicates)
}

func TestFeeRuleToResponse_NoPredicates(t *testing.T) {
	t.Parallel()

	rule := &fee.FeeRule{
		ID:            uuid.New(),
		ContextID:     uuid.New(),
		Side:          fee.MatchingSideAny,
		FeeScheduleID: uuid.New(),
		Name:          "Catch-all rule",
		Priority:      99,
		Predicates:    nil,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}

	resp := FeeRuleToResponse(rule)

	assert.Equal(t, "ANY", resp.Side)
	assert.Equal(t, "Catch-all rule", resp.Name)
	assert.NotNil(t, resp.Predicates)
	assert.Empty(t, resp.Predicates)
}

func TestFeeRulesToResponse_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	rules := []*fee.FeeRule{
		{
			ID:            uuid.New(),
			ContextID:     uuid.New(),
			Side:          fee.MatchingSideLeft,
			FeeScheduleID: uuid.New(),
			Name:          "Rule A",
			Priority:      0,
			Predicates: []fee.FieldPredicate{
				{Field: "type", Operator: fee.PredicateOperatorEquals, Value: "credit"},
			},
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			ID:            uuid.New(),
			ContextID:     uuid.New(),
			Side:          fee.MatchingSideRight,
			FeeScheduleID: uuid.New(),
			Name:          "Rule B",
			Priority:      1,
			Predicates:    nil,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
	}

	responses := FeeRulesToResponse(rules)

	require.Len(t, responses, 2)
	assert.Equal(t, "Rule A", responses[0].Name)
	assert.Equal(t, "LEFT", responses[0].Side)
	assert.Len(t, responses[0].Predicates, 1)
	assert.Equal(t, "Rule B", responses[1].Name)
	assert.Equal(t, "RIGHT", responses[1].Side)
}

func TestFeeRulesToResponse_NilElements(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	rules := []*fee.FeeRule{
		nil,
		{
			ID:            uuid.New(),
			ContextID:     uuid.New(),
			Side:          fee.MatchingSideAny,
			FeeScheduleID: uuid.New(),
			Name:          "Valid Rule",
			Priority:      0,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		nil,
	}

	responses := FeeRulesToResponse(rules)

	require.Len(t, responses, 1, "nil elements should be filtered out")
	assert.Equal(t, "Valid Rule", responses[0].Name)
}

func TestFeeRulesToResponse_Empty(t *testing.T) {
	t.Parallel()

	responses := FeeRulesToResponse([]*fee.FeeRule{})

	assert.NotNil(t, responses)
	assert.Empty(t, responses)
}

func TestFeeRulesToResponse_NilSlice(t *testing.T) {
	t.Parallel()

	responses := FeeRulesToResponse(nil)

	assert.NotNil(t, responses)
	assert.Empty(t, responses)
}

func TestToPredicates_Success(t *testing.T) {
	t.Parallel()

	reqs := []FieldPredicateRequest{
		{
			Field:    "institution",
			Operator: "EQUALS",
			Value:    "Banco do Brasil",
		},
		{
			Field:    "region",
			Operator: "IN",
			Values:   []string{"BR", "AR"},
		},
		{
			Field:    "flagged",
			Operator: "EXISTS",
		},
	}

	predicates := ToPredicates(reqs)

	require.Len(t, predicates, 3)

	assert.Equal(t, "institution", predicates[0].Field)
	assert.Equal(t, fee.PredicateOperatorEquals, predicates[0].Operator)
	assert.Equal(t, "Banco do Brasil", predicates[0].Value)
	assert.Empty(t, predicates[0].Values)

	assert.Equal(t, "region", predicates[1].Field)
	assert.Equal(t, fee.PredicateOperatorIn, predicates[1].Operator)
	assert.Empty(t, predicates[1].Value)
	assert.Equal(t, []string{"BR", "AR"}, predicates[1].Values)

	assert.Equal(t, "flagged", predicates[2].Field)
	assert.Equal(t, fee.PredicateOperatorExists, predicates[2].Operator)
}

func TestToPredicates_NilInput(t *testing.T) {
	t.Parallel()

	predicates := ToPredicates(nil)

	assert.Nil(t, predicates)
}

func TestToPredicates_EmptyInput(t *testing.T) {
	t.Parallel()

	predicates := ToPredicates([]FieldPredicateRequest{})

	assert.Nil(t, predicates, "empty input should return nil, same as nil input")
}
