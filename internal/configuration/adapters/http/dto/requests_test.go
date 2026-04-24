// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestCreateContextRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("all fields populated", func(t *testing.T) {
		t.Parallel()

		abs := "0.01"
		pct := "0.5"
		norm := "NET"
		auto := false

		req := CreateContextRequest{
			Name:              "Bank Reconciliation Q1",
			Type:              "1:1",
			Interval:          "daily",
			FeeToleranceAbs:   &abs,
			FeeTolerancePct:   &pct,
			FeeNormalization:  &norm,
			AutoMatchOnUpload: &auto,
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)

		assert.Equal(t, "Bank Reconciliation Q1", input.Name)
		assert.Equal(t, shared.ContextType("1:1"), input.Type)
		assert.Equal(t, "daily", input.Interval)
		assert.Equal(t, &abs, input.FeeToleranceAbs)
		assert.Equal(t, &pct, input.FeeTolerancePct)
		assert.Equal(t, &norm, input.FeeNormalization)
		assert.Equal(t, &auto, input.AutoMatchOnUpload)
	})

	t.Run("minimal fields", func(t *testing.T) {
		t.Parallel()

		req := CreateContextRequest{
			Name:     "Minimal",
			Type:     "N:M",
			Interval: "weekly",
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)

		assert.Equal(t, "Minimal", input.Name)
		assert.Equal(t, shared.ContextType("N:M"), input.Type)
		assert.Nil(t, input.FeeToleranceAbs)
		assert.Nil(t, input.AutoMatchOnUpload)
	})

	t.Run("with nested sources and rules", func(t *testing.T) {
		t.Parallel()

		req := CreateContextRequest{
			Name:     "Context With Nested",
			Type:     "1:N",
			Interval: "daily",
			Sources: []CreateContextSourceRequest{
				{
					Name:    "Bank Source",
					Type:    "BANK",
					Config:  map[string]any{"format": "csv"},
					Mapping: map[string]any{"amount": "amt"},
				},
			},
			Rules: []CreateMatchRuleRequest{
				{
					Priority: 1,
					Type:     "EXACT",
					Config:   map[string]any{"matchAmount": true},
				},
			},
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)
		require.Len(t, input.Sources, 1)
		require.Len(t, input.Rules, 1)
		assert.Equal(t, "Bank Source", input.Sources[0].Name)
		assert.Equal(t, shared.RuleType("EXACT"), input.Rules[0].Type)
	})

	t.Run("inline rule config above max size returns error", func(t *testing.T) {
		t.Parallel()

		req := CreateContextRequest{
			Name:     "Context With Large Rule Config",
			Type:     "1:1",
			Interval: "daily",
			Rules: []CreateMatchRuleRequest{{
				Priority: 1,
				Type:     "EXACT",
				Config: map[string]any{
					"payload": strings.Repeat("x", maxJSONFieldBytes+1024),
				},
			}},
		}

		_, err := req.ToDomainInput()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rule.config exceeds maximum size")
	})
}

func TestCreateContextRequest_UnmarshalJSON_RejectsDeprecatedRateID(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"name":"Context","type":"1:1","interval":"daily","rateId":"550e8400-e29b-41d4-a716-446655440000"}`)
	var req CreateContextRequest
	err := json.Unmarshal(raw, &req)
	require.ErrorIs(t, err, ErrDeprecatedRateID)
}

func TestCreateContextRequest_UnmarshalJSON_RejectsDeprecatedRateIDPresenceVariants(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		[]byte(`{"name":"Context","type":"1:1","interval":"daily","rateId":null}`),
		[]byte(`{"name":"Context","type":"1:1","interval":"daily","rateId":{"legacy":true}}`),
	} {
		var req CreateContextRequest
		err := json.Unmarshal(raw, &req)
		require.ErrorIs(t, err, ErrDeprecatedRateID)
	}
}

func TestCreateContextRequest_UnmarshalJSON_ValidPayload(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"name":"Context",
		"type":"1:1",
		"interval":"daily",
		"feeToleranceAbs":"0.01",
		"feeNormalization":"NET",
		"autoMatchOnUpload":true
	}`)

	var req CreateContextRequest
	err := json.Unmarshal(raw, &req)
	require.NoError(t, err)
	require.Equal(t, "Context", req.Name)
	require.Equal(t, "1:1", req.Type)
	require.Equal(t, "daily", req.Interval)
	require.NotNil(t, req.FeeToleranceAbs)
	require.Equal(t, "0.01", *req.FeeToleranceAbs)
	require.NotNil(t, req.FeeNormalization)
	require.Equal(t, "NET", *req.FeeNormalization)
	require.NotNil(t, req.AutoMatchOnUpload)
	assert.True(t, *req.AutoMatchOnUpload)
}

func TestCreateContextSourceRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("valid input", func(t *testing.T) {
		t.Parallel()

		req := CreateContextSourceRequest{
			Name:    "Gateway Source",
			Type:    "GATEWAY",
			Config:  map[string]any{"url": "https://gateway"},
			Mapping: map[string]any{"externalId": "id"},
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)
		assert.Equal(t, "Gateway Source", input.Name)
		assert.Equal(t, value_objects.SourceType("GATEWAY"), input.Type)
		assert.Equal(t, "id", input.Mapping["externalId"])
	})

	t.Run("whitespace-only name returns error", func(t *testing.T) {
		t.Parallel()

		req := CreateContextSourceRequest{
			Name: "   ",
			Type: "BANK",
			Side: "LEFT",
		}

		_, err := req.ToDomainInput()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNameWhitespaceOnly)
	})

	t.Run("trims whitespace from name", func(t *testing.T) {
		t.Parallel()

		req := CreateContextSourceRequest{
			Name: "  Inline Source  ",
			Type: "BANK",
			Side: "LEFT",
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)
		assert.Equal(t, "Inline Source", input.Name)
	})
}

func TestCreateContextRequest_Validation_DivesIntoNestedCollections(t *testing.T) {
	t.Parallel()

	v := validator.New()
	req := CreateContextRequest{
		Name:     "Nested Validation",
		Type:     "1:1",
		Interval: "daily",
		Sources: []CreateContextSourceRequest{
			{Type: "BANK"}, // missing required Name
		},
		Rules: []CreateMatchRuleRequest{
			{Priority: 0, Type: "EXACT"}, // invalid min priority
		},
	}

	err := v.Struct(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CreateContextRequest.Sources[0].Name")
	assert.Contains(t, err.Error(), "CreateContextRequest.Rules[0].Priority")
}

func TestCreateContextRequest_Validation_SourcesAndRulesBoundary(t *testing.T) {
	t.Parallel()

	v := validator.New()

	t.Run("exactly 10 sources passes validation", func(t *testing.T) {
		t.Parallel()

		sources := make([]CreateContextSourceRequest, 10)
		for i := range sources {
			sources[i] = CreateContextSourceRequest{
				Name: fmt.Sprintf("Source %d", i+1),
				Type: "BANK",
				Side: "LEFT",
			}
		}

		req := CreateContextRequest{
			Name:     "Boundary 10 Sources",
			Type:     "1:1",
			Interval: "daily",
			Sources:  sources,
		}

		err := v.Struct(req)
		require.NoError(t, err)
	})

	t.Run("11 sources fails validation", func(t *testing.T) {
		t.Parallel()

		sources := make([]CreateContextSourceRequest, 11)
		for i := range sources {
			sources[i] = CreateContextSourceRequest{
				Name: fmt.Sprintf("Source %d", i+1),
				Type: "BANK",
				Side: "LEFT",
			}
		}

		req := CreateContextRequest{
			Name:     "Over Limit 11 Sources",
			Type:     "1:1",
			Interval: "daily",
			Sources:  sources,
		}

		err := v.Struct(req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Sources")
		assert.Contains(t, err.Error(), "max")
	})

	t.Run("exactly 50 rules passes validation", func(t *testing.T) {
		t.Parallel()

		rules := make([]CreateMatchRuleRequest, 50)
		for i := range rules {
			rules[i] = CreateMatchRuleRequest{
				Priority: i + 1,
				Type:     "EXACT",
				Config:   map[string]any{"matchAmount": true},
			}
		}

		req := CreateContextRequest{
			Name:     "Boundary 50 Rules",
			Type:     "1:1",
			Interval: "daily",
			Rules:    rules,
		}

		err := v.Struct(req)
		require.NoError(t, err)
	})

	t.Run("51 rules fails validation", func(t *testing.T) {
		t.Parallel()

		rules := make([]CreateMatchRuleRequest, 51)
		for i := range rules {
			rules[i] = CreateMatchRuleRequest{
				Priority: i + 1,
				Type:     "EXACT",
				Config:   map[string]any{"matchAmount": true},
			}
		}

		req := CreateContextRequest{
			Name:     "Over Limit 51 Rules",
			Type:     "1:1",
			Interval: "daily",
			Rules:    rules,
		}

		err := v.Struct(req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Rules")
		assert.Contains(t, err.Error(), "max")
	})
}

func TestUpdateContextRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("all fields populated", func(t *testing.T) {
		t.Parallel()

		name := "Updated Name"
		typ := "1:N"
		interval := "weekly"
		status := "PAUSED"

		req := UpdateContextRequest{
			Name:     &name,
			Type:     &typ,
			Interval: &interval,
			Status:   &status,
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)

		assert.Equal(t, &name, input.Name)
		assert.NotNil(t, input.Type)
		assert.Equal(t, shared.ContextType("1:N"), *input.Type)
		assert.NotNil(t, input.Status)
		assert.Equal(t, value_objects.ContextStatus("PAUSED"), *input.Status)
	})

	t.Run("nil fields stay nil", func(t *testing.T) {
		t.Parallel()

		req := UpdateContextRequest{}
		input, err := req.ToDomainInput()
		require.NoError(t, err)

		assert.Nil(t, input.Name)
		assert.Nil(t, input.Type)
		assert.Nil(t, input.Status)
	})

	t.Run("status ACTIVE converts correctly", func(t *testing.T) {
		t.Parallel()

		status := "ACTIVE"
		req := UpdateContextRequest{Status: &status}
		input, err := req.ToDomainInput()
		require.NoError(t, err)
		assert.NotNil(t, input.Status)
		assert.Equal(t, value_objects.ContextStatus("ACTIVE"), *input.Status)
	})

	t.Run("status ARCHIVED converts correctly", func(t *testing.T) {
		t.Parallel()

		status := "ARCHIVED"
		req := UpdateContextRequest{Status: &status}
		input, err := req.ToDomainInput()
		require.NoError(t, err)
		assert.NotNil(t, input.Status)
		assert.Equal(t, value_objects.ContextStatus("ARCHIVED"), *input.Status)
	})
}

func TestUpdateContextRequest_UnmarshalJSON_RejectsDeprecatedRateID(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"status":"ACTIVE","rateId":"550e8400-e29b-41d4-a716-446655440000"}`)
	var req UpdateContextRequest
	err := json.Unmarshal(raw, &req)
	require.ErrorIs(t, err, ErrDeprecatedRateID)
}

func TestUpdateContextRequest_UnmarshalJSON_RejectsDeprecatedRateIDPresenceVariants(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		[]byte(`{"status":"ACTIVE","rateId":null}`),
		[]byte(`{"status":"ACTIVE","rateId":{"legacy":true}}`),
	} {
		var req UpdateContextRequest
		err := json.Unmarshal(raw, &req)
		require.ErrorIs(t, err, ErrDeprecatedRateID)
	}
}

func TestUpdateContextRequest_UnmarshalJSON_ValidPayload(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"name":"Updated Context",
		"status":"ACTIVE",
		"feeTolerancePct":"0.5",
		"autoMatchOnUpload":false
	}`)

	var req UpdateContextRequest
	err := json.Unmarshal(raw, &req)
	require.NoError(t, err)
	require.NotNil(t, req.Name)
	require.Equal(t, "Updated Context", *req.Name)
	require.NotNil(t, req.Status)
	require.Equal(t, "ACTIVE", *req.Status)
	require.NotNil(t, req.FeeTolerancePct)
	require.Equal(t, "0.5", *req.FeeTolerancePct)
	require.NotNil(t, req.AutoMatchOnUpload)
	assert.False(t, *req.AutoMatchOnUpload)
}

func TestUpdateContextRequest_StatusValidation_RejectsDraft(t *testing.T) {
	t.Parallel()

	v := validator.New()
	status := "DRAFT"
	req := UpdateContextRequest{Status: &status}

	err := v.Struct(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Status")
	assert.Contains(t, err.Error(), "oneof")
}

func TestCreateSourceRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("with all fields", func(t *testing.T) {
		t.Parallel()

		req := CreateSourceRequest{
			Name:   "Primary Bank",
			Type:   "BANK",
			Side:   "LEFT",
			Config: map[string]any{"key": "value"},
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)

		assert.Equal(t, "Primary Bank", input.Name)
		assert.Equal(t, value_objects.SourceType("BANK"), input.Type)
		assert.Equal(t, sharedfee.MatchingSideLeft, input.Side)
		assert.Equal(t, map[string]any{"key": "value"}, input.Config)
	})

	t.Run("without optional fields", func(t *testing.T) {
		t.Parallel()

		req := CreateSourceRequest{
			Name: "Minimal",
			Type: "LEDGER",
			Side: "RIGHT",
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)
		assert.Nil(t, input.Config)
		assert.Equal(t, sharedfee.MatchingSideRight, input.Side)
	})

	t.Run("whitespace-only name returns error", func(t *testing.T) {
		t.Parallel()

		req := CreateSourceRequest{
			Name: "   ",
			Type: "BANK",
			Side: "LEFT",
		}

		_, err := req.ToDomainInput()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNameWhitespaceOnly)
	})

	t.Run("trims leading and trailing whitespace from name", func(t *testing.T) {
		t.Parallel()

		req := CreateSourceRequest{
			Name: "  Bank Source  ",
			Type: "BANK",
			Side: "LEFT",
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)
		assert.Equal(t, "Bank Source", input.Name)
	})
}

func TestUpdateSourceRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("with type update", func(t *testing.T) {
		t.Parallel()

		name := "Updated"
		typ := "GATEWAY"
		side := "RIGHT"

		req := UpdateSourceRequest{
			Name: &name,
			Type: &typ,
			Side: &side,
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)

		assert.NotNil(t, input.Name)
		assert.Equal(t, "Updated", *input.Name)
		assert.NotNil(t, input.Type)
		assert.Equal(t, value_objects.SourceType("GATEWAY"), *input.Type)
		assert.NotNil(t, input.Side)
		assert.Equal(t, sharedfee.MatchingSideRight, *input.Side)
	})

	t.Run("whitespace-only name returns error", func(t *testing.T) {
		t.Parallel()

		wsName := "   \t\n  "
		req := UpdateSourceRequest{
			Name: &wsName,
		}

		_, err := req.ToDomainInput()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNameWhitespaceOnly)
	})

	t.Run("trims leading and trailing whitespace from name", func(t *testing.T) {
		t.Parallel()

		name := "  Trimmed Source  "
		req := UpdateSourceRequest{
			Name: &name,
		}

		input, err := req.ToDomainInput()
		require.NoError(t, err)
		require.NotNil(t, input.Name)
		assert.Equal(t, "Trimmed Source", *input.Name)
	})

	t.Run("nil name stays nil", func(t *testing.T) {
		t.Parallel()

		req := UpdateSourceRequest{}

		input, err := req.ToDomainInput()
		require.NoError(t, err)
		assert.Nil(t, input.Name)
	})
}

func TestCreateFieldMapRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	req := CreateFieldMapRequest{
		Mapping: map[string]any{"amount": "col_a", "date": "col_b"},
	}

	input := req.ToDomainInput()
	assert.Equal(t, map[string]any{"amount": "col_a", "date": "col_b"}, input.Mapping)
}

func TestUpdateFieldMapRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	req := UpdateFieldMapRequest{
		Mapping: map[string]any{"amount": "col_x"},
	}

	input := req.ToDomainInput()
	assert.Equal(t, map[string]any{"amount": "col_x"}, input.Mapping)
}

func TestCreateMatchRuleRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	req := CreateMatchRuleRequest{
		Priority: 1,
		Type:     "EXACT",
		Config:   map[string]any{"matchAmount": true},
	}

	input, err := req.ToDomainInput()

	require.NoError(t, err)
	assert.Equal(t, 1, input.Priority)
	assert.Equal(t, shared.RuleType("EXACT"), input.Type)
	assert.Equal(t, map[string]any{"matchAmount": true}, input.Config)
}

func TestUpdateMatchRuleRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("with type update", func(t *testing.T) {
		t.Parallel()

		priority := 5
		typ := "TOLERANCE"

		req := UpdateMatchRuleRequest{
			Priority: &priority,
			Type:     &typ,
			Config:   map[string]any{"absTolerance": 0.01},
		}

		input := req.ToDomainInput()

		assert.Equal(t, &priority, input.Priority)
		assert.NotNil(t, input.Type)
		assert.Equal(t, shared.RuleType("TOLERANCE"), *input.Type)
		assert.Equal(t, map[string]any{"absTolerance": 0.01}, input.Config)
	})

	t.Run("nil type stays nil", func(t *testing.T) {
		t.Parallel()

		req := UpdateMatchRuleRequest{}
		input := req.ToDomainInput()
		assert.Nil(t, input.Type)
	})
}

func TestCreateScheduleRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	enabled := true
	req := CreateScheduleRequest{
		CronExpression: "0 0 * * *",
		Enabled:        &enabled,
	}

	input := req.ToDomainInput()

	assert.Equal(t, "0 0 * * *", input.CronExpression)
	assert.Equal(t, &enabled, input.Enabled)
}

func TestUpdateScheduleRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	cron := "0 6 * * *"
	enabled := false

	req := UpdateScheduleRequest{
		CronExpression: &cron,
		Enabled:        &enabled,
	}

	input := req.ToDomainInput()

	assert.Equal(t, &cron, input.CronExpression)
	assert.Equal(t, &enabled, input.Enabled)
}

func TestValidateJSONField(t *testing.T) {
	t.Parallel()

	t.Run("returns error when key count exceeds limit", func(t *testing.T) {
		t.Parallel()

		payload := make(map[string]any, maxJSONFieldKeys+1)
		for i := 0; i <= maxJSONFieldKeys; i++ {
			payload[fmt.Sprintf("k_%d", i)] = i
		}

		err := validateJSONField("field.config", payload)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds maximum key count")
		assert.True(t, errors.Is(err, ErrJSONTooManyKeys))
	})

	t.Run("returns error when nesting depth exceeds limit", func(t *testing.T) {
		t.Parallel()

		payload := map[string]any{"level1": deepJSON(maxJSONFieldDepth)}

		err := validateJSONField("field.mapping", payload)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds maximum nesting depth")
		assert.True(t, errors.Is(err, ErrJSONNestingTooDeep))
	})

	t.Run("returns error when encoded payload exceeds size limit", func(t *testing.T) {
		t.Parallel()

		payload := map[string]any{"blob": strings.Repeat("x", maxJSONFieldBytes+1024)}

		err := validateJSONField("field.config", payload)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds maximum size")
		assert.True(t, errors.Is(err, ErrJSONFieldTooLarge))
	})

	t.Run("accepts payload inside limits", func(t *testing.T) {
		t.Parallel()

		err := validateJSONField("field.config", map[string]any{
			"matchAmount": true,
			"nested": map[string]any{
				"window": 3,
			},
		})
		require.NoError(t, err)
	})
}

func deepJSON(depth int) map[string]any {
	if depth <= 0 {
		return map[string]any{"leaf": true}
	}

	return map[string]any{"child": deepJSON(depth - 1)}
}
