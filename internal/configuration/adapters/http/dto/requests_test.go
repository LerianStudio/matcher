//go:build unit

package dto

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestCreateContextRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("all fields populated", func(t *testing.T) {
		t.Parallel()

		rateID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000").String()
		abs := "0.01"
		pct := "0.5"
		norm := "NET"
		auto := false

		req := CreateContextRequest{
			Name:              "Bank Reconciliation Q1",
			Type:              "1:1",
			Interval:          "daily",
			RateID:            &rateID,
			FeeToleranceAbs:   &abs,
			FeeTolerancePct:   &pct,
			FeeNormalization:  &norm,
			AutoMatchOnUpload: &auto,
		}

		input, err := req.ToDomainInput()
		assert.NoError(t, err)

		assert.Equal(t, "Bank Reconciliation Q1", input.Name)
		assert.Equal(t, value_objects.ContextType("1:1"), input.Type)
		assert.Equal(t, "daily", input.Interval)
		assert.NotNil(t, input.RateID)
		assert.Equal(t, rateID, input.RateID.String())
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
		assert.NoError(t, err)

		assert.Equal(t, "Minimal", input.Name)
		assert.Equal(t, value_objects.ContextType("N:M"), input.Type)
		assert.Nil(t, input.RateID)
		assert.Nil(t, input.FeeToleranceAbs)
		assert.Nil(t, input.AutoMatchOnUpload)
	})

	t.Run("invalid uuid in rateId returns error", func(t *testing.T) {
		t.Parallel()

		invalid := "not-a-uuid"
		req := CreateContextRequest{
			Name:     "Test",
			Type:     "1:1",
			Interval: "daily",
			RateID:   &invalid,
		}

		_, err := req.ToDomainInput()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid rateId")
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
		rateID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440000").String()

		req := UpdateContextRequest{
			Name:     &name,
			Type:     &typ,
			Interval: &interval,
			Status:   &status,
			RateID:   &rateID,
		}

		input, err := req.ToDomainInput()
		assert.NoError(t, err)

		assert.Equal(t, &name, input.Name)
		assert.NotNil(t, input.Type)
		assert.Equal(t, value_objects.ContextType("1:N"), *input.Type)
		assert.NotNil(t, input.Status)
		assert.Equal(t, value_objects.ContextStatus("PAUSED"), *input.Status)
		assert.NotNil(t, input.RateID)
	})

	t.Run("nil fields stay nil", func(t *testing.T) {
		t.Parallel()

		req := UpdateContextRequest{}
		input, err := req.ToDomainInput()
		assert.NoError(t, err)

		assert.Nil(t, input.Name)
		assert.Nil(t, input.Type)
		assert.Nil(t, input.Status)
		assert.Nil(t, input.RateID)
	})

	t.Run("status ACTIVE converts correctly", func(t *testing.T) {
		t.Parallel()

		status := "ACTIVE"
		req := UpdateContextRequest{Status: &status}
		input, err := req.ToDomainInput()
		assert.NoError(t, err)
		assert.NotNil(t, input.Status)
		assert.Equal(t, value_objects.ContextStatus("ACTIVE"), *input.Status)
	})

	t.Run("status ARCHIVED converts correctly", func(t *testing.T) {
		t.Parallel()

		status := "ARCHIVED"
		req := UpdateContextRequest{Status: &status}
		input, err := req.ToDomainInput()
		assert.NoError(t, err)
		assert.NotNil(t, input.Status)
		assert.Equal(t, value_objects.ContextStatus("ARCHIVED"), *input.Status)
	})

	t.Run("invalid uuid in rateId returns error", func(t *testing.T) {
		t.Parallel()

		invalid := "not-a-uuid"
		req := UpdateContextRequest{RateID: &invalid}
		_, err := req.ToDomainInput()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid rateId")
	})
}

func TestCreateSourceRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("with fee schedule id", func(t *testing.T) {
		t.Parallel()

		feeID := uuid.MustParse("770e8400-e29b-41d4-a716-446655440000").String()

		req := CreateSourceRequest{
			Name:          "Primary Bank",
			Type:          "BANK",
			Config:        map[string]any{"key": "value"},
			FeeScheduleID: &feeID,
		}

		input, err := req.ToDomainInput()
		assert.NoError(t, err)

		assert.Equal(t, "Primary Bank", input.Name)
		assert.Equal(t, value_objects.SourceType("BANK"), input.Type)
		assert.Equal(t, map[string]any{"key": "value"}, input.Config)
		assert.NotNil(t, input.FeeScheduleID)
		assert.Equal(t, feeID, input.FeeScheduleID.String())
	})

	t.Run("without optional fields", func(t *testing.T) {
		t.Parallel()

		req := CreateSourceRequest{
			Name: "Minimal",
			Type: "LEDGER",
		}

		input, err := req.ToDomainInput()
		assert.NoError(t, err)
		assert.Nil(t, input.FeeScheduleID)
		assert.Nil(t, input.Config)
	})

	t.Run("invalid uuid in feeScheduleId returns error", func(t *testing.T) {
		t.Parallel()

		invalid := "not-a-uuid"
		req := CreateSourceRequest{
			Name:          "Test",
			Type:          "BANK",
			FeeScheduleID: &invalid,
		}

		_, err := req.ToDomainInput()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid feeScheduleId")
	})
}

func TestUpdateSourceRequest_ToDomainInput(t *testing.T) {
	t.Parallel()

	t.Run("with type update", func(t *testing.T) {
		t.Parallel()

		name := "Updated"
		typ := "GATEWAY"
		feeID := uuid.MustParse("880e8400-e29b-41d4-a716-446655440000").String()

		req := UpdateSourceRequest{
			Name:          &name,
			Type:          &typ,
			FeeScheduleID: &feeID,
		}

		input, err := req.ToDomainInput()
		assert.NoError(t, err)

		assert.Equal(t, &name, input.Name)
		assert.NotNil(t, input.Type)
		assert.Equal(t, value_objects.SourceType("GATEWAY"), *input.Type)
		assert.NotNil(t, input.FeeScheduleID)
	})

	t.Run("invalid uuid in feeScheduleId returns error", func(t *testing.T) {
		t.Parallel()

		invalid := "not-a-uuid"
		req := UpdateSourceRequest{FeeScheduleID: &invalid}
		_, err := req.ToDomainInput()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid feeScheduleId")
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

	input := req.ToDomainInput()

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
