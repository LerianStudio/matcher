//go:build unit

package schedule

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

func TestNewSchedulePostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewSchedulePostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrScheduleEntityRequired)
}

func TestNewSchedulePostgreSQLModel_NilContextID(t *testing.T) {
	t.Parallel()

	entity := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.Nil,
		CronExpression: "0 0 * * *",
		Enabled:        true,
	}

	model, err := NewSchedulePostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrScheduleContextIDRequired)
}

func TestNewSchedulePostgreSQLModel_ValidEntity(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	lastRun := now.Add(-1 * time.Hour)
	nextRun := now.Add(1 * time.Hour)
	id := uuid.New()
	contextID := uuid.New()

	entity := &entities.ReconciliationSchedule{
		ID:             id,
		ContextID:      contextID,
		CronExpression: "0 6 * * *",
		Enabled:        true,
		LastRunAt:      &lastRun,
		NextRunAt:      &nextRun,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	model, err := NewSchedulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, id.String(), model.ID)
	assert.Equal(t, contextID.String(), model.ContextID)
	assert.Equal(t, "0 6 * * *", model.CronExpression)
	assert.True(t, model.Enabled)
	require.NotNil(t, model.LastRunAt)
	assert.Equal(t, lastRun, *model.LastRunAt)
	require.NotNil(t, model.NextRunAt)
	assert.Equal(t, nextRun, *model.NextRunAt)
	assert.Equal(t, now, model.CreatedAt)
	assert.Equal(t, now, model.UpdatedAt)
}

func TestNewSchedulePostgreSQLModel_GeneratesIDWhenNil(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.ReconciliationSchedule{
		ID:             uuid.Nil,
		ContextID:      uuid.New(),
		CronExpression: "0 0 * * *",
		Enabled:        false,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	model, err := NewSchedulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)

	parsedID, parseErr := uuid.Parse(model.ID)
	require.NoError(t, parseErr)
	assert.NotEqual(t, uuid.Nil, parsedID, "generated ID should not be uuid.Nil")
}

func TestNewSchedulePostgreSQLModel_DefaultsTimestamps(t *testing.T) {
	t.Parallel()

	entity := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "*/5 * * * *",
		Enabled:        true,
		// CreatedAt and UpdatedAt are zero values
	}

	before := time.Now().UTC()

	model, err := NewSchedulePostgreSQLModel(entity)

	after := time.Now().UTC()

	require.NoError(t, err)
	require.NotNil(t, model)

	// CreatedAt should have been set to approximately now
	assert.False(t, model.CreatedAt.IsZero(), "CreatedAt should not be zero")
	assert.True(t, !model.CreatedAt.Before(before), "CreatedAt should be >= before")
	assert.True(t, !model.CreatedAt.After(after), "CreatedAt should be <= after")

	// UpdatedAt defaults to CreatedAt when zero
	assert.Equal(t, model.CreatedAt, model.UpdatedAt, "UpdatedAt should default to CreatedAt")
}

func TestNewSchedulePostgreSQLModel_WithOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	lastRun := now.Add(-2 * time.Hour)
	nextRun := now.Add(3 * time.Hour)

	entity := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "30 12 * * 1-5",
		Enabled:        true,
		LastRunAt:      &lastRun,
		NextRunAt:      &nextRun,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	model, err := NewSchedulePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotNil(t, model.LastRunAt)
	assert.Equal(t, lastRun, *model.LastRunAt)
	require.NotNil(t, model.NextRunAt)
	assert.Equal(t, nextRun, *model.NextRunAt)
}

func TestScheduleModel_ToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *SchedulePostgreSQLModel

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrScheduleModelRequired)
}

func TestScheduleModel_ToEntity_InvalidID(t *testing.T) {
	t.Parallel()

	model := &SchedulePostgreSQLModel{
		ID:             "not-a-valid-uuid",
		ContextID:      uuid.New().String(),
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	assert.Contains(t, err.Error(), "parsing ID")
}

func TestScheduleModel_ToEntity_InvalidContextID(t *testing.T) {
	t.Parallel()

	model := &SchedulePostgreSQLModel{
		ID:             uuid.New().String(),
		ContextID:      "invalid-context-id",
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	assert.Contains(t, err.Error(), "parsing ContextID")
}

func TestScheduleModel_ToEntity_ValidModel(t *testing.T) {
	t.Parallel()

	// Test round-trip: entity -> model -> entity preserves data
	now := time.Now().UTC()
	lastRun := now.Add(-30 * time.Minute)
	nextRun := now.Add(30 * time.Minute)
	id := uuid.New()
	contextID := uuid.New()

	original := &entities.ReconciliationSchedule{
		ID:             id,
		ContextID:      contextID,
		CronExpression: "0 */2 * * *",
		Enabled:        true,
		LastRunAt:      &lastRun,
		NextRunAt:      &nextRun,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	model, err := NewSchedulePostgreSQLModel(original)
	require.NoError(t, err)

	roundTripped, err := model.ToEntity()
	require.NoError(t, err)
	require.NotNil(t, roundTripped)

	assert.Equal(t, original.ID, roundTripped.ID)
	assert.Equal(t, original.ContextID, roundTripped.ContextID)
	assert.Equal(t, original.CronExpression, roundTripped.CronExpression)
	assert.Equal(t, original.Enabled, roundTripped.Enabled)
	require.NotNil(t, roundTripped.LastRunAt)
	assert.Equal(t, *original.LastRunAt, *roundTripped.LastRunAt)
	require.NotNil(t, roundTripped.NextRunAt)
	assert.Equal(t, *original.NextRunAt, *roundTripped.NextRunAt)
	assert.Equal(t, original.CreatedAt, roundTripped.CreatedAt)
	assert.Equal(t, original.UpdatedAt, roundTripped.UpdatedAt)
}

func TestScheduleModel_ToEntity_WithNilOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &SchedulePostgreSQLModel{
		ID:             uuid.New().String(),
		ContextID:      uuid.New().String(),
		CronExpression: "0 0 * * *",
		Enabled:        false,
		LastRunAt:      nil,
		NextRunAt:      nil,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Nil(t, entity.LastRunAt, "LastRunAt should remain nil")
	assert.Nil(t, entity.NextRunAt, "NextRunAt should remain nil")
	assert.False(t, entity.Enabled)
}
