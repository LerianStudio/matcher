//go:build unit

package entities

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- NewReconciliationSchedule tests ---

func TestNewReconciliationSchedule_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	input := CreateScheduleInput{
		CronExpression: "0 0 * * *",
	}

	schedule, err := NewReconciliationSchedule(ctx, contextID, input)

	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.NotEqual(t, uuid.Nil, schedule.ID)
	assert.Equal(t, contextID, schedule.ContextID)
	assert.Equal(t, "0 0 * * *", schedule.CronExpression)
	assert.True(t, schedule.Enabled)
	assert.NotNil(t, schedule.NextRunAt, "NextRunAt should be computed for enabled schedule")
	assert.True(t, schedule.NextRunAt.After(time.Now().UTC().Add(-time.Second)),
		"NextRunAt should be in the future")
	assert.False(t, schedule.CreatedAt.IsZero())
	assert.False(t, schedule.UpdatedAt.IsZero())
	assert.Equal(t, schedule.CreatedAt, schedule.UpdatedAt)
	assert.Nil(t, schedule.LastRunAt)
}

func TestNewReconciliationSchedule_NilContextID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	input := CreateScheduleInput{
		CronExpression: "0 0 * * *",
	}

	schedule, err := NewReconciliationSchedule(ctx, uuid.Nil, input)

	require.Error(t, err)
	require.Nil(t, schedule)
	assert.ErrorIs(t, err, ErrScheduleContextIDRequired)
}

func TestNewReconciliationSchedule_EmptyCronExpression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	input := CreateScheduleInput{
		CronExpression: "",
	}

	schedule, err := NewReconciliationSchedule(ctx, uuid.New(), input)

	require.Error(t, err)
	require.Nil(t, schedule)
	assert.ErrorIs(t, err, ErrScheduleCronExpressionRequired)
}

func TestNewReconciliationSchedule_InvalidCronExpression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	input := CreateScheduleInput{
		CronExpression: "not-a-cron",
	}

	schedule, err := NewReconciliationSchedule(ctx, uuid.New(), input)

	require.Error(t, err)
	require.Nil(t, schedule)
	assert.ErrorIs(t, err, ErrScheduleCronExpressionInvalid)
}

func TestNewReconciliationSchedule_DisabledByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	enabled := false
	input := CreateScheduleInput{
		CronExpression: "0 0 * * *",
		Enabled:        &enabled,
	}

	schedule, err := NewReconciliationSchedule(ctx, uuid.New(), input)

	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.False(t, schedule.Enabled)
	assert.Nil(t, schedule.NextRunAt, "NextRunAt should be nil for disabled schedule")
}

func TestNewReconciliationSchedule_EnabledByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	input := CreateScheduleInput{
		CronExpression: "*/5 * * * *",
	}

	schedule, err := NewReconciliationSchedule(ctx, uuid.New(), input)

	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.True(t, schedule.Enabled, "Enabled should default to true when Enabled pointer is nil")
	assert.NotNil(t, schedule.NextRunAt, "NextRunAt should be computed when enabled")
}

// --- Update tests ---

func TestReconciliationSchedule_Update_CronExpression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schedule := newTestSchedule(t, true)
	oldCron := schedule.CronExpression

	newCron := "30 6 * * *"
	input := UpdateScheduleInput{
		CronExpression: &newCron,
	}

	err := schedule.Update(ctx, input)

	require.NoError(t, err)
	assert.Equal(t, newCron, schedule.CronExpression)
	assert.NotEqual(t, oldCron, schedule.CronExpression)
	assert.NotNil(t, schedule.NextRunAt, "NextRunAt should be recalculated after cron update")
}

func TestReconciliationSchedule_Update_Disable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schedule := newTestSchedule(t, true)
	require.NotNil(t, schedule.NextRunAt, "precondition: schedule must start enabled with NextRunAt")

	enabled := false
	input := UpdateScheduleInput{
		Enabled: &enabled,
	}

	err := schedule.Update(ctx, input)

	require.NoError(t, err)
	assert.False(t, schedule.Enabled)
	assert.Nil(t, schedule.NextRunAt, "NextRunAt should be nil after disabling")
}

func TestReconciliationSchedule_Update_Enable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schedule := newTestSchedule(t, false)
	require.Nil(t, schedule.NextRunAt, "precondition: disabled schedule should have nil NextRunAt")

	enabled := true
	input := UpdateScheduleInput{
		Enabled: &enabled,
	}

	err := schedule.Update(ctx, input)

	require.NoError(t, err)
	assert.True(t, schedule.Enabled)
	assert.NotNil(t, schedule.NextRunAt, "NextRunAt should be computed after enabling")
}

func TestReconciliationSchedule_Update_InvalidCron(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schedule := newTestSchedule(t, true)

	badCron := "invalid-cron"
	input := UpdateScheduleInput{
		CronExpression: &badCron,
	}

	err := schedule.Update(ctx, input)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrScheduleCronExpressionInvalid)
}

func TestReconciliationSchedule_Update_NilReceiver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var schedule *ReconciliationSchedule

	err := schedule.Update(ctx, UpdateScheduleInput{})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrScheduleNil)
}

// --- CalculateNextRun tests ---

func TestCalculateNextRun_ValidCron(t *testing.T) {
	t.Parallel()

	schedule := &ReconciliationSchedule{
		CronExpression: "0 12 * * *",
	}

	from := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	next := schedule.CalculateNextRun(from)

	require.NotNil(t, next)
	assert.True(t, next.After(from), "next run should be after the from time")
	assert.Equal(t, 12, next.Hour(), "next run should be at noon")
	assert.Equal(t, 0, next.Minute(), "next run should be at minute 0")
}

func TestCalculateNextRun_NilReceiver(t *testing.T) {
	t.Parallel()

	var schedule *ReconciliationSchedule

	next := schedule.CalculateNextRun(time.Now().UTC())

	assert.Nil(t, next)
}

func TestCalculateNextRun_EmptyCron(t *testing.T) {
	t.Parallel()

	schedule := &ReconciliationSchedule{
		CronExpression: "",
	}

	next := schedule.CalculateNextRun(time.Now().UTC())

	assert.Nil(t, next)
}

// --- MarkRun tests ---

func TestMarkRun_SetsLastRunAndNextRun(t *testing.T) {
	t.Parallel()

	schedule := &ReconciliationSchedule{
		CronExpression: "0 0 * * *",
		Enabled:        true,
	}

	now := time.Date(2026, 2, 8, 14, 30, 0, 0, time.UTC)
	schedule.MarkRun(now)

	require.NotNil(t, schedule.LastRunAt)
	assert.Equal(t, now, *schedule.LastRunAt)
	require.NotNil(t, schedule.NextRunAt, "NextRunAt should be computed after marking run")
	assert.True(t, schedule.NextRunAt.After(now), "NextRunAt should be after the run time")
	assert.Equal(t, now, schedule.UpdatedAt)
}

func TestMarkRun_NilReceiver(t *testing.T) {
	t.Parallel()

	var schedule *ReconciliationSchedule

	// Should not panic.
	assert.NotPanics(t, func() {
		schedule.MarkRun(time.Now().UTC())
	})
}

func TestMarkRun_NormalizesToUTC(t *testing.T) {
	t.Parallel()

	schedule := &ReconciliationSchedule{
		CronExpression: "0 0 * * *",
		Enabled:        true,
	}

	// Pass a non-UTC time (EST = UTC-5).
	est := time.FixedZone("EST", -5*3600)
	nonUTC := time.Date(2026, 3, 1, 10, 0, 0, 0, est)

	schedule.MarkRun(nonUTC)

	require.NotNil(t, schedule.LastRunAt)
	assert.Equal(t, time.UTC, schedule.LastRunAt.Location(),
		"LastRunAt should be normalized to UTC")
	assert.Equal(t, time.UTC, schedule.UpdatedAt.Location(),
		"UpdatedAt should be normalized to UTC")
}

// --- Test helpers ---

// newTestSchedule creates a valid ReconciliationSchedule for testing.
// The enabled parameter controls whether the schedule starts enabled or disabled.
func newTestSchedule(t *testing.T, enabled bool) *ReconciliationSchedule {
	t.Helper()

	ctx := context.Background()
	enabledPtr := &enabled
	input := CreateScheduleInput{
		CronExpression: "0 0 * * *",
		Enabled:        enabledPtr,
	}

	schedule, err := NewReconciliationSchedule(ctx, uuid.New(), input)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	return schedule
}
