// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// --- pollCycle tests ---

func TestPollCycle_NoDueSchedules(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	repo := &stubScheduleRepo{
		findDueSchedulesFn: func(_ context.Context, _ time.Time) ([]*entities.ReconciliationSchedule, error) {
			return nil, nil
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.pollCycle(context.Background())
	assert.False(t, trigger.triggerCalled)
}

func TestPollCycle_SkipsNilSchedules(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	contextID := uuid.New()
	tenantID := uuid.New()
	repo := &stubScheduleRepo{
		findDueSchedulesFn: func(_ context.Context, _ time.Time) ([]*entities.ReconciliationSchedule, error) {
			// First element nil: must not panic; second valid so we verify the
			// loop continues past the skip.
			return []*entities.ReconciliationSchedule{
				nil,
				{
					ID:             uuid.New(),
					ContextID:      contextID,
					CronExpression: "0 * * * *",
					Enabled:        true,
					TenantID:       tenantID,
				},
			}, nil
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		w.pollCycle(context.Background())
	})

	assert.True(t, trigger.triggerCalled)
	assert.Equal(t, contextID, trigger.lastContextID)
	assert.Equal(t, tenantID, trigger.lastTenantID)
}

func TestPollCycle_FindDueSchedulesError(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	repo := &stubScheduleRepo{
		findDueSchedulesFn: func(_ context.Context, _ time.Time) ([]*entities.ReconciliationSchedule, error) {
			return nil, errors.New("database error")
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Should not panic
	w.pollCycle(context.Background())
	assert.False(t, trigger.triggerCalled)
}

func TestPollCycle_WithDueSchedules(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000200000")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200001")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200002")

	repo := &stubScheduleRepo{
		findDueSchedulesFn: func(_ context.Context, _ time.Time) ([]*entities.ReconciliationSchedule, error) {
			return []*entities.ReconciliationSchedule{
				{
					ID:             scheduleID,
					ContextID:      contextID,
					CronExpression: "0 * * * *",
					Enabled:        true,
					TenantID:       tenantID,
				},
			}, nil
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.pollCycle(context.Background())

	assert.True(t, trigger.triggerCalled)
	assert.Equal(t, contextID, trigger.lastContextID)
	assert.Equal(t, tenantID, trigger.lastTenantID)
}

// --- processSchedule tests ---

func TestProcessSchedule_LockAcquireError(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	locker := &stubLockManager{
		withLockOptionsFn: func(_ context.Context, _ string, _ libRedis.LockOptions, _ func(context.Context) error) error {
			return errors.New("redis unavailable")
		},
	}

	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, locker,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	schedule := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "0 * * * *",
		Enabled:        true,
	}

	// Should not panic, should log and return
	w.processSchedule(context.Background(), schedule, time.Now().UTC())
	assert.False(t, trigger.triggerCalled)
}

func TestProcessSchedule_LockNotAcquired(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	locker := &stubLockManager{
		withLockOptionsFn: func(_ context.Context, _ string, _ libRedis.LockOptions, _ func(context.Context) error) error {
			// Simulate lock busy — WithLockOptions returns an error when lock is contended.
			return errors.New("failed to acquire lock matcher:scheduler:schedule:*: lock busy")
		},
	}

	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, locker,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	schedule := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "0 * * * *",
		Enabled:        true,
	}

	w.processSchedule(context.Background(), schedule, time.Now().UTC())
	assert.False(t, trigger.triggerCalled)
}

func TestProcessSchedule_SuccessfulTrigger(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000200019")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200020")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200021")

	var updatedSchedule *entities.ReconciliationSchedule

	repo := &stubScheduleRepo{
		updateFn: func(_ context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			updatedSchedule = s
			return s, nil
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	schedule := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      contextID,
		CronExpression: "0 * * * *",
		Enabled:        true,
		TenantID:       tenantID,
	}

	now := time.Now().UTC()
	w.processSchedule(context.Background(), schedule, now)

	assert.True(t, trigger.triggerCalled)
	assert.Equal(t, contextID, trigger.lastContextID)
	assert.Equal(t, tenantID, trigger.lastTenantID)

	require.NotNil(t, updatedSchedule)
	assert.NotNil(t, updatedSchedule.LastRunAt)
	assert.NotNil(t, updatedSchedule.NextRunAt)
}

func TestProcessSchedule_UpdateError(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}

	repo := &stubScheduleRepo{
		updateFn: func(_ context.Context, _ *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			return nil, errors.New("db error")
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	schedule := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "0 * * * *",
		Enabled:        true,
	}

	// Should not panic even though update fails
	w.processSchedule(context.Background(), schedule, time.Now().UTC())
	assert.True(t, trigger.triggerCalled)
}

func TestProcessSchedule_LockError_DoesNotPanic(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	locker := &stubLockManager{
		withLockOptionsFn: func(_ context.Context, _ string, _ libRedis.LockOptions, _ func(context.Context) error) error {
			return errors.New("lock infrastructure error")
		},
	}

	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, locker,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	schedule := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "0 * * * *",
		Enabled:        true,
	}

	// processSchedule should not panic even when lock fails.
	require.NotPanics(t, func() {
		w.processSchedule(context.Background(), schedule, time.Now().UTC())
	})
	assert.False(t, trigger.triggerCalled, "trigger should not be called when lock fails")
}

func TestProcessSchedule_CorrectLockKey(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200050")

	var capturedLockKey string

	locker := &stubLockManager{
		withLockOptionsFn: func(_ context.Context, lockKey string, _ libRedis.LockOptions, fn func(context.Context) error) error {
			capturedLockKey = lockKey
			return fn(context.Background())
		},
	}

	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, locker,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	schedule := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      uuid.New(),
		CronExpression: "0 * * * *",
		Enabled:        true,
	}

	w.processSchedule(context.Background(), schedule, time.Now().UTC())

	expectedKey := schedulerLockKeyPrefix + scheduleID.String()
	assert.Equal(t, expectedKey, capturedLockKey)
}

// --- tracking tests ---

func TestTracking_FallsBackToInstanceValues(t *testing.T) {
	t.Parallel()

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	logger, tracer := w.tracking(context.Background())
	assert.NotNil(t, logger)
	assert.NotNil(t, tracer)
}

func TestTracking_NilInstanceLogger_FallsBackToContextOrNone(t *testing.T) {
	t.Parallel()

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		nil, // nil instance logger
	)
	require.NoError(t, err)

	logger, tracer := w.tracking(context.Background())
	// NewTrackingFromContext returns a NopLogger when no logger in context,
	// which is non-nil. The tracking method then sees a non-nil context logger
	// and does not fall back to the instance logger.
	assert.NotNil(t, logger, "should return a logger even when instance logger is nil (NopLogger from context)")
	assert.NotNil(t, tracer)
}

// --- Multiple schedules in one cycle ---

func TestPollCycle_MultipleSchedules(t *testing.T) {
	t.Parallel()

	var triggerCount atomic.Int32
	trigger := &countingMatchTrigger{count: &triggerCount}

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000200029")
	contextID1 := uuid.MustParse("00000000-0000-0000-0000-000000200030")
	contextID2 := uuid.MustParse("00000000-0000-0000-0000-000000200031")

	repo := &stubScheduleRepo{
		findDueSchedulesFn: func(_ context.Context, _ time.Time) ([]*entities.ReconciliationSchedule, error) {
			return []*entities.ReconciliationSchedule{
				{
					ID:             uuid.New(),
					ContextID:      contextID1,
					CronExpression: "0 * * * *",
					Enabled:        true,
					TenantID:       tenantID,
				},
				{
					ID:             uuid.New(),
					ContextID:      contextID2,
					CronExpression: "0 * * * *",
					Enabled:        true,
					TenantID:       tenantID,
				},
			}, nil
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, &stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.pollCycle(context.Background())

	assert.Equal(t, int32(2), triggerCount.Load())
}

// countingMatchTrigger counts how many times TriggerMatchForContext is called.
type countingMatchTrigger struct {
	count *atomic.Int32
}

func (m *countingMatchTrigger) TriggerMatchForContext(_ context.Context, _, _ uuid.UUID) {
	m.count.Add(1)
}

// --- Context cancellation during run ---

func TestSchedulerWorker_StopsDuringRun(t *testing.T) {
	t.Parallel()

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, w.Start(ctx))

	// Cancel the context
	cancel()

	// Worker should stop
	select {
	case <-w.Done():
		// Expected
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}
}

// --- Done channel behavior ---

func TestSchedulerWorker_DoneNotClosedBeforeStart(t *testing.T) {
	t.Parallel()

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubLockManager{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	select {
	case <-w.Done():
		t.Fatal("Done channel should not be closed before Start")
	default:
		// Expected: channel is open
	}
}

// --- Lock TTL tests ---

func TestSchedulerLockExpiry_ProportionalToInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		expected time.Duration
	}{
		{
			name:     "1 minute interval gives 2 minute expiry",
			interval: time.Minute,
			expected: 2 * time.Minute,
		},
		{
			name:     "30 second interval gives 1 minute expiry",
			interval: 30 * time.Second,
			expected: time.Minute,
		},
		{
			name:     "1 hour interval gives 2 hour expiry",
			interval: time.Hour,
			expected: 2 * time.Hour,
		},
		{
			name:     "very short interval clamps to minimum 5s",
			interval: time.Second,
			expected: schedulerMinLockExpiry,
		},
		{
			name:     "sub-second interval clamps to minimum 5s",
			interval: 100 * time.Millisecond,
			expected: schedulerMinLockExpiry,
		},
		{
			name:     "zero interval clamps to minimum 5s",
			interval: 0,
			expected: schedulerMinLockExpiry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, schedulerLockExpiry(tt.interval))
		})
	}
}

func TestProcessSchedule_LockOptionsUseProportionalTTL(t *testing.T) {
	t.Parallel()

	trigger := &stubMatchTrigger{}
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200060")
	interval := 30 * time.Second

	var capturedOpts libRedis.LockOptions

	locker := &stubLockManager{
		withLockOptionsFn: func(_ context.Context, _ string, opts libRedis.LockOptions, fn func(context.Context) error) error {
			capturedOpts = opts
			return fn(context.Background())
		},
	}

	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, locker,
		SchedulerWorkerConfig{Interval: interval},
		&stubLogger{},
	)
	require.NoError(t, err)

	schedule := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      uuid.New(),
		CronExpression: "0 * * * *",
		Enabled:        true,
	}

	w.processSchedule(context.Background(), schedule, time.Now().UTC())

	assert.True(t, trigger.triggerCalled)
	assert.Equal(t, schedulerLockExpiry(interval), capturedOpts.Expiry, "lock expiry must be 2× poll interval")
	assert.Equal(t, 1, capturedOpts.Tries, "must use single try (non-blocking)")
}
