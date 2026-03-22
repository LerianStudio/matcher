//go:build unit

package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	sharedTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type capturingWarnLogger struct {
	warnCalled bool
	messages   []string
}

func (l *capturingWarnLogger) Log(_ context.Context, level libLog.Level, msg string, _ ...libLog.Field) {
	if level == libLog.LevelWarn {
		l.warnCalled = true
		l.messages = append(l.messages, msg)
	}
}

//nolint:ireturn
func (l *capturingWarnLogger) With(_ ...libLog.Field) libLog.Logger { return l }

//nolint:ireturn
func (l *capturingWarnLogger) WithGroup(_ string) libLog.Logger { return l }

func (l *capturingWarnLogger) Enabled(_ libLog.Level) bool { return true }

func (l *capturingWarnLogger) Sync(_ context.Context) error { return nil }

// --- redisInfraProvider wraps stubInfraProvider with real Redis ---

type redisInfraProvider struct {
	conn *libRedis.Client
}

var _ sharedPorts.InfrastructureProvider = (*redisInfraProvider)(nil)

func (m *redisInfraProvider) GetPostgresConnection(_ context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	return nil, nil
}

func (m *redisInfraProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	if m.conn == nil {
		return nil, errors.New("redis not configured")
	}

	return sharedPorts.NewRedisConnectionLease(m.conn, nil), nil
}

func (m *redisInfraProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, nil
}

func (m *redisInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.ReplicaDBLease, error) {
	return nil, nil
}

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
		repo, trigger, &stubInfraProvider{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.pollCycle(context.Background())
	assert.False(t, trigger.triggerCalled)
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
		repo, trigger, &stubInfraProvider{},
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

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

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
		repo, trigger, provider,
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
	provider := &stubInfraProvider{} // Returns nil redis connection

	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, provider,
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

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

	trigger := &stubMatchTrigger{}
	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200010")
	schedule := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      uuid.New(),
		CronExpression: "0 * * * *",
		Enabled:        true,
	}

	// Pre-acquire the lock
	lockKey := schedulerLockKeyPrefix + scheduleID.String()
	srv.Set(lockKey, "someone-else")

	w.processSchedule(context.Background(), schedule, time.Now().UTC())
	assert.False(t, trigger.triggerCalled)
}

func TestProcessSchedule_SuccessfulTrigger(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

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
		repo, trigger, provider,
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

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

	trigger := &stubMatchTrigger{}

	repo := &stubScheduleRepo{
		updateFn: func(_ context.Context, _ *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			return nil, errors.New("db error")
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, provider,
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

// --- acquireLock tests ---

func TestAcquireLock_RedisConnectionError(t *testing.T) {
	t.Parallel()

	provider := &stubInfraProvider{} // Returns nil redis connection

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	acquired, token, err := w.acquireLock(context.Background(), "test-key")
	assert.Error(t, err)
	assert.False(t, acquired)
	assert.Empty(t, token)
}

func TestAcquireLock_Success(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	acquired, token, err := w.acquireLock(context.Background(), "test-key")
	assert.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, token)
}

func TestAcquireLock_AlreadyHeld(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Pre-acquire the lock
	srv.Set("test-key-held", "other-token")

	acquired, _, err := w.acquireLock(context.Background(), "test-key-held")
	assert.NoError(t, err)
	assert.False(t, acquired)
}

// --- releaseLock tests ---

func TestReleaseLock_RedisConnectionError(t *testing.T) {
	t.Parallel()

	provider := &stubInfraProvider{} // Returns nil redis connection

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Should not panic
	w.releaseLock(context.Background(), "test-key", "test-token")
}

func TestReleaseLock_CorrectTokenReleases(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Set the key
	srv.Set("release-test-key", "my-token")

	w.releaseLock(context.Background(), "release-test-key", "my-token")

	// Key should be deleted
	assert.False(t, srv.Exists("release-test-key"))
}

func TestReleaseLock_WrongTokenDoesNotRelease(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Set the key with one token
	srv.Set("release-test-key-2", "correct-token")

	// Try to release with wrong token
	w.releaseLock(context.Background(), "release-test-key-2", "wrong-token")

	// Key should still exist
	assert.True(t, srv.Exists("release-test-key-2"))
}

// --- tracking tests ---

func TestTracking_FallsBackToInstanceValues(t *testing.T) {
	t.Parallel()

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, &stubInfraProvider{},
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
		&stubScheduleRepo{}, &stubMatchTrigger{}, &stubInfraProvider{},
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

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := sharedTestutil.NewRedisClientWithMock(redisClient)
	provider := &redisInfraProvider{conn: conn}

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
		repo, trigger, provider,
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
		&stubInfraProvider{},
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

// --- GetClient error path tests ---

// errorRedisInfraProvider returns a *libRedis.Client where GetClient(ctx) will fail.
type errorRedisInfraProvider struct {
	conn *libRedis.Client
}

var _ sharedPorts.InfrastructureProvider = (*errorRedisInfraProvider)(nil)

func (m *errorRedisInfraProvider) GetPostgresConnection(_ context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	return nil, nil
}

func (m *errorRedisInfraProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	return sharedPorts.NewRedisConnectionLease(m.conn, nil), nil
}

func (m *errorRedisInfraProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, nil
}

func (m *errorRedisInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.ReplicaDBLease, error) {
	return nil, nil
}

func TestSchedulerWorker_GetClient_LockAcquisitionError(t *testing.T) {
	t.Parallel()

	// NewRedisClientWithMock(nil) creates a client where GetClient(ctx) returns an error
	// (simulating an unconnected client).
	conn := sharedTestutil.NewRedisClientWithMock(nil)
	provider := &errorRedisInfraProvider{conn: conn}

	trigger := &stubMatchTrigger{}
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000300001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000300002")

	repo := &stubScheduleRepo{
		findDueSchedulesFn: func(_ context.Context, _ time.Time) ([]*entities.ReconciliationSchedule, error) {
			return []*entities.ReconciliationSchedule{
				{
					ID:             scheduleID,
					ContextID:      contextID,
					CronExpression: "0 * * * *",
					Enabled:        true,
				},
			}, nil
		},
	}

	w, err := NewSchedulerWorker(
		repo, trigger, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// pollCycle should gracefully handle the GetClient error without panicking.
	// The tick should continue (match trigger should NOT be called since lock was not acquired).
	require.NotPanics(t, func() {
		w.pollCycle(context.Background())
	})
	assert.False(t, trigger.triggerCalled, "match trigger should not be called when lock acquisition fails due to GetClient error")
}

// switchableRedisInfraProvider returns the good connection for the first N calls
// to GetRedisConnection, then switches to the bad connection.
type switchableRedisInfraProvider struct {
	goodConn    *libRedis.Client
	badConn     *libRedis.Client
	switchAfter int32 // switch after this many calls
	callCount   atomic.Int32
}

var _ sharedPorts.InfrastructureProvider = (*switchableRedisInfraProvider)(nil)

func (m *switchableRedisInfraProvider) GetPostgresConnection(_ context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	return nil, nil
}

func (m *switchableRedisInfraProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	count := m.callCount.Add(1)
	if count > m.switchAfter {
		return sharedPorts.NewRedisConnectionLease(m.badConn, nil), nil
	}

	return sharedPorts.NewRedisConnectionLease(m.goodConn, nil), nil
}

func (m *switchableRedisInfraProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, nil
}

func (m *switchableRedisInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.ReplicaDBLease, error) {
	return nil, nil
}

func TestSchedulerWorker_GetClient_LockReleaseError(t *testing.T) {
	t.Parallel()

	// Use a real miniredis for lock acquisition, but switch the provider
	// to a bad connection for lock release.
	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	goodConn := sharedTestutil.NewRedisClientWithMock(redisClient)
	badConn := sharedTestutil.NewRedisClientWithMock(nil)

	// acquireLock calls GetRedisConnection twice (once for conn, once for GetClient internally
	// via the same conn). Actually, it calls GetRedisConnection once and then conn.GetClient.
	// So we need switchAfter=1: first call (acquire) returns good, second call (release) returns bad.
	provider := &switchableRedisInfraProvider{
		goodConn:    goodConn,
		badConn:     badConn,
		switchAfter: 1, // First GetRedisConnection call succeeds (acquire), second fails (release)
	}

	trigger := &stubMatchTrigger{}
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000300003")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000300004")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000300005")

	repo := &stubScheduleRepo{}

	w, err := NewSchedulerWorker(
		repo, trigger, provider,
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

	// processSchedule should not panic even when lock release encounters GetClient error.
	require.NotPanics(t, func() {
		w.processSchedule(context.Background(), schedule, time.Now().UTC())
	})

	// The match trigger should have been called since the lock was acquired successfully.
	assert.True(t, trigger.triggerCalled, "match trigger should be called since lock acquisition succeeded")
}

func TestAcquireLock_GetClientError(t *testing.T) {
	t.Parallel()

	// Create a redis client where GetClient returns an error.
	conn := sharedTestutil.NewRedisClientWithMock(nil)
	provider := &errorRedisInfraProvider{conn: conn}

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	acquired, token, err := w.acquireLock(context.Background(), "test-get-client-error")
	assert.Error(t, err, "acquireLock should return error when GetClient fails")
	assert.False(t, acquired)
	assert.Empty(t, token)
	assert.Contains(t, err.Error(), "get redis client for lock acquire")
}

func TestReleaseLock_GetClientError(t *testing.T) {
	t.Parallel()

	// Create a redis client where GetClient returns an error.
	conn := sharedTestutil.NewRedisClientWithMock(nil)
	provider := &errorRedisInfraProvider{conn: conn}

	logger := &capturingWarnLogger{}

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{}, &stubMatchTrigger{}, provider,
		SchedulerWorkerConfig{Interval: time.Hour},
		logger,
	)
	require.NoError(t, err)

	// releaseLock should not panic when GetClient fails - it should silently return.
	require.NotPanics(t, func() {
		w.releaseLock(context.Background(), "test-release-get-client-error", "some-token")
	})

	assert.True(t, logger.warnCalled, "releaseLock should log warning when redis client acquisition fails")
	assert.Contains(t, logger.messages, "scheduler: failed to acquire redis client for lock release")
}

// --- Done channel behavior ---

func TestSchedulerWorker_DoneNotClosedBeforeStart(t *testing.T) {
	t.Parallel()

	w, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubInfraProvider{},
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
