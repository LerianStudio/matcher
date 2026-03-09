//go:build unit

package worker

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// --- stub mocks ---

// stubScheduleRepo implements configPorts.ScheduleRepository for worker tests.
type stubScheduleRepo struct {
	findDueSchedulesFn func(ctx context.Context, now time.Time) ([]*entities.ReconciliationSchedule, error)
	findByIDFn         func(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error)
	createFn           func(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	findByContextIDFn  func(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error)
	updateFn           func(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	deleteFn           func(ctx context.Context, id uuid.UUID) error
}

var _ configPorts.ScheduleRepository = (*stubScheduleRepo)(nil)

func (m *stubScheduleRepo) Create(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	if m.createFn != nil {
		return m.createFn(ctx, s)
	}

	return s, nil
}

func (m *stubScheduleRepo) FindByID(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	return nil, sql.ErrNoRows
}

func (m *stubScheduleRepo) FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error) {
	if m.findByContextIDFn != nil {
		return m.findByContextIDFn(ctx, contextID)
	}

	return nil, nil
}

func (m *stubScheduleRepo) FindDueSchedules(ctx context.Context, now time.Time) ([]*entities.ReconciliationSchedule, error) {
	if m.findDueSchedulesFn != nil {
		return m.findDueSchedulesFn(ctx, now)
	}

	return nil, nil
}

func (m *stubScheduleRepo) Update(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, s)
	}

	return s, nil
}

func (m *stubScheduleRepo) Delete(ctx context.Context, id uuid.UUID) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}

	return nil
}

// stubMatchTrigger implements sharedPorts.MatchTrigger.
type stubMatchTrigger struct {
	triggerCalled bool
	lastTenantID  uuid.UUID
	lastContextID uuid.UUID
}

var _ sharedPorts.MatchTrigger = (*stubMatchTrigger)(nil)

func (m *stubMatchTrigger) TriggerMatchForContext(_ context.Context, tenantID, contextID uuid.UUID) {
	m.triggerCalled = true
	m.lastTenantID = tenantID
	m.lastContextID = contextID
}

// stubInfraProvider implements sharedPorts.InfrastructureProvider.
type stubInfraProvider struct{}

var _ sharedPorts.InfrastructureProvider = (*stubInfraProvider)(nil)

func (m *stubInfraProvider) GetPostgresConnection(_ context.Context) (*libPostgres.Client, error) {
	return nil, nil
}

func (m *stubInfraProvider) GetRedisConnection(_ context.Context) (*libRedis.Client, error) {
	return nil, nil
}

func (m *stubInfraProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (m *stubInfraProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

// stubLogger implements libLog.Logger (v2) for worker tests.
type stubLogger struct{}

var _ libLog.Logger = (*stubLogger)(nil)

func (m *stubLogger) Log(_ context.Context, _ libLog.Level, _ string, _ ...libLog.Field) {}

//nolint:ireturn
func (m *stubLogger) With(_ ...libLog.Field) libLog.Logger { return m }

//nolint:ireturn
func (m *stubLogger) WithGroup(_ string) libLog.Logger { return m }
func (m *stubLogger) Enabled(_ libLog.Level) bool      { return true }
func (m *stubLogger) Sync(_ context.Context) error     { return nil }

// --- NewSchedulerWorker tests ---

func TestNewSchedulerWorker_NilScheduleRepo(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		nil,
		&stubMatchTrigger{},
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, worker)
	require.ErrorIs(t, err, ErrNilScheduleRepository)
}

func TestNewSchedulerWorker_NilMatchTrigger(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		nil,
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, worker)
	require.ErrorIs(t, err, ErrNilMatchTrigger)
}

func TestNewSchedulerWorker_NilInfraProvider(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		nil,
		SchedulerWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, worker)
	require.ErrorIs(t, err, ErrNilInfraProvider)
}

func TestNewSchedulerWorker_DefaultInterval(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: 0},
		&stubLogger{},
	)

	require.NoError(t, err)
	require.NotNil(t, worker)
	assert.Equal(t, time.Minute, worker.cfg.Interval)
}

func TestNewSchedulerWorker_Success(t *testing.T) {
	t.Parallel()

	repo := &stubScheduleRepo{}
	trigger := &stubMatchTrigger{}
	infra := &stubInfraProvider{}
	logger := &stubLogger{}
	interval := 30 * time.Second

	worker, err := NewSchedulerWorker(
		repo,
		trigger,
		infra,
		SchedulerWorkerConfig{Interval: interval},
		logger,
	)

	require.NoError(t, err)
	require.NotNil(t, worker)
	assert.Equal(t, repo, worker.scheduleRepo)
	assert.Equal(t, trigger, worker.matchTrigger)
	assert.Equal(t, infra, worker.infraProvider)
	assert.Equal(t, interval, worker.cfg.Interval)
	assert.Equal(t, logger, worker.logger)
	assert.NotNil(t, worker.tracer)
	assert.NotNil(t, worker.stopCh)
	assert.NotNil(t, worker.doneCh)
}

func TestNewSchedulerWorker_NegativeInterval_DefaultsToMinute(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: -5 * time.Second},
		&stubLogger{},
	)

	require.NoError(t, err)
	require.NotNil(t, worker)
	assert.Equal(t, time.Minute, worker.cfg.Interval)
}

// --- Start/Stop tests ---

func TestSchedulerWorker_Start_AlreadyRunning(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Start the worker.
	err = worker.Start(context.Background())
	require.NoError(t, err)

	// Second Start should fail.
	err = worker.Start(context.Background())
	require.ErrorIs(t, err, ErrWorkerAlreadyRunning)

	// Clean up.
	require.NoError(t, worker.Stop())
}

func TestSchedulerWorker_Stop_NotRunning(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	err = worker.Stop()
	require.ErrorIs(t, err, ErrWorkerNotRunning)
}

func TestSchedulerWorker_StartStop_Success(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	err = worker.Start(context.Background())
	require.NoError(t, err)

	err = worker.Stop()
	require.NoError(t, err)

	// Verify Done channel is closed after Stop.
	select {
	case <-worker.Done():
		// Expected: channel is closed.
	case <-time.After(2 * time.Second):
		t.Fatal("Done channel was not closed after Stop")
	}
}

func TestSchedulerWorker_Done_ClosedAfterStop(t *testing.T) {
	t.Parallel()

	worker, err := NewSchedulerWorker(
		&stubScheduleRepo{},
		&stubMatchTrigger{},
		&stubInfraProvider{},
		SchedulerWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Before start, Done channel should be open (blocking).
	select {
	case <-worker.Done():
		t.Fatal("Done channel should not be closed before Start")
	default:
		// Expected: channel is open.
	}

	require.NoError(t, worker.Start(context.Background()))
	require.NoError(t, worker.Stop())

	// After stop, Done channel should be closed (non-blocking).
	select {
	case <-worker.Done():
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("Done channel should be closed after Stop")
	}
}

// --- Sentinel errors ---

func TestSchedulerWorkerErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrNilScheduleRepository,
		ErrNilMatchTrigger,
		ErrNilInfraProvider,
		ErrWorkerAlreadyRunning,
		ErrWorkerNotRunning,
		ErrRedisClientNil,
	}

	seen := make(map[string]string)
	for _, e := range errs {
		msg := e.Error()
		if prev, exists := seen[msg]; exists {
			t.Errorf("duplicate sentinel error message %q: both %q and current", msg, prev)
		}

		seen[msg] = msg
	}
}
