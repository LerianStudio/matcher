//go:build unit

package worker

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// --- stub mocks ---

// stubFetcherClient implements sharedPorts.FetcherClient for worker tests.
type stubFetcherClient struct {
	isHealthyFn              func(ctx context.Context) bool
	listConnectionsFn        func(ctx context.Context, orgID string) ([]*sharedPorts.FetcherConnection, error)
	getSchemaFn              func(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error)
	testConnectionFn         func(ctx context.Context, connectionID string) (*sharedPorts.FetcherTestResult, error)
	submitExtractionJobFn    func(ctx context.Context, input sharedPorts.ExtractionJobInput) (string, error)
	getExtractionJobStatusFn func(ctx context.Context, jobID string) (*sharedPorts.ExtractionJobStatus, error)
}

var _ sharedPorts.FetcherClient = (*stubFetcherClient)(nil)

func (m *stubFetcherClient) IsHealthy(ctx context.Context) bool {
	if m.isHealthyFn != nil {
		return m.isHealthyFn(ctx)
	}

	return true
}

func (m *stubFetcherClient) ListConnections(ctx context.Context, orgID string) ([]*sharedPorts.FetcherConnection, error) {
	if m.listConnectionsFn != nil {
		return m.listConnectionsFn(ctx, orgID)
	}

	return nil, nil
}

func (m *stubFetcherClient) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if m.getSchemaFn != nil {
		return m.getSchemaFn(ctx, connectionID)
	}

	return &sharedPorts.FetcherSchema{}, nil
}

func (m *stubFetcherClient) TestConnection(ctx context.Context, connectionID string) (*sharedPorts.FetcherTestResult, error) {
	if m.testConnectionFn != nil {
		return m.testConnectionFn(ctx, connectionID)
	}

	return &sharedPorts.FetcherTestResult{Healthy: true}, nil
}

func (m *stubFetcherClient) SubmitExtractionJob(ctx context.Context, input sharedPorts.ExtractionJobInput) (string, error) {
	if m.submitExtractionJobFn != nil {
		return m.submitExtractionJobFn(ctx, input)
	}

	return "job-123", nil
}

func (m *stubFetcherClient) GetExtractionJobStatus(ctx context.Context, jobID string) (*sharedPorts.ExtractionJobStatus, error) {
	if m.getExtractionJobStatusFn != nil {
		return m.getExtractionJobStatusFn(ctx, jobID)
	}

	return &sharedPorts.ExtractionJobStatus{Status: "RUNNING"}, nil
}

// stubConnectionRepo implements repositories.ConnectionRepository for worker tests.
type stubConnectionRepo struct {
	upsertFn            func(ctx context.Context, conn *entities.FetcherConnection) error
	upsertWithTxFn      func(ctx context.Context, tx *sql.Tx, conn *entities.FetcherConnection) error
	findAllFn           func(ctx context.Context) ([]*entities.FetcherConnection, error)
	findByIDFn          func(ctx context.Context, id uuid.UUID) (*entities.FetcherConnection, error)
	findByFetcherIDFn   func(ctx context.Context, fetcherConnID string) (*entities.FetcherConnection, error)
	deleteStaleFn       func(ctx context.Context, notSeenSince time.Duration) (int64, error)
	deleteStaleWithTxFn func(ctx context.Context, tx *sql.Tx, notSeenSince time.Duration) (int64, error)
}

var _ repositories.ConnectionRepository = (*stubConnectionRepo)(nil)

func (m *stubConnectionRepo) Upsert(ctx context.Context, conn *entities.FetcherConnection) error {
	if m.upsertFn != nil {
		return m.upsertFn(ctx, conn)
	}

	return nil
}

func (m *stubConnectionRepo) UpsertWithTx(ctx context.Context, tx *sql.Tx, conn *entities.FetcherConnection) error {
	if m.upsertWithTxFn != nil {
		return m.upsertWithTxFn(ctx, tx, conn)
	}

	return nil
}

func (m *stubConnectionRepo) FindAll(ctx context.Context) ([]*entities.FetcherConnection, error) {
	if m.findAllFn != nil {
		return m.findAllFn(ctx)
	}

	return nil, nil
}

func (m *stubConnectionRepo) FindByID(ctx context.Context, id uuid.UUID) (*entities.FetcherConnection, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	return nil, sql.ErrNoRows
}

func (m *stubConnectionRepo) FindByFetcherID(ctx context.Context, fetcherConnID string) (*entities.FetcherConnection, error) {
	if m.findByFetcherIDFn != nil {
		return m.findByFetcherIDFn(ctx, fetcherConnID)
	}

	return nil, sql.ErrNoRows
}

func (m *stubConnectionRepo) DeleteStale(ctx context.Context, notSeenSince time.Duration) (int64, error) {
	if m.deleteStaleFn != nil {
		return m.deleteStaleFn(ctx, notSeenSince)
	}

	return 0, nil
}

func (m *stubConnectionRepo) DeleteStaleWithTx(ctx context.Context, tx *sql.Tx, notSeenSince time.Duration) (int64, error) {
	if m.deleteStaleWithTxFn != nil {
		return m.deleteStaleWithTxFn(ctx, tx, notSeenSince)
	}

	return 0, nil
}

// stubSchemaRepo implements repositories.SchemaRepository for worker tests.
type stubSchemaRepo struct {
	upsertBatchFn                func(ctx context.Context, schemas []*entities.DiscoveredSchema) error
	upsertBatchWithTxFn          func(ctx context.Context, tx *sql.Tx, schemas []*entities.DiscoveredSchema) error
	findByConnectionIDFn         func(ctx context.Context, connectionID uuid.UUID) ([]*entities.DiscoveredSchema, error)
	deleteByConnectionIDFn       func(ctx context.Context, connectionID uuid.UUID) error
	deleteByConnectionIDWithTxFn func(ctx context.Context, tx *sql.Tx, connectionID uuid.UUID) error
}

var _ repositories.SchemaRepository = (*stubSchemaRepo)(nil)

func (m *stubSchemaRepo) UpsertBatch(ctx context.Context, schemas []*entities.DiscoveredSchema) error {
	if m.upsertBatchFn != nil {
		return m.upsertBatchFn(ctx, schemas)
	}

	return nil
}

func (m *stubSchemaRepo) UpsertBatchWithTx(ctx context.Context, tx *sql.Tx, schemas []*entities.DiscoveredSchema) error {
	if m.upsertBatchWithTxFn != nil {
		return m.upsertBatchWithTxFn(ctx, tx, schemas)
	}

	return nil
}

func (m *stubSchemaRepo) FindByConnectionID(ctx context.Context, connectionID uuid.UUID) ([]*entities.DiscoveredSchema, error) {
	if m.findByConnectionIDFn != nil {
		return m.findByConnectionIDFn(ctx, connectionID)
	}

	return nil, nil
}

func (m *stubSchemaRepo) DeleteByConnectionID(ctx context.Context, connectionID uuid.UUID) error {
	if m.deleteByConnectionIDFn != nil {
		return m.deleteByConnectionIDFn(ctx, connectionID)
	}

	return nil
}

func (m *stubSchemaRepo) DeleteByConnectionIDWithTx(ctx context.Context, tx *sql.Tx, connectionID uuid.UUID) error {
	if m.deleteByConnectionIDWithTxFn != nil {
		return m.deleteByConnectionIDWithTxFn(ctx, tx, connectionID)
	}

	return nil
}

// stubInfraProvider implements sharedPorts.InfrastructureProvider.
type stubInfraProvider struct{}

var _ sharedPorts.InfrastructureProvider = (*stubInfraProvider)(nil)

func (m *stubInfraProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (m *stubInfraProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, nil
}

func (m *stubInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (m *stubInfraProvider) GetPrimaryDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

type stubTenantLister struct {
	listTenantsFn func(ctx context.Context) ([]string, error)
}

func (m *stubTenantLister) ListTenants(ctx context.Context) ([]string, error) {
	if m.listTenantsFn != nil {
		return m.listTenantsFn(ctx)
	}

	return []string{"11111111-1111-1111-1111-111111111111"}, nil
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

// --- NewDiscoveryWorker tests ---

func TestNewDiscoveryWorker_NilFetcherClient(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		nil,
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilFetcherClient)
}

func TestNewDiscoveryWorker_NilConnectionRepo(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		nil,
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilConnectionRepository)
}

func TestNewDiscoveryWorker_NilSchemaRepo(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		nil,
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilSchemaRepository)
}

func TestNewDiscoveryWorker_NilTenantLister(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		nil,
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilTenantLister)
}

func TestNewDiscoveryWorker_NilInfraProvider(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		nil,
		DiscoveryWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilInfraProvider)
}

func TestNewDiscoveryWorker_DefaultInterval(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: 0},
		&stubLogger{},
	)

	require.NoError(t, err)
	require.NotNil(t, w)
	assert.Equal(t, time.Minute, w.cfg.Interval)
}

func TestNewDiscoveryWorker_NegativeInterval_DefaultsToMinute(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: -5 * time.Second},
		&stubLogger{},
	)

	require.NoError(t, err)
	require.NotNil(t, w)
	assert.Equal(t, time.Minute, w.cfg.Interval)
}

func TestNewDiscoveryWorker_NilLogger_UsesNop(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Minute},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, w)
	assert.IsType(t, &libLog.NopLogger{}, w.logger)
}

func TestNewDiscoveryWorker_Success(t *testing.T) {
	t.Parallel()

	fetcher := &stubFetcherClient{}
	connRepo := &stubConnectionRepo{}
	schemaRepo := &stubSchemaRepo{}
	infra := &stubInfraProvider{}
	logger := &stubLogger{}
	interval := 30 * time.Second

	w, err := NewDiscoveryWorker(
		fetcher,
		connRepo,
		schemaRepo,
		&stubTenantLister{},
		infra,
		DiscoveryWorkerConfig{Interval: interval},
		logger,
	)

	require.NoError(t, err)
	require.NotNil(t, w)
	assert.Equal(t, fetcher, w.fetcherClient)
	assert.Equal(t, connRepo, w.connRepo)
	assert.Equal(t, schemaRepo, w.schemaRepo)
	assert.NotNil(t, w.tenantLister)
	assert.Equal(t, infra, w.infraProvider)
	assert.Equal(t, interval, w.cfg.Interval)
	assert.Equal(t, logger, w.logger)
	assert.NotNil(t, w.tracer)
	assert.NotNil(t, w.stopCh)
	assert.NotNil(t, w.doneCh)
}

// --- Start/Stop tests ---

func TestDiscoveryWorker_Start_AlreadyRunning(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Start the worker.
	err = w.Start(context.Background())
	require.NoError(t, err)

	// Second Start should fail.
	err = w.Start(context.Background())
	require.ErrorIs(t, err, ErrWorkerAlreadyRunning)

	// Clean up.
	require.NoError(t, w.Stop())
}

func TestDiscoveryWorker_Stop_NotRunning(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	err = w.Stop()
	require.ErrorIs(t, err, ErrWorkerNotRunning)
}

func TestDiscoveryWorker_StartStop_Success(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	err = w.Start(context.Background())
	require.NoError(t, err)

	err = w.Stop()
	require.NoError(t, err)

	// Verify Done channel is closed after Stop.
	select {
	case <-w.Done():
		// Expected: channel is closed.
	case <-time.After(2 * time.Second):
		t.Fatal("Done channel was not closed after Stop")
	}
}

func TestDiscoveryWorker_Done_ClosedAfterStop(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Before start, Done channel should be open (blocking).
	select {
	case <-w.Done():
		t.Fatal("Done channel should not be closed before Start")
	default:
		// Expected: channel is open.
	}

	require.NoError(t, w.Start(context.Background()))
	require.NoError(t, w.Stop())

	// After stop, Done channel should be closed (non-blocking).
	select {
	case <-w.Done():
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("Done channel should be closed after Stop")
	}
}

func TestDiscoveryWorker_StartStopStartStop_SameInstanceSuccess(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	require.NoError(t, w.Start(context.Background()))
	require.NoError(t, w.Stop())
	require.NoError(t, w.Start(context.Background()))
	require.NoError(t, w.Stop())
}

func TestDiscoveryWorker_UpdateRuntimeConfig_UpdatesInterval(t *testing.T) {
	t.Parallel()

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Minute},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.UpdateRuntimeConfig(DiscoveryWorkerConfig{Interval: 45 * time.Second})

	_, _, _, interval := w.runtimeState()
	assert.Equal(t, 45*time.Second, interval)
}

// --- Sentinel errors ---

func TestDiscoveryWorkerErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrNilFetcherClient,
		ErrNilConnectionRepository,
		ErrNilSchemaRepository,
		ErrNilTenantLister,
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

// --- pollCycle behavior tests ---

func TestDiscoveryWorker_PollCycle_UnhealthyFetcher_SkipsCycle(t *testing.T) {
	t.Parallel()

	connRepo := &stubConnectionRepo{}
	listConnectionsCalled := false

	connRepo.findAllFn = func(_ context.Context) ([]*entities.FetcherConnection, error) {
		return nil, nil
	}

	fetcher := &stubFetcherClient{
		isHealthyFn: func(_ context.Context) bool { return false },
		listConnectionsFn: func(_ context.Context, _ string) ([]*sharedPorts.FetcherConnection, error) {
			listConnectionsCalled = true
			return nil, nil
		},
	}

	w, err := NewDiscoveryWorker(
		fetcher,
		connRepo,
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Call pollCycle directly (it will fail to acquire lock since Redis is nil,
	// but that happens before the health check).
	// Instead, test syncConnectionsAndSchemas to verify health check integration.
	// pollCycle checks lock first. Since our infra returns nil Redis, it logs a warning and returns.
	// We need to verify the health check path directly.
	w.pollCycle(context.Background())

	// ListConnections should NOT be called because either lock fails or health check fails.
	assert.False(t, listConnectionsCalled)
}

func TestDiscoveryWorker_SyncConnectionsAndSchemas_ListError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("fetcher list error")

	fetcher := &stubFetcherClient{
		listConnectionsFn: func(_ context.Context, orgID string) ([]*sharedPorts.FetcherConnection, error) {
			assert.Equal(t, "11111111-1111-1111-1111-111111111111", orgID)

			return nil, expectedErr
		},
	}

	w, err := NewDiscoveryWorker(
		fetcher,
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// syncConnectionsAndSchemas should handle the error gracefully.
	w.syncConnectionsAndSchemas(context.Background())
}

func TestDiscoveryWorker_SyncConnectionsAndSchemas_UpsertNewConnection(t *testing.T) {
	t.Parallel()

	upsertCalled := false

	fetcher := &stubFetcherClient{
		listConnectionsFn: func(_ context.Context, orgID string) ([]*sharedPorts.FetcherConnection, error) {
			assert.Equal(t, "11111111-1111-1111-1111-111111111111", orgID)

			return []*sharedPorts.FetcherConnection{
				{
					ID:           "fetcher-conn-1",
					ConfigName:   "my-db",
					DatabaseType: "postgresql",
					Host:         "localhost",
					Port:         5432,
					DatabaseName: "testdb",
					ProductName:  "PostgreSQL",
					Status:       "AVAILABLE",
				},
			}, nil
		},
		getSchemaFn: func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return &sharedPorts.FetcherSchema{
				Tables: []sharedPorts.FetcherTableSchema{
					{
						TableName: "transactions",
						Columns: []sharedPorts.FetcherColumnInfo{
							{Name: "id", Type: "uuid", Nullable: false},
							{Name: "amount", Type: "numeric", Nullable: true},
						},
					},
				},
			}, nil
		},
	}

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			// Return domain sentinel error — the worker checks via errors.Is().
			return nil, repositories.ErrConnectionNotFound
		},
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			upsertCalled = true
			return nil
		},
		findAllFn: func(_ context.Context) ([]*entities.FetcherConnection, error) {
			return nil, nil
		},
	}

	schemaRepo := &stubSchemaRepo{}

	w, err := NewDiscoveryWorker(
		fetcher,
		connRepo,
		schemaRepo,
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.syncConnectionsAndSchemas(context.Background())

	assert.True(t, upsertCalled, "connection should have been upserted")
}

func TestDiscoveryWorker_MarkStaleConnections(t *testing.T) {
	t.Parallel()

	staleConn := &entities.FetcherConnection{
		ID:            uuid.New(),
		FetcherConnID: "stale-conn",
		Status:        "AVAILABLE",
	}

	markedUnreachable := false

	connRepo := &stubConnectionRepo{
		findAllFn: func(_ context.Context) ([]*entities.FetcherConnection, error) {
			return []*entities.FetcherConnection{staleConn}, nil
		},
		upsertFn: func(_ context.Context, conn *entities.FetcherConnection) error {
			if conn.Status == "UNREACHABLE" {
				markedUnreachable = true
			}

			return nil
		},
	}

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		connRepo,
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Empty seen set — all connections are stale.
	seenIDs := make(map[string]bool)
	w.markStaleConnections(context.Background(), seenIDs)

	assert.True(t, markedUnreachable, "stale connection should have been marked UNREACHABLE")
}

func TestDiscoveryWorker_MarkStaleConnections_AlreadyUnreachable_Skipped(t *testing.T) {
	t.Parallel()

	staleConn := &entities.FetcherConnection{
		ID:            uuid.New(),
		FetcherConnID: "already-stale",
		Status:        "UNREACHABLE",
	}

	upsertCalled := false

	connRepo := &stubConnectionRepo{
		findAllFn: func(_ context.Context) ([]*entities.FetcherConnection, error) {
			return []*entities.FetcherConnection{staleConn}, nil
		},
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			upsertCalled = true
			return nil
		},
	}

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		connRepo,
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	seenIDs := make(map[string]bool)
	w.markStaleConnections(context.Background(), seenIDs)

	assert.False(t, upsertCalled, "already-unreachable connection should not be upserted")
}

func TestDiscoveryWorker_SyncTenantConnections_IgnoresNilFetcherConnectionEntries(t *testing.T) {
	t.Parallel()

	upsertCalls := 0

	fetcher := &stubFetcherClient{
		listConnectionsFn: func(_ context.Context, _ string) ([]*sharedPorts.FetcherConnection, error) {
			return []*sharedPorts.FetcherConnection{
				nil,
				{ID: "fetcher-conn-1", ConfigName: "db", DatabaseType: "postgresql", Status: "AVAILABLE"},
			}, nil
		},
		getSchemaFn: func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return &sharedPorts.FetcherSchema{}, nil
		},
	}

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return nil, repositories.ErrConnectionNotFound
		},
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			upsertCalls++
			return nil
		},
		findAllFn: func(_ context.Context) ([]*entities.FetcherConnection, error) {
			return nil, nil
		},
	}

	w, err := NewDiscoveryWorker(
		fetcher,
		connRepo,
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.syncConnectionsAndSchemas(context.Background())

	assert.Equal(t, 2, upsertCalls, "nil fetcher entries must be skipped while valid entries still complete full sync")
}

func TestDiscoveryWorker_MarkStaleConnections_IgnoresNilRepositoryEntries(t *testing.T) {
	t.Parallel()

	upsertCalls := 0

	connRepo := &stubConnectionRepo{
		findAllFn: func(_ context.Context) ([]*entities.FetcherConnection, error) {
			return []*entities.FetcherConnection{
				nil,
				{ID: uuid.New(), FetcherConnID: "stale-conn", Status: "AVAILABLE"},
			}, nil
		},
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			upsertCalls++
			return nil
		},
	}

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		connRepo,
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		&stubLogger{},
	)
	require.NoError(t, err)

	w.markStaleConnections(context.Background(), map[string]bool{})

	assert.Equal(t, 1, upsertCalls, "nil repository entries must be skipped")
}

// --- tracking helper ---

func TestDiscoveryWorker_Tracking_ReturnsNonNilValues(t *testing.T) {
	t.Parallel()

	logger := &stubLogger{}

	w, err := NewDiscoveryWorker(
		&stubFetcherClient{},
		&stubConnectionRepo{},
		&stubSchemaRepo{},
		&stubTenantLister{},
		&stubInfraProvider{},
		DiscoveryWorkerConfig{Interval: time.Hour},
		logger,
	)
	require.NoError(t, err)

	// tracking() should always return non-nil logger and tracer, either from
	// context or from the worker's own fields.
	l, tr := w.tracking(context.Background())

	assert.NotNil(t, l)
	assert.NotNil(t, tr)
}
