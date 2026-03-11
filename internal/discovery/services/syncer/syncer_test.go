//go:build unit

package syncer

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ---------------------------------------------------------------------------
// Stub: ConnectionRepository
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub: SchemaRepository
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub: Logger (libLog.Logger from lib-commons v2)
// ---------------------------------------------------------------------------

type stubLogger struct {
	logCount atomic.Int32
}

var _ libLog.Logger = (*stubLogger)(nil)

func (m *stubLogger) Log(_ context.Context, _ libLog.Level, _ string, _ ...libLog.Field) {
	m.logCount.Add(1)
}

//nolint:ireturn
func (m *stubLogger) With(_ ...libLog.Field) libLog.Logger { return m }

//nolint:ireturn
func (m *stubLogger) WithGroup(_ string) libLog.Logger { return m }
func (m *stubLogger) Enabled(_ libLog.Level) bool      { return true }
func (m *stubLogger) Sync(_ context.Context) error     { return nil }

type stubSchemaCache struct {
	setSchemaFn func(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error
}

var _ discoveryPorts.SchemaCache = (*stubSchemaCache)(nil)

func (m *stubSchemaCache) GetSchema(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
	return nil, nil
}

func (m *stubSchemaCache) SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
	if m.setSchemaFn != nil {
		return m.setSchemaFn(ctx, connectionID, schema, ttl)
	}

	return nil
}

func (m *stubSchemaCache) InvalidateSchema(_ context.Context, _ string) error {
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeFetcherConnection creates a sharedPorts.FetcherConnection with sensible test defaults.
func makeFetcherConnection(id, configName, dbType string) *sharedPorts.FetcherConnection {
	return &sharedPorts.FetcherConnection{
		ID:           id,
		ConfigName:   configName,
		DatabaseType: dbType,
		Host:         "db.example.com",
		Port:         5432,
		DatabaseName: "txns",
		ProductName:  "PostgreSQL 17.1",
		Status:       "AVAILABLE",
	}
}

// makeExistingConnection creates an entities.FetcherConnection as if it were loaded from the DB.
func makeExistingConnection(fetcherConnID string) *entities.FetcherConnection {
	conn, _ := entities.NewFetcherConnection(context.Background(), fetcherConnID, "old-config", "POSTGRESQL")

	return conn
}

// mustNewSyncer creates a ConnectionSyncer, failing the test on error (test helper only).
func mustNewSyncer(t *testing.T, connRepo repositories.ConnectionRepository, schemaRepo repositories.SchemaRepository) *ConnectionSyncer {
	t.Helper()

	cs, err := NewConnectionSyncer(connRepo, schemaRepo)
	require.NoError(t, err)

	return cs
}

// ---------------------------------------------------------------------------
// NewConnectionSyncer tests
// ---------------------------------------------------------------------------

func TestNewConnectionSyncer_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	s := mustNewSyncer(t, &stubConnectionRepo{}, &stubSchemaRepo{})

	require.NotNil(t, s)
	assert.NotNil(t, s.connRepo)
	assert.NotNil(t, s.schemaRepo)
}

func TestNewConnectionSyncer_NilConnRepo_ReturnsError(t *testing.T) {
	t.Parallel()

	s, err := NewConnectionSyncer(nil, &stubSchemaRepo{})

	require.ErrorIs(t, err, ErrNilConnectionRepository)
	assert.Nil(t, s)
}

func TestNewConnectionSyncer_NilSchemaRepo_ReturnsError(t *testing.T) {
	t.Parallel()

	s, err := NewConnectionSyncer(&stubConnectionRepo{}, nil)

	require.ErrorIs(t, err, ErrNilSchemaRepository)
	assert.Nil(t, s)
}

// ---------------------------------------------------------------------------
// SyncConnection — guard clause tests
// ---------------------------------------------------------------------------

func TestSyncConnection_NilSyncer_ReturnsErrNilSyncer(t *testing.T) {
	t.Parallel()

	var cs *ConnectionSyncer

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		makeFetcherConnection("fc-1", "primary", "POSTGRESQL"),
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil
		},
	)

	require.ErrorIs(t, err, ErrNilSyncer)
}

func TestSyncConnection_NilConnection_ReturnsErrNilConnection(t *testing.T) {
	t.Parallel()

	cs := mustNewSyncer(t, &stubConnectionRepo{}, &stubSchemaRepo{})

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		nil,
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil
		},
	)

	require.ErrorIs(t, err, ErrNilConnection)
}

func TestSyncConnection_NilFetchSchema_ReturnsErrNilFetcher(t *testing.T) {
	t.Parallel()

	cs := mustNewSyncer(t, &stubConnectionRepo{}, &stubSchemaRepo{})

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		makeFetcherConnection("fc-1", "primary", "POSTGRESQL"),
		nil,
	)

	require.ErrorIs(t, err, ErrNilFetcher)
}

// ---------------------------------------------------------------------------
// SyncConnection — upsert existing connection + schema sync
// ---------------------------------------------------------------------------

func TestSyncConnection_ExistingConnection_UpsertsAndSyncsSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	existing := makeExistingConnection("fc-existing")

	var upsertCount atomic.Int32

	var batchUpsertCalled atomic.Bool

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, fetcherConnID string) (*entities.FetcherConnection, error) {
			assert.Equal(t, "fc-existing", fetcherConnID)

			return existing, nil
		},
		upsertFn: func(_ context.Context, conn *entities.FetcherConnection) error {
			upsertCount.Add(1)
			// First call: upsertConnection path. Second call: SyncSchema marking discovered.
			assert.Equal(t, existing.ID, conn.ID)

			return nil
		},
	}

	schemaRepo := &stubSchemaRepo{
		upsertBatchFn: func(_ context.Context, schemas []*entities.DiscoveredSchema) error {
			batchUpsertCalled.Store(true)
			require.Len(t, schemas, 1)
			assert.Equal(t, "orders", schemas[0].TableName)

			return nil
		},
	}

	cs := mustNewSyncer(t, connRepo, schemaRepo)

	fc := makeFetcherConnection("fc-existing", "updated-config", "POSTGRESQL")

	fetchSchema := func(_ context.Context, connID string) (*sharedPorts.FetcherSchema, error) {
		assert.Equal(t, "fc-existing", connID)

		return &sharedPorts.FetcherSchema{
			ConnectionID: connID,
			Tables: []sharedPorts.FetcherTableSchema{
				{
					TableName: "orders",
					Columns: []sharedPorts.FetcherColumnInfo{
						{Name: "id", Type: "uuid", Nullable: false},
						{Name: "amount", Type: "numeric", Nullable: true},
					},
				},
			},
		}, nil
	}

	err := cs.SyncConnection(ctx, &stubLogger{}, fc, fetchSchema)

	require.NoError(t, err)
	// Two upserts: one for upsertConnection, one for marking schema discovered.
	assert.Equal(t, int32(2), upsertCount.Load())
	assert.True(t, batchUpsertCalled.Load())
	assert.True(t, existing.SchemaDiscovered)
}

// ---------------------------------------------------------------------------
// SyncConnection — new connection creation when none exists
// ---------------------------------------------------------------------------

func TestSyncConnection_NewConnection_CreatesAndSyncsSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var upsertedConns []*entities.FetcherConnection

	var batchUpsertCalled atomic.Bool

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			// Not found — triggers new connection creation.
			return nil, repositories.ErrConnectionNotFound
		},
		upsertFn: func(_ context.Context, conn *entities.FetcherConnection) error {
			upsertedConns = append(upsertedConns, conn)

			return nil
		},
	}

	schemaRepo := &stubSchemaRepo{
		upsertBatchFn: func(_ context.Context, schemas []*entities.DiscoveredSchema) error {
			batchUpsertCalled.Store(true)
			require.Len(t, schemas, 2)

			return nil
		},
	}

	cs := mustNewSyncer(t, connRepo, schemaRepo)

	fc := makeFetcherConnection("fc-new", "new-config", "MYSQL")

	fetchSchema := func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
		return &sharedPorts.FetcherSchema{
			Tables: []sharedPorts.FetcherTableSchema{
				{TableName: "payments", Columns: []sharedPorts.FetcherColumnInfo{{Name: "id", Type: "int"}}},
				{TableName: "refunds", Columns: []sharedPorts.FetcherColumnInfo{{Name: "id", Type: "int"}}},
			},
		}, nil
	}

	err := cs.SyncConnection(ctx, &stubLogger{}, fc, fetchSchema)

	require.NoError(t, err)
	// Two upserts: upsertConnection (new) + SyncSchema update.
	require.Len(t, upsertedConns, 2)
	assert.Equal(t, "fc-new", upsertedConns[0].FetcherConnID)
	assert.Equal(t, "new-config", upsertedConns[0].ConfigName)
	assert.Equal(t, "MYSQL", upsertedConns[0].DatabaseType)
	assert.True(t, batchUpsertCalled.Load())
	// SyncSchema marks the connection discovered.
	assert.True(t, upsertedConns[1].SchemaDiscovered)
}

// ---------------------------------------------------------------------------
// SyncConnection — FindByFetcherID unexpected error propagates
// ---------------------------------------------------------------------------

func TestSyncConnection_FindByFetcherID_UnexpectedError_Propagates(t *testing.T) {
	t.Parallel()

	dbErr := errors.New("database connection refused")

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return nil, dbErr
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		makeFetcherConnection("fc-1", "c", "PG"),
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, dbErr)
	assert.Contains(t, err.Error(), "find connection by fetcher id")
}

// ---------------------------------------------------------------------------
// SyncConnection — upsert failure on existing connection
// ---------------------------------------------------------------------------

func TestSyncConnection_UpsertExistingFails_ReturnsError(t *testing.T) {
	t.Parallel()

	upsertErr := errors.New("unique constraint violation")
	existing := makeExistingConnection("fc-1")

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return existing, nil
		},
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			return upsertErr
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		makeFetcherConnection("fc-1", "c", "PG"),
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, upsertErr)
	assert.Contains(t, err.Error(), "upsert existing connection")
}

// ---------------------------------------------------------------------------
// SyncConnection — upsert failure on new connection
// ---------------------------------------------------------------------------

func TestSyncConnection_UpsertNewFails_ReturnsError(t *testing.T) {
	t.Parallel()

	upsertErr := errors.New("disk full")

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return nil, repositories.ErrConnectionNotFound
		},
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			return upsertErr
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		makeFetcherConnection("fc-new", "c", "PG"),
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, upsertErr)
	assert.Contains(t, err.Error(), "upsert new connection")
}

func TestSyncConnection_InvalidPort_ReturnsError(t *testing.T) {
	t.Parallel()

	cs := mustNewSyncer(t, &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return nil, repositories.ErrConnectionNotFound
		},
	}, &stubSchemaRepo{})
	fc := makeFetcherConnection("fc-invalid-port", "c", "PG")
	fc.Port = 70000

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		fc,
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidConnectionPort)
	assert.Contains(t, err.Error(), "update new connection details")
}

// ---------------------------------------------------------------------------
// SyncConnection — schema fetch failure logs warning but succeeds
// ---------------------------------------------------------------------------

func TestSyncConnection_SchemaFetchFails_LogsWarning_ReturnsNil(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	existing := makeExistingConnection("fc-warn")
	logger := &stubLogger{}

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return existing, nil
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})

	fetchSchema := func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
		return nil, errors.New("network timeout")
	}

	err := cs.SyncConnection(ctx, logger, makeFetcherConnection("fc-warn", "c", "PG"), fetchSchema)

	require.NoError(t, err, "schema fetch failure is best-effort — should not propagate")
	assert.GreaterOrEqual(t, logger.logCount.Load(), int32(1), "warning should be logged")
}

func TestSyncConnection_SchemaFetchFails_NilLogger_DoesNotPanic(t *testing.T) {
	t.Parallel()

	existing := makeExistingConnection("fc-nil-log")

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return existing, nil
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})

	fetchSchema := func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
		return nil, errors.New("connection refused")
	}

	assert.NotPanics(t, func() {
		err := cs.SyncConnection(context.Background(), nil, makeFetcherConnection("fc-nil-log", "c", "PG"), fetchSchema)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// SyncConnection — SyncSchema failure logs warning but SyncConnection still returns nil
// ---------------------------------------------------------------------------

func TestSyncConnection_SchemaSyncFails_LogsWarning_ReturnsNil(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	existing := makeExistingConnection("fc-sync-fail")
	logger := &stubLogger{}

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return existing, nil
		},
	}

	schemaRepo := &stubSchemaRepo{
		upsertBatchFn: func(_ context.Context, _ []*entities.DiscoveredSchema) error {
			return errors.New("batch insert failed")
		},
	}

	cs := mustNewSyncer(t, connRepo, schemaRepo)

	fetchSchema := func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
		return &sharedPorts.FetcherSchema{
			Tables: []sharedPorts.FetcherTableSchema{
				{TableName: "t1", Columns: []sharedPorts.FetcherColumnInfo{{Name: "id", Type: "int"}}},
			},
		}, nil
	}

	err := cs.SyncConnection(ctx, logger, makeFetcherConnection("fc-sync-fail", "c", "PG"), fetchSchema)

	require.NoError(t, err, "SyncSchema failure is best-effort — SyncConnection should still succeed")
	assert.GreaterOrEqual(t, logger.logCount.Load(), int32(1), "warning should be logged")
}

// ---------------------------------------------------------------------------
// SyncSchema — nil schema / empty tables → early return nil
// ---------------------------------------------------------------------------

func TestSyncSchema_NilSchema_ReturnsNil(t *testing.T) {
	t.Parallel()

	cs := mustNewSyncer(t, &stubConnectionRepo{}, &stubSchemaRepo{})
	conn := makeExistingConnection("fc-1")

	err := cs.SyncSchema(context.Background(), conn, nil)

	require.NoError(t, err)
}

func TestSyncSchema_EmptyTables_ReturnsNil(t *testing.T) {
	t.Parallel()

	cs := mustNewSyncer(t, &stubConnectionRepo{}, &stubSchemaRepo{})
	conn := makeExistingConnection("fc-1")

	err := cs.SyncSchema(context.Background(), conn, &sharedPorts.FetcherSchema{
		Tables: []sharedPorts.FetcherTableSchema{},
	})

	require.NoError(t, err)
}

func TestSyncSchema_EmptyTables_ReplacesPersistedSnapshot(t *testing.T) {
	t.Parallel()

	var upsertBatchCalled atomic.Bool

	var connUpsertCalled atomic.Bool

	var deleteCalled atomic.Bool

	connRepo := &stubConnectionRepo{
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			connUpsertCalled.Store(true)

			return nil
		},
	}

	schemaRepo := &stubSchemaRepo{
		upsertBatchFn: func(_ context.Context, _ []*entities.DiscoveredSchema) error {
			upsertBatchCalled.Store(true)

			return nil
		},
		deleteByConnectionIDFn: func(_ context.Context, _ uuid.UUID) error {
			deleteCalled.Store(true)

			return nil
		},
	}

	cs := mustNewSyncer(t, connRepo, schemaRepo)
	conn := makeExistingConnection("fc-1")

	err := cs.SyncSchema(context.Background(), conn, &sharedPorts.FetcherSchema{})

	require.NoError(t, err)
	assert.False(t, upsertBatchCalled.Load(), "UpsertBatch should not be called for empty tables")
	assert.True(t, deleteCalled.Load(), "existing schemas should be removed for empty snapshots")
	assert.True(t, connUpsertCalled.Load(), "connection should be marked schema-discovered after empty snapshot replacement")
}

// ---------------------------------------------------------------------------
// SyncSchema — successful persistence + marks connection discovered
// ---------------------------------------------------------------------------

func TestSyncSchema_Success_PersistsAndMarksDiscovered(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := makeExistingConnection("fc-schema-ok")

	assert.False(t, conn.SchemaDiscovered, "precondition: not yet discovered")

	var persistedSchemas []*entities.DiscoveredSchema

	var markedConn *entities.FetcherConnection

	connRepo := &stubConnectionRepo{
		upsertFn: func(_ context.Context, c *entities.FetcherConnection) error {
			markedConn = c

			return nil
		},
	}

	schemaRepo := &stubSchemaRepo{
		upsertBatchFn: func(_ context.Context, schemas []*entities.DiscoveredSchema) error {
			persistedSchemas = schemas

			return nil
		},
	}

	cs := mustNewSyncer(t, connRepo, schemaRepo)

	schema := &sharedPorts.FetcherSchema{
		ConnectionID: "fc-schema-ok",
		Tables: []sharedPorts.FetcherTableSchema{
			{
				TableName: "transactions",
				Columns: []sharedPorts.FetcherColumnInfo{
					{Name: "id", Type: "uuid", Nullable: false},
					{Name: "amount", Type: "numeric(18,4)", Nullable: false},
					{Name: "description", Type: "text", Nullable: true},
				},
			},
			{
				TableName: "accounts",
				Columns: []sharedPorts.FetcherColumnInfo{
					{Name: "id", Type: "uuid", Nullable: false},
				},
			},
		},
	}

	err := cs.SyncSchema(ctx, conn, schema)

	require.NoError(t, err)

	// Verify schemas were persisted.
	require.Len(t, persistedSchemas, 2)
	assert.Equal(t, "transactions", persistedSchemas[0].TableName)
	assert.Equal(t, "accounts", persistedSchemas[1].TableName)

	// Verify columns mapped correctly on the first table.
	require.Len(t, persistedSchemas[0].Columns, 3)
	assert.Equal(t, "id", persistedSchemas[0].Columns[0].Name)
	assert.Equal(t, "uuid", persistedSchemas[0].Columns[0].Type)
	assert.False(t, persistedSchemas[0].Columns[0].Nullable)
	assert.True(t, persistedSchemas[0].Columns[2].Nullable)

	// Verify each schema entity has the connection's internal UUID (not the fetcherConnID string).
	for _, s := range persistedSchemas {
		assert.Equal(t, conn.ID, s.ConnectionID)
		assert.NotEqual(t, uuid.Nil, s.ID)
	}

	// Verify connection marked as schema-discovered.
	assert.True(t, conn.SchemaDiscovered)
	require.NotNil(t, markedConn)
	assert.True(t, markedConn.SchemaDiscovered)
}

func TestSyncSchema_Success_RefreshesSchemaCache(t *testing.T) {
	t.Parallel()

	conn := makeExistingConnection("fc-cache")

	var cachedSchema *sharedPorts.FetcherSchema
	var cachedConnID string
	var cachedTTL time.Duration

	cs := mustNewSyncer(t, &stubConnectionRepo{upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
		return nil
	}}, &stubSchemaRepo{upsertBatchFn: func(_ context.Context, _ []*entities.DiscoveredSchema) error {
		return nil
	}})
	cs.WithSchemaCache(&stubSchemaCache{setSchemaFn: func(_ context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
		cachedConnID = connectionID
		cachedSchema = schema
		cachedTTL = ttl
		return nil
	}}, 2*time.Minute)

	err := cs.SyncSchema(context.Background(), conn, &sharedPorts.FetcherSchema{
		ConnectionID: "fc-cache",
		Tables: []sharedPorts.FetcherTableSchema{{
			TableName: "transactions",
			Columns:   []sharedPorts.FetcherColumnInfo{{Name: "id", Type: "uuid", Nullable: false}},
		}},
	})

	require.NoError(t, err)
	assert.Equal(t, conn.ID.String(), cachedConnID)
	require.NotNil(t, cachedSchema)
	require.Len(t, cachedSchema.Tables, 1)
	assert.Equal(t, "transactions", cachedSchema.Tables[0].TableName)
	assert.Equal(t, 2*time.Minute, cachedTTL)
}

// ---------------------------------------------------------------------------
// SyncSchema — UpsertBatch failure propagates
// ---------------------------------------------------------------------------

func TestSyncSchema_UpsertBatchFails_ReturnsError(t *testing.T) {
	t.Parallel()

	batchErr := errors.New("serialization failure")

	schemaRepo := &stubSchemaRepo{
		upsertBatchFn: func(_ context.Context, _ []*entities.DiscoveredSchema) error {
			return batchErr
		},
	}

	cs := mustNewSyncer(t, &stubConnectionRepo{}, schemaRepo)
	conn := makeExistingConnection("fc-1")

	schema := &sharedPorts.FetcherSchema{
		Tables: []sharedPorts.FetcherTableSchema{
			{TableName: "t1", Columns: []sharedPorts.FetcherColumnInfo{{Name: "id", Type: "int"}}},
		},
	}

	err := cs.SyncSchema(context.Background(), conn, schema)

	require.Error(t, err)
	assert.ErrorIs(t, err, batchErr)
	assert.Contains(t, err.Error(), "upsert schemas")
}

// ---------------------------------------------------------------------------
// SyncSchema — Upsert (mark discovered) failure propagates
// ---------------------------------------------------------------------------

func TestSyncSchema_MarkDiscoveredUpsertFails_ReturnsError(t *testing.T) {
	t.Parallel()

	connUpsertErr := errors.New("connection pool exhausted")

	connRepo := &stubConnectionRepo{
		upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
			return connUpsertErr
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})
	conn := makeExistingConnection("fc-1")

	schema := &sharedPorts.FetcherSchema{
		Tables: []sharedPorts.FetcherTableSchema{
			{TableName: "t1", Columns: []sharedPorts.FetcherColumnInfo{{Name: "id", Type: "int"}}},
		},
	}

	err := cs.SyncSchema(context.Background(), conn, schema)

	require.Error(t, err)
	assert.ErrorIs(t, err, connUpsertErr)
	assert.Contains(t, err.Error(), "update connection schema flag")
	// Even though the upsert failed, MarkSchemaDiscovered was already called on the entity.
	assert.True(t, conn.SchemaDiscovered)
}

func TestSyncSchema_EmptyTables_DeletesPersistedSchemasAndRefreshesEmptyCache(t *testing.T) {
	t.Parallel()

	conn := makeExistingConnection("fc-empty")

	deleteCalled := false
	upsertCalled := false
	var cachedSchema *sharedPorts.FetcherSchema

	connRepo := &stubConnectionRepo{upsertFn: func(_ context.Context, _ *entities.FetcherConnection) error {
		upsertCalled = true
		return nil
	}}
	schemaRepo := &stubSchemaRepo{deleteByConnectionIDFn: func(_ context.Context, connectionID uuid.UUID) error {
		deleteCalled = true
		assert.Equal(t, conn.ID, connectionID)
		return nil
	}}

	cs := mustNewSyncer(t, connRepo, schemaRepo)
	cs.WithSchemaCache(&stubSchemaCache{setSchemaFn: func(_ context.Context, _ string, schema *sharedPorts.FetcherSchema, _ time.Duration) error {
		cachedSchema = schema
		return nil
	}}, time.Minute)

	err := cs.SyncSchema(context.Background(), conn, &sharedPorts.FetcherSchema{
		ConnectionID: "fc-empty",
		Tables:       []sharedPorts.FetcherTableSchema{},
	})

	require.NoError(t, err)
	assert.True(t, deleteCalled)
	assert.True(t, upsertCalled)
	require.NotNil(t, cachedSchema)
	assert.Empty(t, cachedSchema.Tables)
	assert.True(t, conn.SchemaDiscovered)
}

// ---------------------------------------------------------------------------
// SyncSchema — table with no columns
// ---------------------------------------------------------------------------

func TestSyncSchema_TableWithNoColumns_CreatesSchemaWithEmptyColumns(t *testing.T) {
	t.Parallel()

	var persistedSchemas []*entities.DiscoveredSchema

	schemaRepo := &stubSchemaRepo{
		upsertBatchFn: func(_ context.Context, schemas []*entities.DiscoveredSchema) error {
			persistedSchemas = schemas

			return nil
		},
	}

	cs := mustNewSyncer(t, &stubConnectionRepo{}, schemaRepo)
	conn := makeExistingConnection("fc-1")

	schema := &sharedPorts.FetcherSchema{
		Tables: []sharedPorts.FetcherTableSchema{
			{TableName: "empty_table", Columns: nil},
		},
	}

	err := cs.SyncSchema(context.Background(), conn, schema)

	require.NoError(t, err)
	require.Len(t, persistedSchemas, 1)
	assert.Equal(t, "empty_table", persistedSchemas[0].TableName)
	// NewDiscoveredSchema normalizes nil columns to empty slice.
	assert.NotNil(t, persistedSchemas[0].Columns)
	assert.Empty(t, persistedSchemas[0].Columns)
}

// ---------------------------------------------------------------------------
// Sentinel errors are distinct
// ---------------------------------------------------------------------------

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{ErrNilSyncer, ErrNilConnection, ErrNilFetcher}

	seen := make(map[string]bool)
	for _, e := range errs {
		msg := e.Error()
		assert.False(t, seen[msg], "duplicate sentinel error message: %s", msg)

		seen[msg] = true
	}
}

// ---------------------------------------------------------------------------
// SyncConnection — existing connection updates metadata from FetcherConnection
// ---------------------------------------------------------------------------

func TestSyncConnection_ExistingConnection_UpdatesHostPortDbProduct(t *testing.T) {
	t.Parallel()

	existing := makeExistingConnection("fc-metadata")

	var upsertedConn *entities.FetcherConnection

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return existing, nil
		},
		upsertFn: func(_ context.Context, conn *entities.FetcherConnection) error {
			// Capture the first upsert (upsertConnection).
			if upsertedConn == nil {
				upsertedConn = conn
			}

			return nil
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})

	fc := &sharedPorts.FetcherConnection{
		ID:           "fc-metadata",
		ConfigName:   "updated-config",
		DatabaseType: "POSTGRESQL",
		Host:         "new-host.example.com",
		Port:         5433,
		DatabaseName: "new_db",
		ProductName:  "PostgreSQL 18.0",
		Status:       "AVAILABLE",
	}

	// Fetch schema returns nil to exercise the best-effort path.
	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		fc,
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil
		},
	)

	require.NoError(t, err)
	require.NotNil(t, upsertedConn)
	assert.Equal(t, "new-host.example.com", upsertedConn.Host)
	assert.Equal(t, 5433, upsertedConn.Port)
	assert.Equal(t, "new_db", upsertedConn.DatabaseName)
	assert.Equal(t, "PostgreSQL 18.0", upsertedConn.ProductName)
}

// ---------------------------------------------------------------------------
// SyncConnection — schema fetch returns nil schema → still succeeds
// ---------------------------------------------------------------------------

func TestSyncConnection_SchemaFetchReturnsNilSchema_Succeeds(t *testing.T) {
	t.Parallel()

	existing := makeExistingConnection("fc-nil-schema")

	connRepo := &stubConnectionRepo{
		findByFetcherIDFn: func(_ context.Context, _ string) (*entities.FetcherConnection, error) {
			return existing, nil
		},
	}

	cs := mustNewSyncer(t, connRepo, &stubSchemaRepo{})

	err := cs.SyncConnection(
		context.Background(),
		&stubLogger{},
		makeFetcherConnection("fc-nil-schema", "c", "PG"),
		func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, nil // nil schema, no error
		},
	)

	require.NoError(t, err)
	// SyncSchema receives nil schema → early return, so SchemaDiscovered stays false.
	assert.False(t, existing.SchemaDiscovered)
}
