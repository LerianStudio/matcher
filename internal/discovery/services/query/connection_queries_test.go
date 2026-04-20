//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type mockSchemaCache struct {
	getSchemaFn func(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error)
	setSchemaFn func(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error
}

var _ ports.SchemaCache = (*mockSchemaCache)(nil)

func (m *mockSchemaCache) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if m.getSchemaFn != nil {
		return m.getSchemaFn(ctx, connectionID)
	}

	return nil, nil
}

func (m *mockSchemaCache) SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
	if m.setSchemaFn != nil {
		return m.setSchemaFn(ctx, connectionID, schema, ttl)
	}

	return nil
}

func (m *mockSchemaCache) InvalidateSchema(_ context.Context, _ string) error {
	return nil
}

func readySchemaConnection(id uuid.UUID) *entities.FetcherConnection {
	return &entities.FetcherConnection{
		ID:               id,
		Status:           vo.ConnectionStatusAvailable,
		SchemaDiscovered: true,
	}
}

func TestGetDiscoveryStatus_FetcherHealthy(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	earlier := now.Add(-1 * time.Hour)

	connRepo := &mockConnectionRepo{
		findAllConns: []*entities.FetcherConnection{
			{
				ID:         uuid.New(),
				LastSeenAt: earlier,
			},
			{
				ID:         uuid.New(),
				LastSeenAt: now,
			},
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		connRepo,
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	status, err := uc.GetDiscoveryStatus(context.Background())

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.True(t, status.FetcherHealthy)
	assert.Equal(t, 2, status.ConnectionCount)
	assert.Equal(t, now, status.LastSyncAt)
}

func TestGetDiscoveryStatus_FetcherUnhealthy(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: false},
		&mockConnectionRepo{findAllConns: []*entities.FetcherConnection{}},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	status, err := uc.GetDiscoveryStatus(context.Background())

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.False(t, status.FetcherHealthy)
	assert.Equal(t, 0, status.ConnectionCount)
}

func TestGetDiscoveryStatus_FindAllError(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findAllErr: errors.New("db error")},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	status, err := uc.GetDiscoveryStatus(context.Background())

	assert.Nil(t, status)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list connections")
}

func TestGetDiscoveryStatus_IgnoresNilConnectionEntries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findAllConns: []*entities.FetcherConnection{
			nil,
			{ID: uuid.New(), LastSeenAt: now},
		}},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	status, err := uc.GetDiscoveryStatus(context.Background())

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, 2, status.ConnectionCount)
	assert.Equal(t, now, status.LastSyncAt)
}

func TestListConnections_Success(t *testing.T) {
	t.Parallel()

	expected := []*entities.FetcherConnection{
		{
			ID:            uuid.New(),
			FetcherConnID: "conn-1",
			ConfigName:    "pg-primary",
			Status:        vo.ConnectionStatusAvailable,
		},
		{
			ID:            uuid.New(),
			FetcherConnID: "conn-2",
			ConfigName:    "mysql-read",
			Status:        vo.ConnectionStatusUnreachable,
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findAllConns: expected},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	conns, err := uc.ListConnections(context.Background())

	require.NoError(t, err)
	assert.Len(t, conns, 2)
	assert.Equal(t, expected, conns)
}

func TestListConnections_Error(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findAllErr: errors.New("connection error")},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	conns, err := uc.ListConnections(context.Background())

	assert.Nil(t, conns)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list connections")
}

func TestGetConnection_Success(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	expected := &entities.FetcherConnection{
		ID:            connID,
		FetcherConnID: "conn-abc",
		ConfigName:    "test-pg",
		DatabaseType:  "POSTGRES",
		Status:        vo.ConnectionStatusAvailable,
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: expected},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	conn, err := uc.GetConnection(context.Background(), connID)

	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, expected, conn)
}

func TestGetConnection_Error(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDErr: repositories.ErrConnectionNotFound},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	conn, err := uc.GetConnection(context.Background(), uuid.New())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestGetConnection_NilResult_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	conn, err := uc.GetConnection(context.Background(), uuid.New())

	assert.Nil(t, conn)
	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestGetConnectionSchema_Success(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	expected := []*entities.DiscoveredSchema{
		{
			ID:           uuid.New(),
			ConnectionID: connID,
			TableName:    "transactions",
			Columns: []entities.ColumnInfo{
				{Name: "id", Type: "uuid", Nullable: false},
				{Name: "amount", Type: "numeric", Nullable: false},
			},
		},
		{
			ID:           uuid.New(),
			ConnectionID: connID,
			TableName:    "accounts",
			Columns: []entities.ColumnInfo{
				{Name: "id", Type: "uuid", Nullable: false},
				{Name: "name", Type: "varchar", Nullable: true},
			},
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: readySchemaConnection(connID)},
		&mockSchemaRepo{findByConnID: expected},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	schemas, err := uc.GetConnectionSchema(context.Background(), connID)

	require.NoError(t, err)
	assert.Len(t, schemas, 2)
	assert.Equal(t, expected, schemas)
}

func TestGetConnectionSchema_Error(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: readySchemaConnection(connID)},
		&mockSchemaRepo{findByConnErr: errors.New("schema query failed")},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	schemas, err := uc.GetConnectionSchema(context.Background(), connID)

	assert.Nil(t, schemas)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get connection schema")
}

func TestGetConnectionSchema_EmptyResult(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: readySchemaConnection(connID)},
		&mockSchemaRepo{findByConnID: []*entities.DiscoveredSchema{}},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	schemas, err := uc.GetConnectionSchema(context.Background(), connID)

	require.NoError(t, err)
	assert.Empty(t, schemas)
}

func TestGetConnectionSchema_FiltersNilEntries(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	valid := &entities.DiscoveredSchema{
		ID:           uuid.New(),
		ConnectionID: connID,
		TableName:    "transactions",
		Columns: []entities.ColumnInfo{
			{Name: "id", Type: "uuid", Nullable: false},
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: readySchemaConnection(connID)},
		&mockSchemaRepo{findByConnID: []*entities.DiscoveredSchema{nil, valid}},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	schemas, err := uc.GetConnectionSchema(context.Background(), connID)

	require.NoError(t, err)
	require.Len(t, schemas, 1)
	assert.Equal(t, valid, schemas[0])
}

func TestGetConnectionSchema_UnavailableConnectionReturnsEmptyWithoutUsingCache(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	cacheCalled := false

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: &entities.FetcherConnection{ID: connID, Status: vo.ConnectionStatusUnreachable}},
		&mockSchemaRepo{findByConnErr: errors.New("schema repo should not be called")},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	uc.WithSchemaCache(&mockSchemaCache{
		getSchemaFn: func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			cacheCalled = true
			return &sharedPorts.FetcherSchema{}, nil
		},
	}, time.Minute)

	schemas, err := uc.GetConnectionSchema(context.Background(), connID)

	require.NoError(t, err)
	assert.False(t, cacheCalled)
	assert.Empty(t, schemas)
}

func TestGetConnectionSchema_UsesCacheWhenAvailable(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	discoveredAt := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: readySchemaConnection(connID)},
		&mockSchemaRepo{findByConnErr: errors.New("db should not be called")},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	uc.WithSchemaCache(&mockSchemaCache{
		getSchemaFn: func(_ context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
			assert.Equal(t, connID.String(), connectionID)

			return &sharedPorts.FetcherSchema{Tables: []sharedPorts.FetcherTableSchema{{
				Name:   "transactions",
				Fields: []string{"id"},
			}}, DiscoveredAt: discoveredAt}, nil
		},
	}, time.Minute)

	first, err := uc.GetConnectionSchema(context.Background(), connID)

	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, "transactions", first[0].TableName)
	assert.Equal(t, discoveredAt, first[0].DiscoveredAt)

	second, err := uc.GetConnectionSchema(context.Background(), connID)
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Equal(t, first[0].ID, second[0].ID)
	assert.Equal(t, first[0].DiscoveredAt, second[0].DiscoveredAt)
}

func TestGetConnectionSchema_FallsBackToRepositoryOnCacheError(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	expected := []*entities.DiscoveredSchema{{
		ID:           uuid.New(),
		ConnectionID: connID,
		TableName:    "accounts",
	}}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: readySchemaConnection(connID)},
		&mockSchemaRepo{findByConnID: expected},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	uc.WithSchemaCache(&mockSchemaCache{
		getSchemaFn: func(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
			return nil, errors.New("cache unavailable")
		},
	}, time.Minute)

	schemas, err := uc.GetConnectionSchema(context.Background(), connID)

	require.NoError(t, err)
	assert.Equal(t, expected, schemas)
}

func TestCacheSchemas_PreservesLatestDiscoveredAt(t *testing.T) {
	t.Parallel()

	connID := uuid.New()
	firstSeen := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	secondSeen := firstSeen.Add(2 * time.Hour)

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	var cached *sharedPorts.FetcherSchema
	uc.WithSchemaCache(&mockSchemaCache{
		setSchemaFn: func(_ context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
			assert.Equal(t, connID.String(), connectionID)
			assert.Equal(t, time.Minute, ttl)
			cached = schema
			return nil
		},
	}, time.Minute)

	uc.cacheSchemas(context.Background(), connID, []*entities.DiscoveredSchema{
		{ID: uuid.New(), ConnectionID: connID, TableName: "accounts", DiscoveredAt: firstSeen},
		{ID: uuid.New(), ConnectionID: connID, TableName: "transactions", DiscoveredAt: secondSeen},
	})

	require.NotNil(t, cached)
	assert.Equal(t, secondSeen, cached.DiscoveredAt)
}
