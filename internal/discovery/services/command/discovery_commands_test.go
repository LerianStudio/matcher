//go:build unit

package command

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/discovery/services/syncer"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestRefreshDiscovery_FetcherUnhealthy(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: false},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	assert.Equal(t, 0, synced)
	require.ErrorIs(t, err, ErrFetcherUnavailable)
}

func TestRefreshDiscovery_ListConnectionsError(t *testing.T) {
	t.Parallel()

	listErr := errors.New("fetcher list error")
	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, listErr: listErr},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	assert.Equal(t, 0, synced)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list fetcher connections")
}

func TestRefreshDiscovery_PassesMatcherAsProductName(t *testing.T) {
	t.Parallel()

	fetcherClient := &mockFetcherClient{healthy: true, connections: []*sharedPorts.FetcherConnection{}}
	uc, err := NewUseCase(
		fetcherClient,
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	// Even with tenant context present, ListConnections receives the product name "matcher",
	// not the tenant ID. Tenant filtering is done server-side via JWT.
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "22222222-2222-2222-2222-222222222222")

	_, err = uc.RefreshDiscovery(ctx)

	require.NoError(t, err)
	assert.Equal(t, "matcher", fetcherClient.lastListOrgID)
}

func TestRefreshDiscovery_RejectsConcurrentManualRefresh(t *testing.T) {
	t.Parallel()

	redisServer := miniredis.RunT(t)
	defer redisServer.Close()

	redisClient := goredis.NewClient(&goredis.Options{Addr: redisServer.Addr()})
	defer redisClient.Close()

	provider := &testutil.MockInfrastructureProvider{RedisConn: testutil.NewRedisClientWithMock(redisClient)}
	fetcherClient := &mockFetcherClient{healthy: true, connections: []*sharedPorts.FetcherConnection{}}
	uc, err := NewUseCase(
		fetcherClient,
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)
	uc.WithDiscoveryRefreshLock(provider, time.Minute)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "22222222-2222-2222-2222-222222222222")
	locked, err := redisClient.SetNX(ctx, discoveryRefreshLockKey, "other-token", 2*time.Minute).Result()
	require.NoError(t, err)
	require.True(t, locked)

	_, err = uc.RefreshDiscovery(ctx)

	require.ErrorIs(t, err, ErrDiscoveryRefreshInProgress)
	assert.Empty(t, fetcherClient.lastListOrgID)
}

func TestRefreshDiscovery_NoConnections(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy:     true,
			connections: []*sharedPorts.FetcherConnection{},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, synced)
}

func TestRefreshDiscovery_SuccessWithConnections(t *testing.T) {
	t.Parallel()

	connRepo := &mockConnectionRepo{}
	schemaRepo := &mockSchemaRepo{}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			connections: []*sharedPorts.FetcherConnection{
				{
					ID:           "conn-1",
					ConfigName:   "pg-primary",
					DatabaseType: "POSTGRES",
					Host:         "localhost",
					Port:         5432,
					DatabaseName: "mydb",
				},
				{
					ID:           "conn-2",
					ConfigName:   "mysql-primary",
					DatabaseType: "MYSQL",
					Host:         "localhost",
					Port:         3306,
					DatabaseName: "orders",
				},
			},
			schema: &sharedPorts.FetcherSchema{
				Tables: []sharedPorts.FetcherTableSchema{
					{
						Name:   "transactions",
						Fields: []string{"id", "amount"},
					},
				},
			},
		},
		connRepo,
		schemaRepo,
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 2, synced)
	// Each connection is upserted once for initial save, then once more after schema discovery.
	assert.Equal(t, 4, connRepo.upsertCount)
	assert.Equal(t, 2, schemaRepo.upsertCount)
}

func TestRefreshDiscovery_SyncedConnectionIsMarkedAvailable(t *testing.T) {
	t.Parallel()

	var capturedStatus vo.ConnectionStatus

	connRepo := &mockConnectionRepo{
		findByFetcherErr: repositories.ErrConnectionNotFound,
		upsertFn: func(_ context.Context, conn *entities.FetcherConnection) error {
			capturedStatus = conn.Status

			return nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			connections: []*sharedPorts.FetcherConnection{{
				ID:           "conn-1",
				ConfigName:   "pg-primary",
				DatabaseType: "POSTGRES",
			}},
			schemaErr: errors.New("schema fetch failed"),
		},
		connRepo,
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	// The syncer always calls MarkAvailable on upserted connections.
	assert.Equal(t, vo.ConnectionStatusAvailable, capturedStatus)
}

func TestRefreshDiscovery_ConnectionUpsertError_ContinuesToNext(t *testing.T) {
	t.Parallel()

	connRepo := &mockConnectionRepo{upsertErr: errors.New("upsert failed")}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			connections: []*sharedPorts.FetcherConnection{
				{
					ID:           "conn-1",
					ConfigName:   "pg-primary",
					DatabaseType: "POSTGRES",
				},
				{
					ID:           "conn-2",
					ConfigName:   "mysql-primary",
					DatabaseType: "MYSQL",
				},
			},
		},
		connRepo,
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	// Both should fail to upsert, but no fatal error; synced should be 0.
	assert.Equal(t, 0, synced)
}

func TestRefreshDiscovery_SchemaDiscoveryError_ReturnsError(t *testing.T) {
	t.Parallel()

	connRepo := &mockConnectionRepo{}
	schemaRepo := &mockSchemaRepo{}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			connections: []*sharedPorts.FetcherConnection{
				{
					ID:           "conn-1",
					ConfigName:   "pg-primary",
					DatabaseType: "POSTGRES",
				},
			},
			schemaErr: errors.New("schema fetch failed"),
		},
		connRepo,
		schemaRepo,
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, synced)
	assert.Equal(t, 1, connRepo.upsertCount)
	assert.Equal(t, 0, schemaRepo.upsertCount)
}

func TestRefreshDiscovery_EmptySchemaTables_Skipped(t *testing.T) {
	t.Parallel()

	connRepo := &mockConnectionRepo{}
	schemaRepo := &mockSchemaRepo{}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			connections: []*sharedPorts.FetcherConnection{
				{
					ID:           "conn-1",
					ConfigName:   "pg-primary",
					DatabaseType: "POSTGRES",
				},
			},
			schema: &sharedPorts.FetcherSchema{
				Tables: []sharedPorts.FetcherTableSchema{},
			},
		},
		connRepo,
		schemaRepo,
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, synced)
	// Connection saved once for upsert and again after replacing the persisted
	// schema snapshot with an empty schema set.
	assert.Equal(t, 2, connRepo.upsertCount)
	assert.Equal(t, 0, schemaRepo.upsertCount)
	assert.Equal(t, 1, schemaRepo.deleteCount)
}

func TestRefreshDiscovery_SchemaUpsertBatchError(t *testing.T) {
	t.Parallel()

	connRepo := &mockConnectionRepo{}
	schemaRepo := &mockSchemaRepo{upsertBatchErr: errors.New("batch insert failed")}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			connections: []*sharedPorts.FetcherConnection{
				{
					ID:           "conn-1",
					ConfigName:   "pg-primary",
					DatabaseType: "POSTGRES",
				},
			},
			schema: &sharedPorts.FetcherSchema{
				Tables: []sharedPorts.FetcherTableSchema{
					{
						Name:   "users",
						Fields: []string{"id"},
					},
				},
			},
		},
		connRepo,
		schemaRepo,
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, synced)
}

func TestRefreshDiscovery_MarksStaleConnectionsUnreachable(t *testing.T) {
	t.Parallel()

	staleConn := &entities.FetcherConnection{
		ID:               uuid.New(),
		FetcherConnID:    "conn-stale",
		Status:           vo.ConnectionStatusAvailable,
		SchemaDiscovered: true,
	}

	var staleMarked bool
	connRepo := &mockConnectionRepo{
		findAllConns: []*entities.FetcherConnection{staleConn},
		upsertFn: func(_ context.Context, conn *entities.FetcherConnection) error {
			if conn.ID == staleConn.ID && conn.Status == vo.ConnectionStatusUnreachable {
				staleMarked = true
			}

			return nil
		},
	}
	schemaRepo := &mockSchemaRepo{}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			connections: []*sharedPorts.FetcherConnection{{
				ID:           "conn-live",
				ConfigName:   "pg-primary",
				DatabaseType: "POSTGRES",
			}},
			schema: &sharedPorts.FetcherSchema{Tables: []sharedPorts.FetcherTableSchema{{
				Name:   "transactions",
				Fields: []string{"id"},
			}}},
		},
		connRepo,
		schemaRepo,
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	synced, err := uc.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, synced)
	assert.True(t, staleMarked)
	assert.Equal(t, vo.ConnectionStatusUnreachable, staleConn.Status)
	assert.False(t, staleConn.SchemaDiscovered)
	assert.Equal(t, 1, schemaRepo.deleteCount)
}

func TestSyncSchema_NilSchema(t *testing.T) {
	t.Parallel()

	cs, _ := syncer.NewConnectionSyncer(&mockConnectionRepo{}, &mockSchemaRepo{})
	conn, _ := newTestConnection(t)

	err := cs.SyncSchema(context.Background(), conn, nil)

	require.NoError(t, err)
}

func TestTestConnection_Success(t *testing.T) {
	t.Parallel()

	conn, err := newTestConnection(t)
	require.NoError(t, err)

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			testResult: &sharedPorts.FetcherTestResult{
				Status:    "success",
				LatencyMs: 25,
			},
		},
		&mockConnectionRepo{findByIDConn: conn},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.TestConnection(context.Background(), conn.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, conn.ID, result.ConnectionID)
	assert.True(t, result.Healthy)
	assert.Equal(t, int64(25), result.LatencyMs)
}

func TestTestConnection_NilConnection_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.TestConnection(context.Background(), uuid.New())

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestTestConnection_NilFetcherResult_ReturnsError(t *testing.T) {
	t.Parallel()

	conn, err := newTestConnection(t)
	require.NoError(t, err)

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: conn},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.TestConnection(context.Background(), conn.ID)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrNilTestConnectionResult)
}

// newTestConnection is a test helper that creates a valid FetcherConnection entity.
func newTestConnection(t *testing.T) (*entities.FetcherConnection, error) {
	t.Helper()

	return entities.NewFetcherConnection(context.Background(), "test-conn-id", "test-config", "POSTGRES")
}
