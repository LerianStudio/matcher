//go:build unit

package adapters

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

type tenantContextKey struct{}

var errTestGetDB = errors.New("test get db error")

func TestNewSingleTenantInfrastructureProvider(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}
	redis := testutil.NewRedisClientWithMock(nil)

	provider := NewSingleTenantInfrastructureProvider(postgres, redis)

	require.NotNil(t, provider)
	assert.Same(t, postgres, provider.postgres)
	assert.Same(t, redis, provider.redis)
}

func TestGetPostgresConnection_ReturnsStored(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}

	provider := NewSingleTenantInfrastructureProvider(postgres, nil)

	result, err := provider.GetPostgresConnection(context.Background())

	require.NoError(t, err)
	assert.Same(t, postgres, result)
}

func TestGetRedisConnection_ReturnsStored(t *testing.T) {
	t.Parallel()

	redis := testutil.NewRedisClientWithMock(nil)

	provider := NewSingleTenantInfrastructureProvider(nil, redis)

	result, err := provider.GetRedisConnection(context.Background())

	require.NoError(t, err)
	assert.Same(t, redis, result)
}

func TestGetPostgresConnection_NilConnection(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	result, err := provider.GetPostgresConnection(context.Background())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
	assert.Nil(t, result)
}

func TestGetRedisConnection_NilConnection(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	result, err := provider.GetRedisConnection(context.Background())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRedisConnectionNotConfigured)
	assert.Nil(t, result)
}

func TestSingleTenantProvider_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.InfrastructureProvider = (*SingleTenantInfrastructureProvider)(nil)
}

func TestGetPostgresConnection_IgnoresContext(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}

	provider := NewSingleTenantInfrastructureProvider(postgres, nil)

	ctx1 := context.Background()
	ctx2 := context.WithValue(context.Background(), tenantContextKey{}, "tenant-1")
	ctx3 := context.WithValue(context.Background(), tenantContextKey{}, "tenant-2")

	result1, err1 := provider.GetPostgresConnection(ctx1)
	result2, err2 := provider.GetPostgresConnection(ctx2)
	result3, err3 := provider.GetPostgresConnection(ctx3)

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)

	assert.Same(t, result1, result2)
	assert.Same(t, result2, result3)
}

func TestGetRedisConnection_IgnoresContext(t *testing.T) {
	t.Parallel()

	redis := testutil.NewRedisClientWithMock(nil)

	provider := NewSingleTenantInfrastructureProvider(nil, redis)

	ctx1 := context.Background()
	ctx2 := context.WithValue(context.Background(), tenantContextKey{}, "tenant-1")
	ctx3 := context.WithValue(context.Background(), tenantContextKey{}, "tenant-2")

	result1, err1 := provider.GetRedisConnection(ctx1)
	result2, err2 := provider.GetRedisConnection(ctx2)
	result3, err3 := provider.GetRedisConnection(ctx3)

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)

	assert.Same(t, result1, result2)
	assert.Same(t, result2, result3)
}

func TestBeginTx_NilPostgresConnection(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	tx, err := provider.BeginTx(context.Background())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
	assert.Nil(t, tx)
}

func TestGetReplicaDB_NilPostgresConnection(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	db, err := provider.GetReplicaDB(context.Background())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
	assert.Nil(t, db)
}

func TestGetReplicaDB_ConsistentBehavior(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	ctx1 := context.Background()
	ctx2 := context.WithValue(context.Background(), tenantContextKey{}, "tenant-1")

	_, err1 := provider.GetReplicaDB(ctx1)
	_, err2 := provider.GetReplicaDB(ctx2)

	require.ErrorIs(t, err1, ErrPostgresConnectionNotConfigured)
	require.ErrorIs(t, err2, ErrPostgresConnectionNotConfigured)
}

func TestNewSingleTenantInfrastructureProvider_NilBoth(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	require.NotNil(t, provider)
	assert.Nil(t, provider.postgres)
	assert.Nil(t, provider.redis)
}

func TestNewSingleTenantInfrastructureProvider_OnlyPostgres(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}

	provider := NewSingleTenantInfrastructureProvider(postgres, nil)

	require.NotNil(t, provider)
	assert.Same(t, postgres, provider.postgres)
	assert.Nil(t, provider.redis)
}

func TestNewSingleTenantInfrastructureProvider_OnlyRedis(t *testing.T) {
	t.Parallel()

	redis := testutil.NewRedisClientWithMock(nil)

	provider := NewSingleTenantInfrastructureProvider(nil, redis)

	require.NotNil(t, provider)
	assert.Nil(t, provider.postgres)
	assert.Same(t, redis, provider.redis)
}

func TestGetPostgresConnection_MultipleCallsSameConnection(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}
	provider := NewSingleTenantInfrastructureProvider(postgres, nil)

	result1, err1 := provider.GetPostgresConnection(context.Background())
	result2, err2 := provider.GetPostgresConnection(context.Background())
	result3, err3 := provider.GetPostgresConnection(context.Background())

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)
	assert.Same(t, result1, result2)
	assert.Same(t, result2, result3)
	assert.Same(t, postgres, result1)
}

func TestGetRedisConnection_MultipleCallsSameConnection(t *testing.T) {
	t.Parallel()

	redis := testutil.NewRedisClientWithMock(nil)
	provider := NewSingleTenantInfrastructureProvider(nil, redis)

	result1, err1 := provider.GetRedisConnection(context.Background())
	result2, err2 := provider.GetRedisConnection(context.Background())
	result3, err3 := provider.GetRedisConnection(context.Background())

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)
	assert.Same(t, result1, result2)
	assert.Same(t, result2, result3)
	assert.Same(t, redis, result1)
}

func TestGetPostgresConnection_WithCanceledContext(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}
	provider := NewSingleTenantInfrastructureProvider(postgres, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := provider.GetPostgresConnection(ctx)

	require.NoError(t, err)
	assert.Same(t, postgres, result)
}

func TestGetRedisConnection_WithCanceledContext(t *testing.T) {
	t.Parallel()

	redis := testutil.NewRedisClientWithMock(nil)
	provider := NewSingleTenantInfrastructureProvider(nil, redis)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := provider.GetRedisConnection(ctx)

	require.NoError(t, err)
	assert.Same(t, redis, result)
}

func TestSingleTenantProvider_SentinelErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrPostgresConnectionNotConfigured",
			err:      ErrPostgresConnectionNotConfigured,
			expected: "postgres connection not configured",
		},
		{
			name:     "ErrRedisConnectionNotConfigured",
			err:      ErrRedisConnectionNotConfigured,
			expected: "redis connection not configured",
		},
		{
			name:     "ErrNoPrimaryDatabaseForTransaction",
			err:      ErrNoPrimaryDatabaseForTransaction,
			expected: "no primary database configured for single-tenant transaction",
		},
		{
			name:     "ErrNoDatabaseForRead",
			err:      ErrNoDatabaseForRead,
			expected: "no database configured for read operations",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, tc.err.Error())
		})
	}
}

func TestBeginTx_WithCanceledContext(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tx, err := provider.BeginTx(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
	assert.Nil(t, tx)
}

func TestGetReplicaDB_WithCanceledContext(t *testing.T) {
	t.Parallel()

	provider := NewSingleTenantInfrastructureProvider(nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := provider.GetReplicaDB(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
	assert.Nil(t, db)
}

func TestProviderErrorWrapping(t *testing.T) {
	t.Parallel()

	wrappedPostgres := errors.Join(errors.New("wrapper"), ErrPostgresConnectionNotConfigured)
	wrappedRedis := errors.Join(errors.New("wrapper"), ErrRedisConnectionNotConfigured)
	wrappedTx := errors.Join(errors.New("wrapper"), ErrNoPrimaryDatabaseForTransaction)
	wrappedRead := errors.Join(errors.New("wrapper"), ErrNoDatabaseForRead)

	assert.ErrorIs(t, wrappedPostgres, ErrPostgresConnectionNotConfigured)
	assert.ErrorIs(t, wrappedRedis, ErrRedisConnectionNotConfigured)
	assert.ErrorIs(t, wrappedTx, ErrNoPrimaryDatabaseForTransaction)
	assert.ErrorIs(t, wrappedRead, ErrNoDatabaseForRead)

	assert.False(t, errors.Is(ErrPostgresConnectionNotConfigured, ErrRedisConnectionNotConfigured))
	assert.False(t, errors.Is(ErrNoPrimaryDatabaseForTransaction, ErrNoDatabaseForRead))
}

func TestBeginTx_WithSqlmock_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	mock.ExpectBegin()

	tx, err := provider.BeginTx(context.Background())

	require.NoError(t, err)
	require.NotNil(t, tx)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_WithSqlmock_BeginError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	mock.ExpectBegin().WillReturnError(errTestGetDB)

	tx, err := provider.BeginTx(context.Background())

	require.Error(t, err)
	require.Nil(t, tx)
	assert.Contains(t, err.Error(), "failed to begin transaction")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetReplicaDB_WithSqlmock_ReplicaAvailable(t *testing.T) {
	t.Parallel()

	primaryDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = primaryDB.Close() }()

	replicaDB, replicaMock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = replicaDB.Close() }()

	resolver := dbresolver.New(
		dbresolver.WithPrimaryDBs(primaryDB),
		dbresolver.WithReplicaDBs(replicaDB),
	)
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	result, err := provider.GetReplicaDB(context.Background())

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Same(t, replicaDB, result)

	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock.ExpectationsWereMet())
}

func TestGetReplicaDB_WithSqlmock_FallbackToPrimary(t *testing.T) {
	t.Parallel()

	primaryDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = primaryDB.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(primaryDB))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	result, err := provider.GetReplicaDB(context.Background())

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Same(t, primaryDB, result)

	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestGetReplicaDB_OnlyPrimaryConfigured(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	result, dbErr := provider.GetReplicaDB(context.Background())

	require.NoError(t, dbErr)
	require.NotNil(t, result)
	assert.Same(t, db, result)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_ResolverErrorReturnsError(t *testing.T) {
	t.Parallel()

	// An empty Client without a resolver will fail when Resolver(ctx) is called.
	conn := &libPostgres.Client{}
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	tx, err := provider.BeginTx(context.Background())

	require.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "failed to get database connection")
	assert.ErrorIs(t, err, ErrNoPrimaryDatabaseForTransaction)
}

func TestGetReplicaDB_ResolverErrorReturnsError(t *testing.T) {
	t.Parallel()

	// An empty Client without a resolver will fail when Resolver(ctx) is called.
	conn := &libPostgres.Client{}
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	db, err := provider.GetReplicaDB(context.Background())

	require.Error(t, err)
	assert.Nil(t, db)
	assert.Contains(t, err.Error(), "failed to get database connection")
	assert.ErrorIs(t, err, ErrNoDatabaseForRead)
}

func TestBeginTx_CommitAfterSuccess(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := provider.BeginTx(context.Background())

	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_RollbackAfterSuccess(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := provider.BeginTx(context.Background())

	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Rollback()
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetReplicaDB_MultipleReplicas(t *testing.T) {
	t.Parallel()

	primaryDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = primaryDB.Close() }()

	replica1, replica1Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = replica1.Close() }()

	replica2, replica2Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = replica2.Close() }()

	resolver := dbresolver.New(
		dbresolver.WithPrimaryDBs(primaryDB),
		dbresolver.WithReplicaDBs(replica1, replica2),
	)
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	result, err := provider.GetReplicaDB(context.Background())

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Same(t, replica1, result)

	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replica1Mock.ExpectationsWereMet())
	require.NoError(t, replica2Mock.ExpectationsWereMet())
}

func TestProviderWithBothConnections(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	postgres := testutil.NewClientWithResolver(resolver)
	redis := testutil.NewRedisClientWithMock(nil)

	provider := NewSingleTenantInfrastructureProvider(postgres, redis)

	pgConn, err := provider.GetPostgresConnection(context.Background())
	require.NoError(t, err)
	assert.Same(t, postgres, pgConn)

	redisConn, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	assert.Same(t, redis, redisConn)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_MultiplePrimaryDatabases(t *testing.T) {
	t.Parallel()

	primary1, mock1, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = primary1.Close() }()

	primary2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = primary2.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(primary1, primary2))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	mock1.ExpectBegin()
	mock1.ExpectRollback()

	tx, err := provider.BeginTx(context.Background())

	require.NoError(t, err)
	require.NotNil(t, tx)

	require.NoError(t, tx.Rollback())

	require.NoError(t, mock1.ExpectationsWereMet())
	require.NoError(t, mock2.ExpectationsWereMet())
}

func TestGetReplicaDB_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			result, err := provider.GetReplicaDB(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Same(t, db, result)
		}()
	}

	wg.Wait()

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_SequentialTransactions(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	numTransactions := 3
	for i := 0; i < numTransactions; i++ {
		mock.ExpectBegin()
		mock.ExpectRollback()
	}

	for i := 0; i < numTransactions; i++ {
		tx, txErr := provider.BeginTx(context.Background())
		require.NoError(t, txErr)
		require.NotNil(t, tx)

		err = tx.Rollback()
		require.NoError(t, err)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPostgresConnection_ThreadSafety(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}
	provider := NewSingleTenantInfrastructureProvider(postgres, nil)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			result, err := provider.GetPostgresConnection(context.Background())
			assert.NoError(t, err)
			assert.Same(t, postgres, result)
		}()
	}

	wg.Wait()
}

func TestGetRedisConnection_ThreadSafety(t *testing.T) {
	t.Parallel()

	redis := testutil.NewRedisClientWithMock(nil)
	provider := NewSingleTenantInfrastructureProvider(nil, redis)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			result, err := provider.GetRedisConnection(context.Background())
			assert.NoError(t, err)
			assert.Same(t, redis, result)
		}()
	}

	wg.Wait()
}

func TestBeginTx_WithTenantSchema_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	tenantID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SET LOCAL search_path TO "` + tenantID + `", public`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := provider.BeginTx(ctx)

	require.NoError(t, err)
	require.NotNil(t, tx)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_ApplyTenantSchemaError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	tenantID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SET LOCAL search_path TO "` + tenantID + `", public`)).
		WillReturnError(errors.New("permission denied"))
	mock.ExpectRollback()

	tx, err := provider.BeginTx(ctx)

	require.Error(t, err)
	require.Nil(t, tx)
	assert.Contains(t, err.Error(), "failed to apply tenant schema")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_ApplyTenantSchemaError_RollbackFails(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	tenantID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SET LOCAL search_path TO "` + tenantID + `", public`)).
		WillReturnError(errors.New("schema error"))
	errRollback := errors.New("connection lost")
	mock.ExpectRollback().WillReturnError(errRollback)

	tx, err := provider.BeginTx(ctx)

	require.Error(t, err)
	require.Nil(t, tx)
	assert.Contains(t, err.Error(), "failed to apply tenant schema")
	assert.Contains(t, err.Error(), "rollback transaction")
	assert.ErrorIs(t, err, errRollback)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_InvalidTenantID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := provider.BeginTx(ctx)

	require.Error(t, err)
	require.Nil(t, tx)
	assert.Contains(t, err.Error(), "failed to apply tenant schema")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTx_DefaultTenantID_NoSchemaSet(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := NewSingleTenantInfrastructureProvider(conn, nil)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	mock.ExpectBegin()

	tx, err := provider.BeginTx(ctx)

	require.NoError(t, err)
	require.NotNil(t, tx)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())

	require.NoError(t, mock.ExpectationsWereMet())
}
