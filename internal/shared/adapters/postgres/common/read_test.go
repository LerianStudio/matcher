//go:build unit

package common

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestQueryExecutorInterface(t *testing.T) {
	t.Parallel()

	// Verify QueryExecutor interface is defined with expected methods
	var _ QueryExecutor = (*mockQueryExecutor)(nil)
}

type mockQueryExecutor struct{}

func (m *mockQueryExecutor) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockQueryExecutor) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

func (m *mockQueryExecutor) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return &mockResult{}, nil
}

type mockResult struct{}

func (m *mockResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (m *mockResult) RowsAffected() (int64, error) {
	return 0, nil
}

func TestWithTenantRead_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantRead[string](context.Background(), nil, func(_ *sql.Conn) (string, error) {
		return "", nil
	})

	require.ErrorIs(t, err, ErrConnectionRequired)
}

func TestWithTenantReadQuery_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantReadQuery[string](
		context.Background(),
		nil,
		func(_ QueryExecutor) (string, error) {
			return "", nil
		},
	)

	require.ErrorIs(t, err, ErrConnectionRequired)
}

// readContextWithDefaultTenant creates a context with the default tenant ID for testing.
func readContextWithDefaultTenant() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
}

// setupReadMockConnection creates a mock PostgresConnection for testing read operations.
func setupReadMockConnection(
	t *testing.T,
) (*libPostgres.Client, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)

	return provider.PostgresConn, mock, db
}

func TestWithTenantRead_ReplicaDBError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("replica db error")
	provider := &testutil.MockInfrastructureProvider{
		ReplicaDBErr: expectedErr,
	}

	ctx := readContextWithDefaultTenant()

	_, err := WithTenantRead[string](ctx, provider, func(_ *sql.Conn) (string, error) {
		return "result", nil
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "get replica db")
}

func TestWithTenantRead_FallbackToPrimary_Success(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupReadMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
		ReplicaDB:    nil,
	}

	ctx := readContextWithDefaultTenant()

	mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := WithTenantRead[string](ctx, provider, func(_ *sql.Conn) (string, error) {
		return "fallback success", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "fallback success", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantRead_FallbackToPrimary_NilConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: nil,
		ReplicaDB:    nil,
	}

	ctx := readContextWithDefaultTenant()

	_, err := WithTenantRead[string](ctx, provider, func(_ *sql.Conn) (string, error) {
		return "result", nil
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "get replica db")
}

func TestWithTenantRead_FallbackToPrimary_PostgresConnError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("postgres connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: expectedErr,
		ReplicaDB:   nil,
	}

	ctx := readContextWithDefaultTenant()

	_, err := WithTenantRead[string](ctx, provider, func(_ *sql.Conn) (string, error) {
		return "result", nil
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "get replica db")
}

func TestWithTenantRead_WithReplicaDB(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		ReplicaDB: db,
	}

	ctx := readContextWithDefaultTenant()

	mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := WithTenantRead[string](ctx, provider, func(_ *sql.Conn) (string, error) {
		return "replica read", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "replica read", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantRead_FnReturnsError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		ReplicaDB: db,
	}

	ctx := readContextWithDefaultTenant()
	expectedErr := errors.New("function error")

	mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err = WithTenantRead[string](ctx, provider, func(_ *sql.Conn) (string, error) {
		return "", expectedErr
	})

	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantReadQuery_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		ReplicaDB: db,
	}

	ctx := readContextWithDefaultTenant()

	mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := WithTenantReadQuery[int](ctx, provider, func(_ QueryExecutor) (int, error) {
		return 42, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantReadQuery_FnReturnsError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		ReplicaDB: db,
	}

	ctx := readContextWithDefaultTenant()
	expectedErr := errors.New("query function error")

	mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err = WithTenantReadQuery[int](ctx, provider, func(_ QueryExecutor) (int, error) {
		return 0, expectedErr
	})

	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantRead_GenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("returns struct type", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		provider := &testutil.MockInfrastructureProvider{
			ReplicaDB: db,
		}

		ctx := readContextWithDefaultTenant()

		mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

		type Data struct {
			ID   int
			Name string
		}

		result, err := WithTenantRead[Data](ctx, provider, func(_ *sql.Conn) (Data, error) {
			return Data{ID: 1, Name: "test"}, nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, result.ID)
		assert.Equal(t, "test", result.Name)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns slice type", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		provider := &testutil.MockInfrastructureProvider{
			ReplicaDB: db,
		}

		ctx := readContextWithDefaultTenant()

		mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

		result, err := WithTenantRead[[]string](ctx, provider, func(_ *sql.Conn) ([]string, error) {
			return []string{"a", "b", "c"}, nil
		})

		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns pointer type", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		provider := &testutil.MockInfrastructureProvider{
			ReplicaDB: db,
		}

		ctx := readContextWithDefaultTenant()

		mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

		type Entity struct{ Value int }

		result, err := WithTenantRead[*Entity](ctx, provider, func(_ *sql.Conn) (*Entity, error) {
			return &Entity{Value: 42}, nil
		})

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 42, result.Value)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestWithTenantReadQuery_GenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("returns map type", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		provider := &testutil.MockInfrastructureProvider{
			ReplicaDB: db,
		}

		ctx := readContextWithDefaultTenant()

		mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

		result, err := WithTenantReadQuery[map[string]int](
			ctx,
			provider,
			func(_ QueryExecutor) (map[string]int, error) {
				return map[string]int{"key": 123}, nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, 123, result["key"])
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestWithTenantRead_NilProviderReturnsZeroValue(t *testing.T) {
	t.Parallel()

	t.Run("returns zero for int", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantRead[int](
			context.Background(),
			nil,
			func(_ *sql.Conn) (int, error) {
				return 42, nil
			},
		)

		require.Error(t, err)
		assert.Zero(t, result)
	})

	t.Run("returns nil for pointer", func(t *testing.T) {
		t.Parallel()

		type Entity struct{ ID string }

		result, err := WithTenantRead[*Entity](
			context.Background(),
			nil,
			func(_ *sql.Conn) (*Entity, error) {
				return &Entity{ID: "123"}, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns nil for slice", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantRead[[]string](
			context.Background(),
			nil,
			func(_ *sql.Conn) ([]string, error) {
				return []string{"a"}, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestQueryExecutorInterface_SqlTxSatisfies(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := db.Begin()
	require.NoError(t, err)

	var executor QueryExecutor = tx

	assert.NotNil(t, executor)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryExecutorInterface_SqlConnSatisfies(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	var executor QueryExecutor = conn

	assert.NotNil(t, executor)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetReadDB_ReturnsReplicaWhenAvailable(t *testing.T) {
	t.Parallel()

	replicaDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB.Close()

	provider := &testutil.MockInfrastructureProvider{
		ReplicaDB: replicaDB,
	}

	ctx := readContextWithDefaultTenant()

	lease, err := getReadDB(ctx, provider)

	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, replicaDB, lease.DB())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetReadDB_FallsToPrimaryWhenReplicaNil(t *testing.T) {
	t.Parallel()

	conn, mock, primaryDB := setupReadMockConnection(t)
	defer primaryDB.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
		ReplicaDB:    nil,
	}

	ctx := readContextWithDefaultTenant()

	lease, err := getReadDB(ctx, provider)

	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, primaryDB, lease.DB())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetReadDB_ReplicaDBError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("replica connection failed")
	provider := &testutil.MockInfrastructureProvider{
		ReplicaDBErr: expectedErr,
	}

	ctx := readContextWithDefaultTenant()

	db, err := getReadDB(ctx, provider)

	require.Error(t, err)
	require.ErrorContains(t, err, "get replica db")
	assert.Nil(t, db)
}

func TestGetPrimaryDBFallback_Success(t *testing.T) {
	t.Parallel()

	conn, mock, primaryDB := setupReadMockConnection(t)
	defer primaryDB.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := readContextWithDefaultTenant()

	lease, err := getPrimaryDBFallback(ctx, provider)

	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, primaryDB, lease.DB())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPrimaryDBFallback_NilConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: nil,
	}

	ctx := readContextWithDefaultTenant()

	db, err := getPrimaryDBFallback(ctx, provider)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrConnectionRequired)
	assert.Nil(t, db)
}

func TestGetPrimaryDBFallback_GetPostgresConnectionError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("postgres connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: expectedErr,
	}

	ctx := readContextWithDefaultTenant()

	db, err := getPrimaryDBFallback(ctx, provider)

	require.Error(t, err)
	require.ErrorContains(t, err, "get primary connection as fallback")
	assert.Nil(t, db)
}

func TestApplyTenantSchemaToConn_InvalidUUID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")

	err = applyTenantSchemaToConn(ctx, conn)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidTenantID)
	require.ErrorContains(t, err, "not-a-uuid")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyTenantSchemaToConn_ValidUUID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	validUUID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, validUUID)

	mock.ExpectExec("SET search_path TO").WillReturnResult(sqlmock.NewResult(0, 0))

	err = applyTenantSchemaToConn(ctx, conn)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyTenantSchemaToConn_EmptyTenantID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "")

	err = applyTenantSchemaToConn(ctx, conn)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyTenantSchemaToConn_DefaultTenantID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	ctx := readContextWithDefaultTenant()

	err = applyTenantSchemaToConn(ctx, conn)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantRead_InvalidTenantID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		ReplicaDB: db,
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	// resetSearchPath is called in defer, but since the tenant is invalid
	// it won't have been applied, so the conn is clean.
	mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err = WithTenantRead[string](ctx, provider, func(_ *sql.Conn) (string, error) {
		return "should not reach", nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidTenantID)
	require.ErrorContains(t, err, "apply tenant schema")
}

func TestResetSearchPath_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	mock.ExpectExec("SET search_path TO public").WillReturnResult(sqlmock.NewResult(0, 0))

	// Should not panic or close the connection on success
	resetSearchPath(context.Background(), conn)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResetSearchPath_FailureClosesConnection(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	mock.ExpectExec("SET search_path TO public").WillReturnError(errors.New("connection lost"))

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "550e8400-e29b-41d4-a716-446655440000")

	// Should log warning and close the connection without panicking.
	// conn.Close() returns the connection to the pool (not a DB close),
	// so we verify it doesn't panic and that the error expectation was met.
	resetSearchPath(ctx, conn)

	require.NoError(t, mock.ExpectationsWereMet())
}
