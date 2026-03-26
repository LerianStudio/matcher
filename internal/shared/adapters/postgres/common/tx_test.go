//go:build unit

package common

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrConnectionRequired has correct message", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "postgres connection is required", ErrConnectionRequired.Error())
	})

	t.Run("ErrNoPrimaryDB has correct message", func(t *testing.T) {
		t.Parallel()
		assert.Equal(
			t,
			"no primary database configured for tenant transaction",
			ErrNoPrimaryDB.Error(),
		)
	})

	t.Run("ErrNilTxLease has correct message", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "tenant transaction lease is required", ErrNilTxLease.Error())
	})

	t.Run("ErrNilCallback has correct message", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "pgcommon: callback function must not be nil", ErrNilCallback.Error())
	})

	t.Run("errors can be checked with errors.Is", func(t *testing.T) {
		t.Parallel()
		assert.NotErrorIs(t, ErrConnectionRequired, ErrNoPrimaryDB)
		assert.NotErrorIs(t, ErrNilCallback, ErrConnectionRequired)
	})
}

func TestWithTenantTxNilConnection(t *testing.T) {
	t.Parallel()

	result, err := WithTenantTx(context.Background(), nil, func(_ *sql.Tx) (string, error) {
		return "success", nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrConnectionRequired)
	assert.Empty(t, result)
}

func TestWithTenantTxOrExistingNilConnection(t *testing.T) {
	t.Parallel()

	result, err := WithTenantTxOrExisting[int](
		context.Background(),
		nil,
		nil,
		func(_ *sql.Tx) (int, error) {
			return 42, nil
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrConnectionRequired)
	assert.Zero(t, result)
}

func TestWithTenantTxOrExisting_NilCallback(t *testing.T) {
	t.Parallel()

	conn, _, db := setupMockConnection(t)
	defer db.Close()

	result, err := WithTenantTxOrExisting[string](
		context.Background(),
		conn,
		nil,
		nil,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilCallback)
	assert.Empty(t, result)
}

func TestWithTenantTx_NilCallback(t *testing.T) {
	t.Parallel()

	conn, _, db := setupMockConnection(t)
	defer db.Close()

	result, err := WithTenantTx[string](
		context.Background(),
		conn,
		nil,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilCallback)
	assert.Empty(t, result)
}

func TestWithTenantTxProvider_NilCallback(t *testing.T) {
	t.Parallel()

	conn, _, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	result, err := WithTenantTxProvider[string](
		context.Background(),
		provider,
		nil,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilCallback)
	assert.Empty(t, result)
}

func TestWithTenantTxOrExistingProvider_NilCallback(t *testing.T) {
	t.Parallel()

	conn, _, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	result, err := WithTenantTxOrExistingProvider[string](
		context.Background(),
		provider,
		nil,
		nil,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilCallback)
	assert.Empty(t, result)
}

func TestWithTenantTxGenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("returns zero value for string on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTx(
			context.Background(),
			nil,
			func(_ *sql.Tx) (string, error) {
				return "value", nil
			},
		)
		require.Error(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns zero value for int on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTx(context.Background(), nil, func(_ *sql.Tx) (int, error) {
			return 100, nil
		})
		require.Error(t, err)
		assert.Zero(t, result)
	})

	t.Run("returns zero value for slice on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTx(
			context.Background(),
			nil,
			func(_ *sql.Tx) ([]string, error) {
				return []string{"a", "b"}, nil
			},
		)
		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns zero value for pointer on error", func(t *testing.T) {
		t.Parallel()

		type Entity struct{ ID string }

		result, err := WithTenantTx(
			context.Background(),
			nil,
			func(_ *sql.Tx) (*Entity, error) {
				return &Entity{ID: "123"}, nil
			},
		)
		require.Error(t, err)
		assert.Nil(t, result)
	})
}

// contextWithDefaultTenant creates a context with the default tenant ID for testing.
func contextWithDefaultTenant() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
}

// setupMockConnection creates a mock PostgresConnection for testing.
func setupMockConnection(t *testing.T) (*libPostgres.Client, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)

	return provider.PostgresConn, mock, db
}

func TestWithTenantTxProvider_NilProvider(t *testing.T) {
	t.Parallel()

	result, err := WithTenantTxProvider(
		context.Background(),
		nil,
		func(_ *sql.Tx) (string, error) {
			return "success", nil
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrConnectionRequired)
	assert.Empty(t, result)
}

func TestWithTenantTxProvider_ProviderReturnsError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: expectedErr,
	}

	result, err := WithTenantTxProvider(
		context.Background(),
		provider,
		func(_ *sql.Tx) (string, error) {
			return "success", nil
		},
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to begin transaction")
	assert.Empty(t, result)
}

func TestWithTenantTxProvider_ProviderReturnsNilConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: nil,
	}

	result, err := WithTenantTxProvider(
		context.Background(),
		provider,
		func(_ *sql.Tx) (string, error) {
			return "success", nil
		},
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to begin transaction")
	assert.Empty(t, result)
}

func TestWithTenantTxProvider_Success(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectCommit()

	result, err := WithTenantTxProvider(ctx, provider, func(_ *sql.Tx) (string, error) {
		return "success", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "success", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantTxProvider_FnReturnsError(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithDefaultTenant()
	expectedErr := errors.New("function error")

	mock.ExpectBegin()
	mock.ExpectRollback()

	result, err := WithTenantTxProvider(ctx, provider, func(_ *sql.Tx) (string, error) {
		return "", expectedErr
	})

	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
	assert.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantTxOrExistingProvider_NilProvider(t *testing.T) {
	t.Parallel()

	result, err := WithTenantTxOrExistingProvider(
		context.Background(),
		nil,
		nil,
		func(_ *sql.Tx) (string, error) {
			return "success", nil
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrConnectionRequired)
	assert.Empty(t, result)
}

func TestWithTenantTxOrExistingProvider_WithExistingTx(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = tx.Rollback() }()

	result, err := WithTenantTxOrExistingProvider(
		ctx,
		provider,
		tx,
		func(_ *sql.Tx) (string, error) {
			return "success", nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, "success", result)
}

func TestWithTenantTxOrExistingProvider_CreateNewTx(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectCommit()

	result, err := WithTenantTxOrExistingProvider(
		ctx,
		provider,
		nil,
		func(_ *sql.Tx) (int, error) {
			return 42, nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, 42, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

type nilTxLeaseProvider struct{}

func (nilTxLeaseProvider) GetRedisConnection(context.Context) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (nilTxLeaseProvider) BeginTx(context.Context) (*ports.TxLease, error) {
	return nil, nil
}

func (nilTxLeaseProvider) GetReplicaDB(context.Context) (*ports.DBLease, error) {
	return nil, nil
}

func (nilTxLeaseProvider) GetPrimaryDB(context.Context) (*ports.DBLease, error) {
	return nil, nil
}

type emptyTxLeaseProvider struct{}

func (emptyTxLeaseProvider) GetRedisConnection(context.Context) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (emptyTxLeaseProvider) BeginTx(context.Context) (*ports.TxLease, error) {
	return &ports.TxLease{}, nil
}

func (emptyTxLeaseProvider) GetReplicaDB(context.Context) (*ports.DBLease, error) {
	return nil, nil
}

func (emptyTxLeaseProvider) GetPrimaryDB(context.Context) (*ports.DBLease, error) {
	return nil, nil
}

type deadlineCapturingProvider struct {
	deadlineSet bool
	ctxErr      error
	tx          *sql.Tx
}

func (provider *deadlineCapturingProvider) GetRedisConnection(context.Context) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (provider *deadlineCapturingProvider) BeginTx(ctx context.Context) (*ports.TxLease, error) {
	_, provider.deadlineSet = ctx.Deadline()
	if provider.ctxErr != nil {
		return nil, provider.ctxErr
	}

	return ports.NewTxLease(provider.tx, nil), nil
}

func (provider *deadlineCapturingProvider) GetReplicaDB(context.Context) (*ports.DBLease, error) {
	return nil, nil
}

func (provider *deadlineCapturingProvider) GetPrimaryDB(context.Context) (*ports.DBLease, error) {
	return nil, nil
}

func TestWithTenantTxOrExistingProvider_NilTxLease(t *testing.T) {
	t.Parallel()

	result, err := WithTenantTxOrExistingProvider(
		contextWithDefaultTenant(),
		nilTxLeaseProvider{},
		nil,
		func(_ *sql.Tx) (string, error) {
			return "should not run", nil
		},
	)

	require.ErrorIs(t, err, ErrNilTxLease)
	assert.Empty(t, result)
}

func TestWithTenantTxOrExistingProvider_EmptyTxLease(t *testing.T) {
	t.Parallel()

	result, err := WithTenantTxOrExistingProvider(
		contextWithDefaultTenant(),
		emptyTxLeaseProvider{},
		nil,
		func(_ *sql.Tx) (string, error) {
			return "should not run", nil
		},
	)

	require.ErrorIs(t, err, ErrNilTxLease)
	assert.Empty(t, result)
}

func TestWithTenantTxOrExisting_WithExistingTx_DefaultTenant(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = tx.Rollback() }()

	result, err := WithTenantTxOrExisting(ctx, conn, tx, func(_ *sql.Tx) (string, error) {
		return "with existing tx", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "with existing tx", result)
}

func TestWithTenantTxOrExisting_CreateNewTx_Success(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectCommit()

	result, err := WithTenantTxOrExisting(ctx, conn, nil, func(_ *sql.Tx) (string, error) {
		return "new tx created", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "new tx created", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantTxOrExisting_CommitError(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("commit failed"))

	result, err := WithTenantTxOrExisting(ctx, conn, nil, func(_ *sql.Tx) (string, error) {
		return "result", nil
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to commit transaction")
	assert.Empty(t, result)
}

func TestBeginTenantTx_NilProvider(t *testing.T) {
	t.Parallel()

	tx, cancel, err := BeginTenantTx(context.Background(), nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrConnectionRequired)
	assert.Nil(t, tx)
	assert.NotNil(t, cancel, "cancel must always be non-nil")
	cancel() // safe to call (no-op)
}

func TestBeginTenantTx_ProviderReturnsError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: expectedErr,
	}

	tx, cancel, err := BeginTenantTx(context.Background(), provider)

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to begin transaction")
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	cancel()
}

func TestBeginTenantTx_ProviderReturnsNilConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: nil,
	}

	tx, cancel, err := BeginTenantTx(context.Background(), provider)

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to begin transaction")
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	cancel()
}

func TestBeginTenantTx_NilTxLease(t *testing.T) {
	t.Parallel()

	tx, cancel, err := BeginTenantTx(context.Background(), nilTxLeaseProvider{})

	require.ErrorIs(t, err, ErrNilTxLease)
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	cancel()
}

func TestBeginTenantTx_EmptyTxLease(t *testing.T) {
	t.Parallel()

	tx, cancel, err := BeginTenantTx(context.Background(), emptyTxLeaseProvider{})

	require.ErrorIs(t, err, ErrNilTxLease)
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	cancel()
}

func TestWithTenantTxOrExistingProvider_AddsDefaultDeadlineWhenMissing(t *testing.T) {
	t.Parallel()

	provider := &deadlineCapturingProvider{ctxErr: errors.New("stop")}

	_, err := WithTenantTxOrExistingProvider(context.Background(), provider, nil, func(_ *sql.Tx) (string, error) {
		return "ok", nil
	})
	require.Error(t, err)
	assert.True(t, provider.deadlineSet)
}

func TestWithTenantTxOrExistingProvider_PreservesExistingDeadline(t *testing.T) {
	t.Parallel()

	provider := &deadlineCapturingProvider{ctxErr: errors.New("stop")}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err := WithTenantTxOrExistingProvider(ctx, provider, nil, func(_ *sql.Tx) (string, error) {
		return "ok", nil
	})
	require.Error(t, err)
	assert.True(t, provider.deadlineSet)
}

func TestBeginTenantTx_AddsDefaultDeadlineWhenMissing(t *testing.T) {
	t.Parallel()

	provider := &deadlineCapturingProvider{ctxErr: errors.New("stop")}

	tx, cancel, err := BeginTenantTx(context.Background(), provider)
	require.Error(t, err)
	require.Nil(t, tx)
	assert.True(t, provider.deadlineSet)
	cancel()
}

func TestBeginTenantTx_Success_DefaultTenant(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, cancel, err := BeginTenantTx(ctx, provider)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.NotNil(t, cancel)

	_ = tx.Rollback()
	cancel()

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTenantTx_BeginTxError(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

	tx, cancel, err := BeginTenantTx(ctx, provider)

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to begin transaction")
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	cancel()
}

func TestWithTenantTx_DelegatesCorrectly(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectCommit()

	result, err := WithTenantTx(ctx, conn, func(_ *sql.Tx) (string, error) {
		return "delegated", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "delegated", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantTxOrExisting_FnError_RollsBack(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	ctx := contextWithDefaultTenant()
	fnErr := errors.New("function failed")

	mock.ExpectBegin()
	mock.ExpectRollback()

	result, err := WithTenantTxOrExisting(ctx, conn, nil, func(_ *sql.Tx) (string, error) {
		return "", fnErr
	})

	require.Error(t, err)
	require.ErrorIs(t, err, fnErr)
	assert.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantTxProvider_GenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("returns struct type", func(t *testing.T) {
		t.Parallel()

		conn, mock, db := setupMockConnection(t)
		defer db.Close()

		provider := &testutil.MockInfrastructureProvider{
			PostgresConn: conn,
		}

		ctx := contextWithDefaultTenant()

		mock.ExpectBegin()
		mock.ExpectCommit()

		type Result struct {
			Value int
			Name  string
		}

		result, err := WithTenantTxProvider(ctx, provider, func(_ *sql.Tx) (Result, error) {
			return Result{Value: 100, Name: "test"}, nil
		})

		require.NoError(t, err)
		assert.Equal(t, 100, result.Value)
		assert.Equal(t, "test", result.Name)
	})

	t.Run("returns map type", func(t *testing.T) {
		t.Parallel()

		conn, mock, db := setupMockConnection(t)
		defer db.Close()

		provider := &testutil.MockInfrastructureProvider{
			PostgresConn: conn,
		}

		ctx := contextWithDefaultTenant()

		mock.ExpectBegin()
		mock.ExpectCommit()

		result, err := WithTenantTxProvider(
			ctx,
			provider,
			func(_ *sql.Tx) (map[string]int, error) {
				return map[string]int{"a": 1, "b": 2}, nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, 1, result["a"])
		assert.Equal(t, 2, result["b"])
	})
}

// nonDefaultTenantID is a valid UUID that differs from auth.DefaultTenantID,
// causing ApplyTenantSchema to execute SET LOCAL search_path.
const nonDefaultTenantID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

// contextWithNonDefaultTenant creates a context with a non-default tenant ID.
// This triggers the SET LOCAL search_path SQL statement in ApplyTenantSchema.
func contextWithNonDefaultTenant() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, nonDefaultTenantID)
}

func TestWithTenantTxOrExisting_NonDefaultTenant_NewTx(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	ctx := contextWithNonDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	result, err := WithTenantTxOrExisting(ctx, conn, nil, func(_ *sql.Tx) (string, error) {
		return "tenant-scoped", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "tenant-scoped", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantTxOrExisting_NonDefaultTenant_ExistingTx(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	ctx := contextWithNonDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = tx.Rollback() }()

	result, err := WithTenantTxOrExisting(ctx, conn, tx, func(_ *sql.Tx) (string, error) {
		return "existing-tx-tenant-scoped", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "existing-tx-tenant-scoped", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithTenantTxProvider_NonDefaultTenant_Success(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithNonDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	result, err := WithTenantTxProvider(ctx, provider, func(_ *sql.Tx) (string, error) {
		return "provider-tenant-scoped", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "provider-tenant-scoped", result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTenantTx_NonDefaultTenant_Success(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithNonDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	tx, cancel, err := BeginTenantTx(ctx, provider)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.NotNil(t, cancel)

	_ = tx.Rollback()
	cancel()

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTenantTx_NonDefaultTenant_SchemaError(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithNonDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path").
		WillReturnError(errors.New("schema not found"))
	mock.ExpectRollback()

	tx, cancel, err := BeginTenantTx(ctx, provider)

	require.Error(t, err)
	require.ErrorContains(t, err, "apply tenant schema")
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	cancel()

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBeginTenantTx_CancelFunc_IsCallableAfterCommit(t *testing.T) {
	t.Parallel()

	conn, mock, db := setupMockConnection(t)
	defer db.Close()

	provider := &testutil.MockInfrastructureProvider{
		PostgresConn: conn,
	}

	ctx := contextWithDefaultTenant()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, cancel, err := BeginTenantTx(ctx, provider)

	require.NoError(t, err)
	require.NotNil(t, tx)

	require.NoError(t, tx.Commit())
	cancel() // must not panic

	require.NoError(t, mock.ExpectationsWereMet())
}
