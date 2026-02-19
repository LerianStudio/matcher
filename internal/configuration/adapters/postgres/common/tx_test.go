//go:build unit

package common

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"

	sharedCommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestWithTenantTx(t *testing.T) {
	t.Parallel()

	t.Run("returns error when connection is nil", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (int, error) {
			return 42, nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Zero(t, result)
	})

	t.Run("returns zero value for string type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (string, error) {
			return "value", nil
		})

		require.Error(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns zero value for slice type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) ([]string, error) {
			return []string{"a", "b"}, nil
		})

		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns zero value for pointer type on error", func(t *testing.T) {
		t.Parallel()

		type Entity struct {
			ID string
		}

		result, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (*Entity, error) {
			return &Entity{ID: "123"}, nil
		})

		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns zero value for map type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTx(
			context.Background(),
			nil,
			func(tx *sql.Tx) (map[string]int, error) {
				return map[string]int{"a": 1}, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns error when connection has nil db", func(t *testing.T) {
		t.Parallel()

		conn := &libPostgres.Client{}

		result, err := WithTenantTx(context.Background(), conn, func(tx *sql.Tx) (int, error) {
			return 42, nil
		})

		require.Error(t, err)
		assert.Zero(t, result)
	})
}

func TestWithTenantTxOrExisting(t *testing.T) {
	t.Parallel()

	t.Run("returns error when connection is nil and tx is nil", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (int, error) {
				return 42, nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Zero(t, result)
	})

	t.Run("returns error when connection is nil even with valid tx", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.Begin()
		require.NoError(t, err)

		result, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			tx,
			func(execTx *sql.Tx) (string, error) {
				return "success", nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Empty(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns zero value for struct type on error", func(t *testing.T) {
		t.Parallel()

		type Result struct {
			Count   int
			Message string
		}

		result, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (Result, error) {
				return Result{Count: 10, Message: "ok"}, nil
			},
		)

		require.Error(t, err)
		assert.Zero(t, result.Count)
		assert.Empty(t, result.Message)
	})

	t.Run("returns zero value for bool type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (bool, error) {
				return true, nil
			},
		)

		require.Error(t, err)
		assert.False(t, result)
	})

	t.Run("returns error when connection has nil db", func(t *testing.T) {
		t.Parallel()

		conn := &libPostgres.Client{}

		result, err := WithTenantTxOrExisting(
			context.Background(),
			conn,
			nil,
			func(tx *sql.Tx) (int, error) {
				return 42, nil
			},
		)

		require.Error(t, err)
		assert.Zero(t, result)
	})
}

func TestWithTenantTxProvider(t *testing.T) {
	t.Parallel()

	t.Run("returns error when provider is nil", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxProvider(
			context.Background(),
			nil,
			func(tx *sql.Tx) (int, error) {
				return 42, nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Zero(t, result)
	})

	t.Run("returns error when provider returns nil connection", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresConn: nil,
		}

		result, err := WithTenantTxProvider(
			context.Background(),
			provider,
			func(tx *sql.Tx) (string, error) {
				return "value", nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Empty(t, result)
	})

	t.Run("returns error when provider returns error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("connection error")
		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: expectedErr,
		}

		result, err := WithTenantTxProvider(
			context.Background(),
			provider,
			func(tx *sql.Tx) (int, error) {
				return 100, nil
			},
		)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get postgres connection")
		assert.Zero(t, result)
	})

	t.Run("returns zero value for slice type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxProvider(
			context.Background(),
			nil,
			func(tx *sql.Tx) ([]int, error) {
				return []int{1, 2, 3}, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns zero value for interface type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxProvider(
			context.Background(),
			nil,
			func(tx *sql.Tx) (any, error) {
				return "anything", nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestWithTenantTxOrExistingProvider(t *testing.T) {
	t.Parallel()

	t.Run("returns error when provider is nil and tx is nil", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxOrExistingProvider(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (int, error) {
				return 42, nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Zero(t, result)
	})

	t.Run("returns error when provider is nil even with valid tx", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.Begin()
		require.NoError(t, err)

		result, err := WithTenantTxOrExistingProvider(
			context.Background(),
			nil,
			tx,
			func(execTx *sql.Tx) (string, error) {
				return "success", nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Empty(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error when provider returns nil connection", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresConn: nil,
		}

		result, err := WithTenantTxOrExistingProvider(
			context.Background(),
			provider,
			nil,
			func(tx *sql.Tx) (float64, error) {
				return 3.14, nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
		assert.Zero(t, result)
	})

	t.Run("returns error when provider returns error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("provider error")
		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: expectedErr,
		}

		result, err := WithTenantTxOrExistingProvider(
			context.Background(),
			provider,
			nil,
			func(tx *sql.Tx) (int, error) {
				return 100, nil
			},
		)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get postgres connection")
		assert.Zero(t, result)
	})

	t.Run("returns zero value for channel type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxOrExistingProvider(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (chan int, error) {
				ch := make(chan int)
				return ch, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns zero value for function type on error", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxOrExistingProvider(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (func() string, error) {
				return func() string { return "hello" }, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestErrorWrapping(t *testing.T) {
	t.Parallel()

	t.Run("WithTenantTx passes through shared layer error", func(t *testing.T) {
		t.Parallel()

		_, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (int, error) {
			return 0, nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
	})

	t.Run("WithTenantTxOrExisting passes through shared layer error", func(t *testing.T) {
		t.Parallel()

		_, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (int, error) {
				return 0, nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
	})

	t.Run("WithTenantTxProvider passes through shared layer error", func(t *testing.T) {
		t.Parallel()

		_, err := WithTenantTxProvider(context.Background(), nil, func(tx *sql.Tx) (int, error) {
			return 0, nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
	})

	t.Run("WithTenantTxOrExistingProvider passes through shared layer error", func(t *testing.T) {
		t.Parallel()

		_, err := WithTenantTxOrExistingProvider(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (int, error) {
				return 0, nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
	})

	t.Run("provider error is wrapped correctly", func(t *testing.T) {
		t.Parallel()

		providerErr := errors.New("custom provider error")
		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: providerErr,
		}

		_, err := WithTenantTxProvider(
			context.Background(),
			provider,
			func(tx *sql.Tx) (int, error) {
				return 0, nil
			},
		)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get postgres connection")
		assert.Contains(t, err.Error(), "custom provider error")
	})
}

func TestGenericTypeVariants(t *testing.T) {
	t.Parallel()

	t.Run("WithTenantTx handles complex struct types", func(t *testing.T) {
		t.Parallel()

		type ComplexResult struct {
			Items  []string
			Counts map[string]int
			Active bool
		}

		result, err := WithTenantTx(
			context.Background(),
			nil,
			func(tx *sql.Tx) (ComplexResult, error) {
				return ComplexResult{
					Items:  []string{"a", "b"},
					Counts: map[string]int{"x": 1},
					Active: true,
				}, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result.Items)
		assert.Nil(t, result.Counts)
		assert.False(t, result.Active)
	})

	t.Run("WithTenantTxProvider handles nested pointer types", func(t *testing.T) {
		t.Parallel()

		type Inner struct {
			Value int
		}
		type Outer struct {
			Inner *Inner
		}

		result, err := WithTenantTxProvider(
			context.Background(),
			nil,
			func(tx *sql.Tx) (*Outer, error) {
				return &Outer{Inner: &Inner{Value: 42}}, nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WithTenantTxOrExisting handles error type result", func(t *testing.T) {
		t.Parallel()

		result, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (error, error) {
				return errors.New("inner error"), nil
			},
		)

		require.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestContextHandling(t *testing.T) {
	t.Parallel()

	t.Run("WithTenantTx accepts canceled context", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := WithTenantTx(ctx, nil, func(tx *sql.Tx) (int, error) {
			return 0, nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
	})

	t.Run("WithTenantTxProvider accepts context with values", func(t *testing.T) {
		t.Parallel()

		type contextKey string
		ctx := context.WithValue(context.Background(), contextKey("key"), "value")

		_, err := WithTenantTxProvider(ctx, nil, func(tx *sql.Tx) (int, error) {
			return 0, nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, sharedCommon.ErrConnectionRequired)
	})
}
