//go:build unit

package common

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// mockInfrastructureProvider implements ports.InfrastructureProvider for testing.
type mockInfrastructureProvider struct {
	postgresConn  *libPostgres.Client
	postgresErr   error
	redisConn     *libRedis.Client
	redisErr      error
	beginTxResult *sql.Tx
	beginTxErr    error
	replicaDB     *sql.DB
	replicaDBErr  error
}

func (m *mockInfrastructureProvider) GetPostgresConnection(_ context.Context) (*libPostgres.Client, error) {
	return m.postgresConn, m.postgresErr
}

func (m *mockInfrastructureProvider) GetRedisConnection(_ context.Context) (*libRedis.Client, error) {
	return m.redisConn, m.redisErr
}

func (m *mockInfrastructureProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return m.beginTxResult, m.beginTxErr
}

func (m *mockInfrastructureProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return m.replicaDB, m.replicaDBErr
}

var _ ports.InfrastructureProvider = (*mockInfrastructureProvider)(nil)

func TestWithTenantTxNilConnection(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTx(
		context.Background(),
		nil,
		func(tx *sql.Tx) (int, error) { return 0, nil },
	)
	require.Error(t, err)
}

func TestWithTenantTx_NilConnectionDB_ReturnsError(t *testing.T) {
	t.Parallel()

	conn := &libPostgres.Client{}

	_, err := WithTenantTx(
		context.Background(),
		conn,
		func(tx *sql.Tx) (int, error) { return 0, nil },
	)
	require.Error(t, err)
}

func TestWithTenantTxOrExisting_NilConnection(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxOrExisting(
		context.Background(),
		nil,
		nil,
		func(tx *sql.Tx) (string, error) {
			return "result", nil
		},
	)
	require.Error(t, err)
}

func TestWithTenantTxOrExisting_NilConnectionAndNilTx(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxOrExisting(context.Background(), nil, nil, func(tx *sql.Tx) (int, error) {
		return 42, nil
	})
	require.Error(t, err)
}

func TestWithTenantTxProvider_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxProvider(context.Background(), nil, func(tx *sql.Tx) (bool, error) {
		return true, nil
	})
	require.Error(t, err)
}

func TestWithTenantTxOrExistingProvider_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxOrExistingProvider(
		context.Background(),
		nil,
		nil,
		func(tx *sql.Tx) (float64, error) {
			return 3.14, nil
		},
	)
	require.Error(t, err)
}

func TestWithTenantReadQuery_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantReadQuery(
		context.Background(),
		nil,
		func(qe QueryExecutor) ([]string, error) {
			return []string{"test"}, nil
		},
	)
	require.Error(t, err)
}

func TestWithTenantTx_ErrorWrapping(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (int, error) {
		return 0, errors.New("inner error")
	})
	require.Error(t, err)
}

func TestWithTenantTxOrExisting_ErrorWrapping(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxOrExisting(context.Background(), nil, nil, func(tx *sql.Tx) (int, error) {
		return 0, errors.New("inner error")
	})
	require.Error(t, err)
}

func TestWithTenantTxProvider_ErrorWrapping(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxProvider(context.Background(), nil, func(tx *sql.Tx) (int, error) {
		return 0, errors.New("inner error")
	})
	require.Error(t, err)
}

func TestWithTenantTxOrExistingProvider_ErrorWrapping(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxOrExistingProvider(
		context.Background(),
		nil,
		nil,
		func(tx *sql.Tx) (int, error) {
			return 0, errors.New("inner error")
		},
	)
	require.Error(t, err)
}

func TestWithTenantReadQuery_ErrorWrapping(t *testing.T) {
	t.Parallel()

	_, err := WithTenantReadQuery(context.Background(), nil, func(qe QueryExecutor) (int, error) {
		return 0, errors.New("inner error")
	})
	require.Error(t, err)
}

func TestWithTenantTx_GenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("string type", func(t *testing.T) {
		t.Parallel()
		_, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (string, error) {
			return "test", nil
		})
		require.Error(t, err)
	})

	t.Run("struct type", func(t *testing.T) {
		t.Parallel()
		type Result struct {
			ID   int
			Name string
		}
		_, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (Result, error) {
			return Result{ID: 1, Name: "test"}, nil
		})
		require.Error(t, err)
	})

	t.Run("slice type", func(t *testing.T) {
		t.Parallel()
		_, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) ([]int, error) {
			return []int{1, 2, 3}, nil
		})
		require.Error(t, err)
	})

	t.Run("pointer type", func(t *testing.T) {
		t.Parallel()
		_, err := WithTenantTx(context.Background(), nil, func(tx *sql.Tx) (*int, error) {
			val := 42
			return &val, nil
		})
		require.Error(t, err)
	})
}

func TestWithTenantTxOrExisting_GenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("map type", func(t *testing.T) {
		t.Parallel()
		_, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (map[string]int, error) {
				return map[string]int{"key": 1}, nil
			},
		)
		require.Error(t, err)
	})

	t.Run("interface type", func(t *testing.T) {
		t.Parallel()
		_, err := WithTenantTxOrExisting(
			context.Background(),
			nil,
			nil,
			func(tx *sql.Tx) (interface{}, error) {
				return "any value", nil
			},
		)
		require.Error(t, err)
	})
}

func TestWithTenantTxProvider_GenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("bool type", func(t *testing.T) {
		t.Parallel()
		_, err := WithTenantTxProvider(context.Background(), nil, func(tx *sql.Tx) (bool, error) {
			return true, nil
		})
		require.Error(t, err)
	})

	t.Run("float64 type", func(t *testing.T) {
		t.Parallel()
		_, err := WithTenantTxProvider(
			context.Background(),
			nil,
			func(tx *sql.Tx) (float64, error) {
				return 3.14159, nil
			},
		)
		require.Error(t, err)
	})
}

func TestWithTenantReadQuery_GenericTypes(t *testing.T) {
	t.Parallel()

	t.Run("struct slice type", func(t *testing.T) {
		t.Parallel()
		type Item struct {
			Value string
		}
		_, err := WithTenantReadQuery(
			context.Background(),
			nil,
			func(qe QueryExecutor) ([]Item, error) {
				return []Item{{Value: "test"}}, nil
			},
		)
		require.Error(t, err)
	})
}

func TestQueryExecutor_TypeAlias(t *testing.T) {
	t.Parallel()

	var _ QueryExecutor = (*sql.DB)(nil)
	var _ QueryExecutor = (*sql.Tx)(nil)
}

func TestWithTenantTxProvider_GetPostgresConnectionError(t *testing.T) {
	t.Parallel()

	provider := &mockInfrastructureProvider{
		postgresErr: errors.New("connection failed"),
	}

	_, err := WithTenantTxProvider(context.Background(), provider, func(tx *sql.Tx) (int, error) {
		return 42, nil
	})
	require.Error(t, err)
}

func TestWithTenantTxProvider_NilConnectionReturned(t *testing.T) {
	t.Parallel()

	provider := &mockInfrastructureProvider{
		postgresConn: nil,
		postgresErr:  nil,
	}

	_, err := WithTenantTxProvider(context.Background(), provider, func(tx *sql.Tx) (string, error) {
		return "result", nil
	})
	require.Error(t, err)
}

func TestWithTenantTxOrExistingProvider_GetPostgresConnectionError(t *testing.T) {
	t.Parallel()

	provider := &mockInfrastructureProvider{
		postgresErr: errors.New("database unavailable"),
	}

	_, err := WithTenantTxOrExistingProvider(
		context.Background(),
		provider,
		nil,
		func(tx *sql.Tx) (bool, error) {
			return true, nil
		},
	)
	require.Error(t, err)
}

func TestWithTenantTxOrExistingProvider_NilConnectionReturned(t *testing.T) {
	t.Parallel()

	provider := &mockInfrastructureProvider{
		postgresConn: nil,
		postgresErr:  nil,
	}

	_, err := WithTenantTxOrExistingProvider(
		context.Background(),
		provider,
		nil,
		func(tx *sql.Tx) (float64, error) {
			return 3.14, nil
		},
	)
	require.Error(t, err)
}

func TestWithTenantReadQuery_GetReplicaDBError(t *testing.T) {
	t.Parallel()

	provider := &mockInfrastructureProvider{
		replicaDBErr: errors.New("replica unavailable"),
	}

	_, err := WithTenantReadQuery(
		context.Background(),
		provider,
		func(qe QueryExecutor) ([]int, error) {
			return []int{1, 2, 3}, nil
		},
	)
	require.Error(t, err)
}

func TestWithTenantTxOrExisting_EmptyConnection(t *testing.T) {
	t.Parallel()

	conn := &libPostgres.Client{}

	_, err := WithTenantTxOrExisting(
		context.Background(),
		conn,
		nil,
		func(tx *sql.Tx) (int, error) {
			return 100, nil
		},
	)
	require.Error(t, err)
}

func TestWithTenantTxProvider_EmptyConnection(t *testing.T) {
	t.Parallel()

	provider := &mockInfrastructureProvider{
		postgresConn: &libPostgres.Client{},
		postgresErr:  nil,
	}

	_, err := WithTenantTxProvider(context.Background(), provider, func(tx *sql.Tx) (int, error) {
		return 42, nil
	})
	require.Error(t, err)
}

func TestWithTenantTxOrExistingProvider_EmptyConnection(t *testing.T) {
	t.Parallel()

	provider := &mockInfrastructureProvider{
		postgresConn: &libPostgres.Client{},
		postgresErr:  nil,
	}

	_, err := WithTenantTxOrExistingProvider(
		context.Background(),
		provider,
		nil,
		func(tx *sql.Tx) (string, error) {
			return "test", nil
		},
	)
	require.Error(t, err)
}
