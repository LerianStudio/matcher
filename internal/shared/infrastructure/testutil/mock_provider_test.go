//go:build unit

package testutil

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errTestPostgresConnection = errors.New("postgres connection error")
	errTestPostgresPrecedence = errors.New("postgres error takes precedence")
	errTestRedisConnection    = errors.New("redis connection error")
	errTestRedisPrecedence    = errors.New("redis error takes precedence")
)

func TestMockInfrastructureProvider_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.InfrastructureProvider = (*MockInfrastructureProvider)(nil)
}

func TestMockInfrastructureProvider_GetPrimaryDB(t *testing.T) {
	t.Parallel()

	testDB := &sql.DB{}
	testClient := NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(testDB)))

	tests := []struct {
		name         string
		provider     *MockInfrastructureProvider
		wantConn     *sql.DB
		wantErr      error
		wantNilConn  bool
		wantNilError bool
	}{
		{
			name: "returns mocked connection",
			provider: &MockInfrastructureProvider{
				PostgresConn: testClient,
			},
			wantConn:     testDB,
			wantNilError: true,
		},
		{
			name: "returns mocked error when PostgresErr is set",
			provider: &MockInfrastructureProvider{
				PostgresErr: errTestPostgresConnection,
			},
			wantErr:     errTestPostgresConnection,
			wantNilConn: true,
		},
		{
			name: "returns error even when both connection and error are set",
			provider: &MockInfrastructureProvider{
				PostgresConn: testClient,
				PostgresErr:  errTestPostgresPrecedence,
			},
			wantErr:     errTestPostgresPrecedence,
			wantNilConn: true,
		},
		{
			name:        "returns nil connection without error when PostgresConn is nil",
			provider:    &MockInfrastructureProvider{},
			wantErr:     ErrNoPostgresConnection,
			wantNilConn: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			conn, err := tt.provider.GetPrimaryDB(ctx)

			if tt.wantNilError {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr.Error(), err.Error())
			}

			if tt.wantNilConn {
				assert.Nil(t, conn)
			} else {
				require.NotNil(t, conn)
				assert.Same(t, tt.wantConn, conn.DB())
			}
		})
	}
}

func TestMockInfrastructureProvider_GetRedisConnection(t *testing.T) {
	t.Parallel()

	// v2 redis.Client has all unexported fields, so we use pointer identity checks.
	testRedisClient := NewRedisClientWithMock(nil)

	tests := []struct {
		name         string
		provider     *MockInfrastructureProvider
		wantConn     *libRedis.Client
		wantErr      error
		wantNilConn  bool
		wantNilError bool
	}{
		{
			name: "returns mocked connection",
			provider: &MockInfrastructureProvider{
				RedisConn: testRedisClient,
			},
			wantConn:     testRedisClient,
			wantNilError: true,
		},
		{
			name: "returns mocked error when RedisErr is set",
			provider: &MockInfrastructureProvider{
				RedisErr: errTestRedisConnection,
			},
			wantErr:     errTestRedisConnection,
			wantNilConn: true,
		},
		{
			name: "returns error even when both connection and error are set",
			provider: &MockInfrastructureProvider{
				RedisConn: testRedisClient,
				RedisErr:  errTestRedisPrecedence,
			},
			wantErr:     errTestRedisPrecedence,
			wantNilConn: true,
		},
		{
			name:         "returns nil connection without error when RedisConn is nil",
			provider:     &MockInfrastructureProvider{},
			wantNilConn:  true,
			wantNilError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			conn, err := tt.provider.GetRedisConnection(ctx)

			if tt.wantNilError {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr.Error(), err.Error())
			}

			if tt.wantNilConn {
				assert.Nil(t, conn)
			} else {
				require.NotNil(t, conn)
				assert.Same(t, tt.wantConn, conn.Connection())
			}
		})
	}
}

func TestMockInfrastructureProvider_ContextIsIgnored(t *testing.T) {
	t.Parallel()

	pgDB := &sql.DB{}
	pgClient := NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(pgDB)))
	redisClient := NewRedisClientWithMock(nil)

	provider := &MockInfrastructureProvider{
		PostgresConn: pgClient,
		RedisConn:    redisClient,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pgConn, pgErr := provider.GetPrimaryDB(ctx)
	require.NoError(t, pgErr)
	assert.NotNil(t, pgConn)
	assert.Same(t, pgDB, pgConn.DB())

	redisConn, redisErr := provider.GetRedisConnection(ctx)
	require.NoError(t, redisErr)
	assert.NotNil(t, redisConn)
	assert.Same(t, redisClient, redisConn.Connection())
}
