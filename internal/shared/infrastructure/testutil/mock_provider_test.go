//go:build unit

package testutil

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"
	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"

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

func TestMockInfrastructureProvider_GetPostgresConnection(t *testing.T) {
	t.Parallel()

	// v2 postgres.Client has all unexported fields, so we use pointer identity checks.
	testClient := &libPostgres.Client{}

	tests := []struct {
		name         string
		provider     *MockInfrastructureProvider
		wantConn     *libPostgres.Client
		wantErr      error
		wantNilConn  bool
		wantNilError bool
	}{
		{
			name: "returns mocked connection",
			provider: &MockInfrastructureProvider{
				PostgresConn: testClient,
			},
			wantConn:     testClient,
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
			name:         "returns nil connection without error when PostgresConn is nil",
			provider:     &MockInfrastructureProvider{},
			wantNilConn:  true,
			wantNilError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			conn, err := tt.provider.GetPostgresConnection(ctx)

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
				assert.Same(t, tt.wantConn, conn)
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
				assert.Same(t, tt.wantConn, conn)
			}
		})
	}
}

func TestMockInfrastructureProvider_ContextIsIgnored(t *testing.T) {
	t.Parallel()

	pgClient := &libPostgres.Client{}
	redisClient := NewRedisClientWithMock(nil)

	provider := &MockInfrastructureProvider{
		PostgresConn: pgClient,
		RedisConn:    redisClient,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pgConn, pgErr := provider.GetPostgresConnection(ctx)
	require.NoError(t, pgErr)
	assert.NotNil(t, pgConn)

	redisConn, redisErr := provider.GetRedisConnection(ctx)
	require.NoError(t, redisErr)
	assert.NotNil(t, redisConn)
}
