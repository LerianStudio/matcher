//go:build unit

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupRedis(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close redis client: %v", err)
		}

		srv.Close()
	})

	return srv, client
}

func TestDedupeServiceCalculateHashDeterministic(t *testing.T) {
	t.Parallel()

	service := &DedupeService{}
	id := uuid.MustParse("00000000-0000-0000-0000-000000000003")

	hash1 := service.CalculateHash(id, "ext")
	hash2 := service.CalculateHash(id, "ext")
	require.Equal(t, hash1, hash2)
	require.NotEmpty(t, hash1)
}

func TestDedupeServiceIsDuplicateNilConnection(t *testing.T) {
	t.Parallel()

	// Zero-value DedupeService has a nil connection.
	service := &DedupeService{}
	_, err := service.IsDuplicate(context.Background(), uuid.New(), "hash")
	require.ErrorIs(t, err, errRedisConnRequired)
}

func TestDedupeServiceIsDuplicate(t *testing.T) {
	t.Parallel()

	_, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)

	ctx := context.Background()
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	hash := service.CalculateHash(sourceID, "ext")

	isDuplicate, err := service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	require.False(t, isDuplicate)

	key := service.buildKey(contextID, hash)
	require.NoError(t, client.Set(ctx, key, "1", 0).Err())

	isDuplicate, err = service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	require.True(t, isDuplicate)
}

func TestDedupeServiceMarkSeenNilConnection(t *testing.T) {
	t.Parallel()

	// Zero-value DedupeService has a nil connection.
	service := &DedupeService{}
	err := service.MarkSeen(context.Background(), uuid.New(), "hash", 0)
	require.ErrorIs(t, err, errRedisConnRequired)
}

func TestDedupeServiceMarkSeenSetsKey(t *testing.T) {
	t.Parallel()

	_, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)

	ctx := context.Background()
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	hash := service.CalculateHash(sourceID, "ext")

	require.NoError(t, service.MarkSeen(ctx, contextID, hash, time.Minute))
	key := service.buildKey(contextID, hash)
	require.Positive(t, client.Exists(ctx, key).Val())
}

func TestDedupeServiceMarkSeenWithRetryReturnsDuplicate(t *testing.T) {
	t.Parallel()

	_, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	hash := service.CalculateHash(sourceID, "ext")
	ttl := 5 * time.Second

	require.NoError(t, service.MarkSeen(context.Background(), contextID, hash, ttl))
	err := service.MarkSeenWithRetry(context.Background(), contextID, hash, ttl, 1)
	require.ErrorIs(t, err, ports.ErrDuplicateTransaction)
}

func TestDedupeServiceMarkSeenWithRetryReturnsRedisError(t *testing.T) {
	t.Parallel()

	server, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	hash := service.CalculateHash(sourceID, "ext")

	server.SetError("LOADING Redis is loading the dataset in memory")
	defer server.SetError("")

	err := service.MarkSeenWithRetry(context.Background(), contextID, hash, 0, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "redis setnx failed")
}

func TestDedupeServiceMarkSeenWithRetryDefaultsWhenZero(t *testing.T) {
	t.Parallel()

	_, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	hash := service.CalculateHash(sourceID, "ext")

	err := service.MarkSeenWithRetry(context.Background(), contextID, hash, 0, 0)
	require.NoError(t, err)
}

func TestDedupeServiceMarkSeenWithRetry_InterruptedByContextCancellation(t *testing.T) {
	t.Parallel()

	server, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	hash := service.CalculateHash(sourceID, "ext")

	server.SetError("LOADING Redis is loading the dataset in memory")
	defer server.SetError("")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := service.MarkSeenWithRetry(ctx, contextID, hash, 0, 2)
	require.Error(t, err)
	require.ErrorContains(t, err, "retry interrupted")
	require.ErrorIs(t, err, context.Canceled)
}

func TestDedupeServiceIsDuplicateRedisError(t *testing.T) {
	t.Parallel()

	server, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)

	server.SetError("LOADING Redis is loading the dataset in memory")
	defer server.SetError("")

	_, err := service.IsDuplicate(context.Background(), uuid.New(), "hash")
	require.Error(t, err)
	require.Contains(t, err.Error(), "redis exists check failed")
}

func TestDedupeServiceMarkSeenRedisError(t *testing.T) {
	t.Parallel()

	server, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)

	server.SetError("READONLY You can't write against a read only replica")
	defer server.SetError("")

	err := service.MarkSeen(context.Background(), uuid.New(), "hash", time.Minute)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to mark seen")
}

func TestDedupeServiceClearRemovesKey(t *testing.T) {
	t.Parallel()

	_, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)

	ctx := context.Background()
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	hash := service.CalculateHash(sourceID, "ext-clear")

	require.NoError(t, service.MarkSeen(ctx, contextID, hash, time.Minute))

	isDup, err := service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	require.True(t, isDup)

	require.NoError(t, service.Clear(ctx, contextID, hash))

	isDup, err = service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	require.False(t, isDup)
}

func TestDedupeServiceClearNilConnection(t *testing.T) {
	t.Parallel()

	service := &DedupeService{}
	err := service.Clear(context.Background(), uuid.New(), "hash")
	require.ErrorIs(t, err, errRedisConnRequired)
}

func TestDedupeServiceClearBatchRemovesKeys(t *testing.T) {
	t.Parallel()

	_, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)

	ctx := context.Background()
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	hashes := []string{
		service.CalculateHash(sourceID, "ext-batch-1"),
		service.CalculateHash(sourceID, "ext-batch-2"),
		service.CalculateHash(sourceID, "ext-batch-3"),
	}

	for _, hash := range hashes {
		require.NoError(t, service.MarkSeen(ctx, contextID, hash, time.Minute))
	}

	for _, hash := range hashes {
		isDup, err := service.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		require.True(t, isDup)
	}

	require.NoError(t, service.ClearBatch(ctx, contextID, hashes))

	for _, hash := range hashes {
		isDup, err := service.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		require.False(t, isDup)
	}
}

func TestDedupeServiceClearBatchNilConnection(t *testing.T) {
	t.Parallel()

	service := &DedupeService{}
	err := service.ClearBatch(context.Background(), uuid.New(), []string{"hash"})
	require.ErrorIs(t, err, errRedisConnRequired)
}

func TestDedupeServiceClearBatchEmptyHashes(t *testing.T) {
	t.Parallel()

	_, client := setupRedis(t)
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewDedupeService(provider)

	err := service.ClearBatch(context.Background(), uuid.New(), []string{})
	require.NoError(t, err)
}
