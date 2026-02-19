//go:build unit

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestLockManager_AcquireAndRelease(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}

	lm := NewLockManager(provider)

	ctx := context.WithValue(
		context.Background(),
		auth.TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440000",
	)
	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000f001")
	transactionID := uuid.MustParse("00000000-0000-0000-0000-00000000f010")

	acquiredLock, err := lm.AcquireTransactionsLock(
		ctx,
		contextID,
		[]uuid.UUID{transactionID},
		2*time.Minute,
	)
	require.NoError(t, err)
	require.NotNil(t, acquiredLock)

	keys := srv.Keys()
	require.Len(t, keys, 1)
	lockTTL := srv.TTL(keys[0])
	require.Greater(t, lockTTL, time.Duration(0))
	require.InDelta(t, (2 * time.Minute).Seconds(), lockTTL.Seconds(), 1)

	require.NoError(t, acquiredLock.Release(ctx))
	require.Empty(t, srv.Keys())
}

func TestLockManager_AlreadyHeld(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}

	lm := NewLockManager(provider)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000f002")
	transactionID := uuid.MustParse("00000000-0000-0000-0000-00000000f011")

	_, err := lm.AcquireTransactionsLock(ctx, contextID, []uuid.UUID{transactionID}, 2*time.Minute)
	require.NoError(t, err)

	_, err = lm.AcquireTransactionsLock(ctx, contextID, []uuid.UUID{transactionID}, 2*time.Minute)
	require.ErrorIs(t, err, ports.ErrLockAlreadyHeld)
}

func TestLockManager_TenantIsolation(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}

	lm := NewLockManager(provider)
	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000f012")
	transactionID := uuid.MustParse("00000000-0000-0000-0000-00000000f013")

	ctxTenantA := context.WithValue(
		context.Background(),
		auth.TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440000",
	)
	ctxTenantB := context.WithValue(
		context.Background(),
		auth.TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440001",
	)

	_, err := lm.AcquireTransactionsLock(
		ctxTenantA,
		contextID,
		[]uuid.UUID{transactionID},
		2*time.Minute,
	)
	require.NoError(t, err)

	_, err = lm.AcquireTransactionsLock(
		ctxTenantB,
		contextID,
		[]uuid.UUID{transactionID},
		2*time.Minute,
	)
	require.NoError(t, err)

	keys := srv.Keys()
	require.Len(t, keys, 2)
	require.NotEqual(t, keys[0], keys[1])
}

func TestLockManager_EmptyIDs_NoOpLock(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}

	lm := NewLockManager(provider)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	emptyIDsLock, err := lm.AcquireTransactionsLock(ctx, uuid.New(), nil, 1*time.Minute)
	require.NoError(t, err)
	require.NotNil(t, emptyIDsLock)
	require.NoError(t, emptyIDsLock.Release(ctx))
	require.Empty(t, srv.Keys())

	emptySliceLock, err := lm.AcquireTransactionsLock(ctx, uuid.New(), []uuid.UUID{}, 1*time.Minute)
	require.NoError(t, err)
	require.NotNil(t, emptySliceLock)
	require.NoError(t, emptySliceLock.Release(ctx))
	require.Empty(t, srv.Keys())
}

func TestNewLockManager(t *testing.T) {
	t.Parallel()

	t.Run("with nil provider returns manager", func(t *testing.T) {
		t.Parallel()

		lm := NewLockManager(nil)

		assert.NotNil(t, lm)
		assert.Nil(t, lm.provider)
	})

	t.Run("with valid provider stores reference", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		lm := NewLockManager(provider)

		assert.NotNil(t, lm)
		assert.NotNil(t, lm.provider)
	})
}

func TestLockManager_NilReceiver(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	t.Run("AcquireTransactionsLock with nil manager", func(t *testing.T) {
		t.Parallel()

		var lm *LockManager

		_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})

	t.Run("AcquireContextLock with nil manager", func(t *testing.T) {
		t.Parallel()

		var lm *LockManager

		_, err := lm.AcquireContextLock(ctx, contextID, time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})
}

func TestLockManager_NilProvider(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(nil)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	t.Run("AcquireTransactionsLock returns error", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})

	t.Run("AcquireContextLock returns error", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireContextLock(ctx, contextID, time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})
}

func TestLockManager_InvalidTTL(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	t.Run("zero TTL", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, 0)

		require.ErrorIs(t, err, errLockTTLInvalid)
	})

	t.Run("negative TTL", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, -time.Minute)

		require.ErrorIs(t, err, errLockTTLInvalid)
	})

	t.Run("zero TTL for context lock", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireContextLock(ctx, contextID, 0)

		require.ErrorIs(t, err, errLockTTLInvalid)
	})

	t.Run("negative TTL for context lock", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireContextLock(ctx, contextID, -time.Minute)

		require.ErrorIs(t, err, errLockTTLInvalid)
	})
}

func TestLockManager_InvalidContextID(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	t.Run("nil UUID for transactions lock", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireTransactionsLock(ctx, uuid.Nil, nil, time.Minute)

		require.ErrorIs(t, err, errLockContextIDInvalid)
	})

	t.Run("nil UUID for context lock", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireContextLock(ctx, uuid.Nil, time.Minute)

		require.ErrorIs(t, err, errLockContextIDInvalid)
	})
}

func TestLockManager_TenantIDHandling(t *testing.T) {
	t.Parallel()

	t.Run("empty context uses default tenant", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		conn := testutil.NewRedisClientWithMock(client)
		provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
		lm := NewLockManager(provider)

		acquiredLock, err := lm.AcquireTransactionsLock(
			context.Background(),
			uuid.New(),
			nil,
			time.Minute,
		)

		require.NoError(t, err)
		require.NotNil(t, acquiredLock)

		keys := srv.Keys()
		assert.Len(t, keys, 1)
		assert.Contains(t, keys[0], auth.DefaultTenantID)
	})

	t.Run("whitespace-only tenant ID returns error", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		conn := testutil.NewRedisClientWithMock(client)
		provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
		lm := NewLockManager(provider)

		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "   ")
		_, err := lm.AcquireTransactionsLock(ctx, uuid.New(), nil, time.Minute)

		require.ErrorIs(t, err, errLockTenantRequired)
	})

	t.Run("custom tenant ID used correctly", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		conn := testutil.NewRedisClientWithMock(client)
		provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
		lm := NewLockManager(provider)

		customTenantID := "22222222-2222-2222-2222-222222222222"
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, customTenantID)
		acquiredLock, err := lm.AcquireTransactionsLock(ctx, uuid.New(), nil, time.Minute)

		require.NoError(t, err)
		require.NotNil(t, acquiredLock)

		keys := srv.Keys()
		assert.Len(t, keys, 1)
		assert.Contains(t, keys[0], customTenantID)
	})
}

func TestLockManager_RedisConnectionError(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisErr: errRedisConnRequired}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	t.Run("AcquireTransactionsLock wraps error", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)

		require.Error(t, err)
		require.ErrorIs(t, err, errRedisConnRequired)
		assert.Contains(t, err.Error(), "get redis connection")
	})

	t.Run("AcquireContextLock wraps error", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireContextLock(ctx, contextID, time.Minute)

		require.Error(t, err)
		require.ErrorIs(t, err, errRedisConnRequired)
		assert.Contains(t, err.Error(), "get redis connection")
	})
}

func TestLockManager_NilRedisConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	t.Run("nil connection returns error", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})
}

func TestLockManager_NilRedisClient(t *testing.T) {
	t.Parallel()

	conn := testutil.NewRedisClientWithMock(nil)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	t.Run("nil client returns error", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})
}

func TestLockManager_AcquireContextLock(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	t.Run("acquire and release", func(t *testing.T) {
		t.Parallel()

		ctxLocal := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-context-lock")
		ctxID := uuid.New()

		acquiredLock, err := lm.AcquireContextLock(ctxLocal, ctxID, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, acquiredLock)

		err = acquiredLock.Release(ctxLocal)

		require.NoError(t, err)
	})

	t.Run("already held", func(t *testing.T) {
		t.Parallel()

		ctxLocal := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-already-held")
		ctxID := uuid.New()

		_, err := lm.AcquireContextLock(ctxLocal, ctxID, time.Minute)
		require.NoError(t, err)

		_, err = lm.AcquireContextLock(ctxLocal, ctxID, time.Minute)

		require.ErrorIs(t, err, ports.ErrLockAlreadyHeld)
	})

	t.Run("same key as transactions lock", func(t *testing.T) {
		t.Parallel()

		_, err := lm.AcquireContextLock(ctx, contextID, time.Minute)
		require.NoError(t, err)

		_, err = lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)

		require.ErrorIs(t, err, ports.ErrLockAlreadyHeld)
	})
}

func TestLock_Release(t *testing.T) {
	t.Parallel()

	t.Run("nil lock", func(t *testing.T) {
		t.Parallel()

		var lck *lock

		err := lck.Release(context.Background())

		require.ErrorIs(t, err, errRedisConnRequired)
	})

	t.Run("nil connection", func(t *testing.T) {
		t.Parallel()

		lck := &lock{conn: nil, key: "test-key", token: "test-token"}

		err := lck.Release(context.Background())

		require.ErrorIs(t, err, errRedisConnRequired)
	})

	t.Run("nil client", func(t *testing.T) {
		t.Parallel()

		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(nil),
			key:   "test-key",
			token: "test-token",
		}

		err := lck.Release(context.Background())

		require.ErrorIs(t, err, errRedisConnRequired)
	})

	t.Run("empty key returns nil", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		lck := &lock{conn: testutil.NewRedisClientWithMock(client), key: "", token: "test-token"}

		err := lck.Release(context.Background())

		require.NoError(t, err)
	})

	t.Run("whitespace-only key returns nil", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(client),
			key:   "   ",
			token: "test-token",
		}

		err := lck.Release(context.Background())

		require.NoError(t, err)
	})

	t.Run("wrong token returns lock already held", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})

		key := "test-release-wrong-token"
		err := srv.Set(key, "correct-token")
		require.NoError(t, err)

		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(client),
			key:   key,
			token: "wrong-token",
		}
		err = lck.Release(context.Background())

		require.ErrorIs(t, err, ports.ErrLockAlreadyHeld)
	})

	t.Run("redis error is wrapped", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(client),
			key:   "test-key",
			token: "test-token",
		}

		srv.Close()

		err := lck.Release(context.Background())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "release lock")
	})
}

func TestLock_Refresh_NilChecks(t *testing.T) {
	t.Parallel()

	t.Run("nil lock", func(t *testing.T) {
		t.Parallel()

		var lck *lock

		err := lck.Refresh(context.Background(), time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})

	t.Run("nil connection", func(t *testing.T) {
		t.Parallel()

		lck := &lock{conn: nil, key: "test-key", token: "test-token"}

		err := lck.Refresh(context.Background(), time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})

	t.Run("nil client", func(t *testing.T) {
		t.Parallel()

		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(nil),
			key:   "test-key",
			token: "test-token",
		}

		err := lck.Refresh(context.Background(), time.Minute)

		require.ErrorIs(t, err, errRedisConnRequired)
	})
}

func TestLock_Refresh_EmptyKey(t *testing.T) {
	t.Parallel()

	t.Run("empty key returns nil", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		lck := &lock{conn: testutil.NewRedisClientWithMock(client), key: "", token: "test-token"}

		err := lck.Refresh(context.Background(), time.Minute)

		require.NoError(t, err)
	})

	t.Run("whitespace-only key returns nil", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(client),
			key:   "   ",
			token: "test-token",
		}

		err := lck.Refresh(context.Background(), time.Minute)

		require.NoError(t, err)
	})
}

func TestLock_Refresh_Operations(t *testing.T) {
	t.Parallel()

	t.Run("successful refresh extends TTL", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})

		key := "test-refresh-success"
		token := "correct-token"
		err := srv.Set(key, token)
		require.NoError(t, err)
		srv.SetTTL(key, 30*time.Second)

		lck := &lock{conn: testutil.NewRedisClientWithMock(client), key: key, token: token}

		err = lck.Refresh(context.Background(), 2*time.Minute)

		require.NoError(t, err)

		newTTL := srv.TTL(key)
		assert.Greater(t, newTTL, 90*time.Second)
	})

	t.Run("wrong token returns lock already held", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})

		key := "test-refresh-wrong-token"
		err := srv.Set(key, "correct-token")
		require.NoError(t, err)

		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(client),
			key:   key,
			token: "wrong-token",
		}

		err = lck.Refresh(context.Background(), time.Minute)

		require.ErrorIs(t, err, ports.ErrLockAlreadyHeld)
	})

	t.Run("key not found returns lock already held", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})

		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(client),
			key:   "nonexistent-key",
			token: "some-token",
		}

		err := lck.Refresh(context.Background(), time.Minute)

		require.ErrorIs(t, err, ports.ErrLockAlreadyHeld)
	})

	t.Run("redis error is wrapped", func(t *testing.T) {
		t.Parallel()

		srv := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		lck := &lock{
			conn:  testutil.NewRedisClientWithMock(client),
			key:   "test-key",
			token: "test-token",
		}

		srv.Close()

		err := lck.Refresh(context.Background(), time.Minute)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "refresh lock")
	})
}

func TestLock_RefreshableLockInterface(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-refreshable")
	contextID := uuid.New()

	acquiredLock, err := lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)
	require.NoError(t, err)

	refreshable, ok := acquiredLock.(ports.RefreshableLock)
	require.True(t, ok, "lock should implement RefreshableLock")

	err = refreshable.Refresh(ctx, 2*time.Minute)

	require.NoError(t, err)

	err = refreshable.Release(ctx)

	require.NoError(t, err)
}

func TestLockManager_SetNXError(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	lm := NewLockManager(provider)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	contextID := uuid.New()

	srv.Close()

	_, err := lm.AcquireTransactionsLock(ctx, contextID, nil, time.Minute)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "acquire lock")
}
