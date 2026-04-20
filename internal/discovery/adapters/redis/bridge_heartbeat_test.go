// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package redis

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupHeartbeatRedis spins up an in-process miniredis and returns a
// connected client plus a cleanup hook. Mirrors setupRedis in
// schema_cache_test.go so the two Redis adapters share test-harness style.
func setupHeartbeatRedis(t *testing.T) (*goredis.Client, *miniredis.Miniredis, func()) {
	t.Helper()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})

	cleanup := func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close redis client: %v", err)
		}

		srv.Close()
	}

	return client, srv, cleanup
}

func TestNewBridgeHeartbeat(t *testing.T) {
	t.Parallel()

	t.Run("with valid client", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)
		require.NotNil(t, hb)
	})

	t.Run("with nil client", func(t *testing.T) {
		t.Parallel()

		hb, err := NewBridgeHeartbeat(nil)
		assert.ErrorIs(t, err, ErrNilRedisClient)
		assert.Nil(t, hb)
	})
}

func TestBridgeHeartbeat_WriteLastTickAt(t *testing.T) {
	t.Parallel()

	t.Run("successful write stores unix seconds with ttl", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		ctx := context.Background()
		now := time.Date(2026, time.April, 17, 12, 0, 0, 0, time.UTC)
		ttl := 90 * time.Second

		require.NoError(t, hb.WriteLastTickAt(ctx, now, ttl))

		// Value must be the exact Unix-seconds string — the reader parses
		// via strconv.ParseInt so any other encoding would be surfaced as
		// corruption on the read path.
		assert.True(t, srv.Exists(bridgeHeartbeatKey))

		stored, err := client.Get(ctx, bridgeHeartbeatKey).Result()
		require.NoError(t, err)
		assert.Equal(t, strconv.FormatInt(now.Unix(), 10), stored)
	})

	t.Run("zero ttl rejected with sentinel", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		err = hb.WriteLastTickAt(context.Background(), time.Now().UTC(), 0)
		require.ErrorIs(t, err, ErrHeartbeatTTLInvalid)
		assert.False(t, srv.Exists(bridgeHeartbeatKey),
			"invalid-ttl write must not touch the key")
	})

	t.Run("negative ttl rejected with sentinel", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		err = hb.WriteLastTickAt(context.Background(), time.Now().UTC(), -time.Second)
		require.ErrorIs(t, err, ErrHeartbeatTTLInvalid)
		assert.False(t, srv.Exists(bridgeHeartbeatKey),
			"invalid-ttl write must not touch the key")
	})

	t.Run("nil receiver returns sentinel", func(t *testing.T) {
		t.Parallel()

		var hb *BridgeHeartbeat

		err := hb.WriteLastTickAt(context.Background(), time.Now().UTC(), time.Minute)
		assert.ErrorIs(t, err, ErrNilRedisClient)
	})

	t.Run("non-utc timestamp coerced to utc unix seconds", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		// Use a fixed offset zone so the test is deterministic regardless
		// of the host's local TZ. The wall-clock instant is the same; only
		// the zone differs. The stored value must equal the UTC Unix time,
		// not the local-clock Unix time — both happen to be equal by
		// definition, but this pins the UTC() call so it can't be
		// inadvertently removed.
		loc := time.FixedZone("test-zone", 3*60*60) // UTC+3
		now := time.Date(2026, time.April, 17, 15, 0, 0, 0, loc)

		require.NoError(t, hb.WriteLastTickAt(context.Background(), now, time.Minute))

		got, err := hb.ReadLastTickAt(context.Background())
		require.NoError(t, err)
		assert.True(t, got.Equal(now), "instant must round-trip regardless of source timezone")
		assert.Equal(t, time.UTC, got.Location(), "reader must surface UTC per adapter contract")
	})
}

func TestBridgeHeartbeat_ReadLastTickAt(t *testing.T) {
	t.Parallel()

	t.Run("missing key returns zero time no error", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		got, err := hb.ReadLastTickAt(context.Background())
		require.NoError(t, err)
		assert.True(t, got.IsZero(),
			"missing key must surface zero-time without error so dashboards don't flag empty state")
	})

	t.Run("round-trip via writer hits the same key", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		now := time.Date(2026, time.April, 17, 12, 0, 0, 0, time.UTC)
		require.NoError(t, hb.WriteLastTickAt(context.Background(), now, time.Minute))

		got, err := hb.ReadLastTickAt(context.Background())
		require.NoError(t, err)
		assert.True(t, got.Equal(now))
	})

	t.Run("non-integer payload surfaces parse error", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		// Seed a corrupt value directly. A legitimate writer never produces
		// this, so surfacing it as an error lets operators notice a
		// manually-tampered key on the next dashboard poll.
		ctx := context.Background()
		require.NoError(t, client.Set(ctx, bridgeHeartbeatKey, "not-an-int", time.Minute).Err())

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		got, err := hb.ReadLastTickAt(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse heartbeat unix seconds")
		assert.True(t, got.IsZero())
	})

	t.Run("nil receiver returns zero time no error", func(t *testing.T) {
		t.Parallel()

		var hb *BridgeHeartbeat

		got, err := hb.ReadLastTickAt(context.Background())
		require.NoError(t, err,
			"nil receiver must be a no-op so a Fetcher-disabled deployment degrades silently")
		assert.True(t, got.IsZero())
	})

	t.Run("expired key behaves as missing", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupHeartbeatRedis(t)
		defer cleanup()

		hb, err := NewBridgeHeartbeat(client)
		require.NoError(t, err)

		require.NoError(t, hb.WriteLastTickAt(context.Background(), time.Now().UTC(), time.Second))

		// miniredis supports TTL fast-forward; advance past the TTL to
		// simulate a worker that stopped ticking.
		srv.FastForward(2 * time.Second)

		got, err := hb.ReadLastTickAt(context.Background())
		require.NoError(t, err, "expired key must degrade to the missing-key contract")
		assert.True(t, got.IsZero())
	})
}
