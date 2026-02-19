//go:build integration

package ingestion

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	ingestionRedis "github.com/LerianStudio/matcher/internal/ingestion/adapters/redis"
	ingestionPorts "github.com/LerianStudio/matcher/internal/ingestion/ports"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/tests/integration"
)

// mustDedupeRedisConn creates a raw redis.Client backed by the testcontainer Redis instance.
func mustDedupeRedisConn(t *testing.T, redisAddr string) *redis.Client {
	t.Helper()

	parsed, err := url.Parse(strings.TrimSpace(redisAddr))
	require.NoError(t, err)
	require.NotEmpty(t, parsed.Host)

	client := redis.NewClient(&redis.Options{Addr: parsed.Host})

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("redis cleanup: %v (expected in test teardown)", err)
		}
	})

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, client.Ping(pingCtx).Err())

	return client
}

// newDedupeService creates a DedupeService wired to the testcontainer Redis and (optionally) Postgres.
func newDedupeService(t *testing.T, h *integration.TestHarness) *ingestionRedis.DedupeService {
	t.Helper()

	rawClient := mustDedupeRedisConn(t, h.RedisAddr)
	libClient := infraTestutil.NewRedisClientWithMock(rawClient)
	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(h.Connection, libClient)

	return ingestionRedis.NewDedupeService(provider)
}

func TestDedupe_CalculateHash_Deterministic(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		dedupe := newDedupeService(t, h)

		sourceID := uuid.New()
		externalID := "txn-abc-123"

		// Same inputs must produce the same hash (deterministic).
		hash1 := dedupe.CalculateHash(sourceID, externalID)
		hash2 := dedupe.CalculateHash(sourceID, externalID)
		require.Equal(t, hash1, hash2, "same inputs must yield identical hashes")
		require.NotEmpty(t, hash1)

		// Different sourceID must produce a different hash.
		otherSource := uuid.New()
		hash3 := dedupe.CalculateHash(otherSource, externalID)
		require.NotEqual(t, hash1, hash3, "different sourceID must yield different hash")

		// Different externalID must produce a different hash.
		hash4 := dedupe.CalculateHash(sourceID, "txn-xyz-999")
		require.NotEqual(t, hash1, hash4, "different externalID must yield different hash")
	})
}

func TestDedupe_MarkSeenAndIsDuplicate(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		dedupe := newDedupeService(t, h)

		ctx := context.Background()
		contextID := uuid.New()
		hash := dedupe.CalculateHash(uuid.New(), "mark-seen-round-trip")

		// Mark the hash as seen with a generous TTL.
		err := dedupe.MarkSeen(ctx, contextID, hash, 30*time.Second)
		require.NoError(t, err)

		// IsDuplicate must now report true.
		dup, err := dedupe.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		require.True(t, dup, "hash should be duplicate after MarkSeen")

		// Clear the hash.
		err = dedupe.Clear(ctx, contextID, hash)
		require.NoError(t, err)

		// IsDuplicate must now report false.
		dup, err = dedupe.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		require.False(t, dup, "hash should not be duplicate after Clear")
	})
}

func TestDedupe_IsDuplicate_NotSeen(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		dedupe := newDedupeService(t, h)

		ctx := context.Background()
		contextID := uuid.New()
		hash := dedupe.CalculateHash(uuid.New(), "never-seen-before")

		dup, err := dedupe.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		require.False(t, dup, "fresh hash must not be reported as duplicate")
	})
}

func TestDedupe_MarkSeenWithRetry_DuplicateReject(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		dedupe := newDedupeService(t, h)

		ctx := context.Background()
		contextID := uuid.New()
		hash := dedupe.CalculateHash(uuid.New(), "retry-dup-reject")

		// First call: MarkSeen with plain Set — key now exists.
		err := dedupe.MarkSeen(ctx, contextID, hash, 30*time.Second)
		require.NoError(t, err)

		// Second call: MarkSeenWithRetry uses SetNX — key already exists → ErrDuplicateTransaction.
		err = dedupe.MarkSeenWithRetry(ctx, contextID, hash, 30*time.Second, 3)
		require.Error(t, err)
		require.True(t,
			errors.Is(err, ingestionPorts.ErrDuplicateTransaction),
			"expected ErrDuplicateTransaction, got: %v", err,
		)
	})
}

func TestDedupe_TTLExpiry(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		dedupe := newDedupeService(t, h)

		ctx := context.Background()
		contextID := uuid.New()
		hash := dedupe.CalculateHash(uuid.New(), "ttl-expiry")

		// Mark with a 1-second TTL.
		err := dedupe.MarkSeen(ctx, contextID, hash, 1*time.Second)
		require.NoError(t, err)

		// Immediately should be duplicate.
		dup, err := dedupe.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		require.True(t, dup, "hash should be duplicate immediately after MarkSeen")

		deadline := time.Now().Add(5 * time.Second)
		expired := false

		for time.Now().Before(deadline) {
			dup, err = dedupe.IsDuplicate(ctx, contextID, hash)
			require.NoError(t, err)

			if !dup {
				expired = true
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		require.True(t, expired, "hash should not be duplicate after TTL expiry")
	})
}

func TestDedupe_ClearBatch(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		dedupe := newDedupeService(t, h)

		ctx := context.Background()
		contextID := uuid.New()

		hashes := make([]string, 3)
		for i := range hashes {
			hashes[i] = dedupe.CalculateHash(uuid.New(), uuid.NewString())
		}

		// Mark all 3 hashes.
		for _, hash := range hashes {
			err := dedupe.MarkSeen(ctx, contextID, hash, 30*time.Second)
			require.NoError(t, err)
		}

		// Verify all are duplicates.
		for _, hash := range hashes {
			dup, err := dedupe.IsDuplicate(ctx, contextID, hash)
			require.NoError(t, err)
			require.True(t, dup, "hash %s should be duplicate after MarkSeen", hash)
		}

		// ClearBatch all 3.
		err := dedupe.ClearBatch(ctx, contextID, hashes)
		require.NoError(t, err)

		// Verify none are duplicates.
		for _, hash := range hashes {
			dup, err := dedupe.IsDuplicate(ctx, contextID, hash)
			require.NoError(t, err)
			require.False(t, dup, "hash %s should not be duplicate after ClearBatch", hash)
		}
	})
}
