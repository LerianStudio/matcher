// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/tests/integration"
)

// -----------------------------------------------------------------------------
// IS-3: Distributed lock prevents concurrent double-processing. Two
// goroutines drive pollCycle simultaneously; the Redis SetNX guard in
// BridgeWorker.acquireLock must admit only one of them into the critical
// section, and the atomic LinkIfUnlinked write guarantees that even if
// both goroutines happened to race past the lock they still produce at
// most one linked extraction.
// -----------------------------------------------------------------------------

func TestIntegration_BridgeWorker_DistributedLock_PreventsConcurrentBridge(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		minIO := getBridgeMinIOHarness(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		tenantCtx := h.Ctx()

		seed := seedBridgeFixtures(t, tenantCtx, h)

		masterKey := randomBridgeMasterKey(t)
		plaintext := fetcherFlatPayload(t, "bridge-lock-tx-001")
		ciphertext, ivHex, hmacHex := encryptBridgeArtifact(t, masterKey, plaintext)

		server := newBridgeFetcherImpersonator(ciphertext, hmacHex, ivHex)
		t.Cleanup(server.Close)

		worker, _ := buildBridgeWorker(
			t, ctx, h, minIO, server, masterKey,
			[]string{auth.DefaultTenantID},
			seed,
		)

		// Run two pollCycle invocations concurrently. Whichever goroutine
		// wins the lock does the full work; the other acquires the lock and
		// finds the extraction already linked, or is rejected by SetNX
		// outright. Either outcome is acceptable — the invariant is that
		// exactly one ingestion job exists at the end.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			worker.pollCycle(tenantCtx)
		}()
		go func() {
			defer wg.Done()
			worker.pollCycle(tenantCtx)
		}()
		wg.Wait()

		// Invariant: exactly one ingestion_jobs row for our seeded source.
		resolver, rErr := h.Connection.Resolver(tenantCtx)
		require.NoError(t, rErr)
		var jobCount int
		err := resolver.QueryRowContext(tenantCtx,
			`SELECT COUNT(*) FROM ingestion_jobs WHERE source_id = $1`,
			seed.sourceID).Scan(&jobCount)
		require.NoError(t, err)
		assert.Equal(t, 1, jobCount,
			"concurrent pollCycle calls must produce exactly one ingestion job")

		linkedJobID, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.True(t, linkedJobID.Valid,
			"extraction must be linked after at least one pollCycle completed")
	})
}
