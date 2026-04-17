// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// End-to-end integration tests for the fetcher→matcher bridge worker (T-003).
// These scenarios wire the full production pipeline and drive it against:
//
//  1. A real Postgres (testcontainers via the shared integration harness) that
//     hosts the extraction_requests, fetcher_connections, reconciliation_sources,
//     ingestion_jobs, and transactions tables exactly as production would.
//  2. A real Redis (shared harness) used by the bridge worker's distributed
//     lock plus the ingestion pipeline's dedup service.
//  3. A real MinIO (testcontainers) that plays the role of Matcher's custody
//     bucket end-to-end — this is where the verified plaintext lands, and
//     from which the bridge reads it back for ingestion.
//  4. An httptest server that impersonates Fetcher's artifact endpoint,
//     serving real AES-256-GCM ciphertext with the contract-locked HMAC + IV
//     headers so the production verifier runs unmodified.
//
// The test file lives inside package `worker` (not `worker_test`) so it can
// drive the worker's unexported pollCycle helper — the same pattern the unit
// tests use. Build-tag isolation keeps the unit build clean.
//
// No testcontainers for Fetcher: Fetcher is a remote HTTP service and its
// artifact contract is fully captured by the httptest impersonator.
// Containerising Fetcher would add no signal and a lot of flakiness.
//
// This file hosts the happy-path scenario (IS-1). Default-tenant and
// distributed-lock scenarios live in sibling files to keep each under the
// 500-line Ring cap.
package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	discoveryVO "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	ingestionJobRepoPkg "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	"github.com/LerianStudio/matcher/tests/integration"
)

// -----------------------------------------------------------------------------
// IS-1: End-to-end bridge worker against real Postgres + Redis + httptest
// Fetcher + MinIO. The happy path drives the full retrieval → verify →
// custody → ingest → link pipeline and verifies every checkpoint
// (custody presence, ingestion job row, link persistence, custody
// delete-after-ingest, and replay idempotency).
// -----------------------------------------------------------------------------

func TestIntegration_BridgeWorker_HappyPath_EndToEnd_PersistsCustodyAndLinks(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		minIO := getBridgeMinIOHarness(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		tenantCtx := h.Ctx()

		seed := seedBridgeFixtures(t, tenantCtx, h)

		masterKey := randomBridgeMasterKey(t)
		plaintext := fetcherFlatPayload(t, "bridge-it-tx-001")
		ciphertext, ivHex, hmacHex := encryptBridgeArtifact(t, masterKey, plaintext)

		server := newBridgeFetcherImpersonator(ciphertext, hmacHex, ivHex)
		t.Cleanup(server.Close)

		worker, rawS3 := buildBridgeWorker(
			t, ctx, h, minIO, server, masterKey,
			[]string{auth.DefaultTenantID},
			seed,
		)

		beforeTick, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.False(t, beforeTick.Valid, "pre-tick extraction must be unlinked")

		// --- Tick 1: full pipeline runs ---
		worker.pollCycle(tenantCtx)

		// Assertion 1: the extraction now carries an ingestion_job_id, and
		// its status is still COMPLETE (linking does not change status).
		after1JobID, after1Status, after1Updated := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.True(t, after1JobID.Valid, "extraction must be linked after tick 1")
		assert.Equal(t, string(discoveryVO.ExtractionStatusComplete), after1Status,
			"extraction status unchanged by bridge linkage")
		assert.True(t, after1Updated.After(time.Now().Add(-60*time.Second)),
			"UpdatedAt must advance when linking")

		// Assertion 2: the ingestion_jobs row exists and reports the flat
		// JSON row count we supplied (1).
		provider := bridgeTestInfraProviderNoRedis(h)
		jobRepo := ingestionJobRepoPkg.NewRepository(provider)
		job, err := jobRepo.FindByID(tenantCtx, after1JobID.UUID)
		require.NoError(t, err, "ingestion job must be persisted")
		require.NotNil(t, job)
		assert.Equal(t, 1, job.Metadata.TotalRows,
			"one row should have flattened from the seed payload")

		// Assertion 3: the custody object under the tenant prefix has been
		// DELETED after successful ingestion (D2: delete-after-ingest).
		expectedKey := auth.DefaultTenantID + "/fetcher-artifacts/" + seed.extractionID.String() + ".json"
		exists, headErr := bridgeCustodyObjectExists(ctx, rawS3, minIO.bucket, expectedKey)
		require.NoError(t, headErr)
		assert.False(t, exists, "custody object must be gone after delete-after-ingest")

		// --- Tick 2: idempotent replay ---
		// The second tick must not produce a second ingestion job. The
		// extraction is already linked so FindEligibleForBridge yields
		// zero rows.
		worker.pollCycle(tenantCtx)

		after2JobID, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		assert.Equal(t, after1JobID.UUID, after2JobID.UUID,
			"link must not change on replay")

		// Count ingestion_jobs owned by this test to prove no duplicate
		// bridge run spawned a second ingestion.
		resolver, rErr := h.Connection.Resolver(tenantCtx)
		require.NoError(t, rErr)
		var jobCount int
		err = resolver.QueryRowContext(tenantCtx,
			`SELECT COUNT(*) FROM ingestion_jobs WHERE source_id = $1`,
			seed.sourceID).Scan(&jobCount)
		require.NoError(t, err)
		assert.Equal(t, 1, jobCount,
			"replay must not create a duplicate ingestion job")
	})
}
