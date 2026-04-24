// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/tests/integration"
)

// -----------------------------------------------------------------------------
// IS-2: Default-tenant inclusion (AC-T2). The shared integration harness
// seeds everything in the public/default schema. This scenario verifies
// that when the bridge worker's TenantLister reports DefaultTenantID, the
// eligible extraction in public still gets processed end-to-end.
// -----------------------------------------------------------------------------

func TestIntegration_BridgeWorker_DefaultTenant_IsProcessed(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		minIO := getBridgeMinIOHarness(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		tenantCtx := h.Ctx()

		seed := seedBridgeFixtures(t, tenantCtx, h)

		masterKey := randomBridgeMasterKey(t)
		plaintext := fetcherFlatPayload(t, "bridge-default-tenant-tx-001")
		ciphertext, ivHex, hmacHex := encryptBridgeArtifact(t, masterKey, plaintext)

		server := newBridgeFetcherImpersonator(ciphertext, hmacHex, ivHex)
		t.Cleanup(server.Close)

		worker, _ := buildBridgeWorker(
			t, ctx, h, minIO, server, masterKey,
			[]string{auth.DefaultTenantID},
			seed,
		)

		// Sanity check: the tenant id we are driving IS the default tenant.
		// If the harness ever starts seeding against a non-default tenant
		// this assertion fails loudly rather than silently testing the
		// wrong code path.
		require.Equal(t, auth.DefaultTenantID, h.Seed.TenantID.String(),
			"shared harness must seed against DefaultTenantID; otherwise the default-tenant path is not what this test exercises")

		worker.pollCycle(tenantCtx)

		linkedJobID, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.True(t, linkedJobID.Valid,
			"extraction seeded in the default (public) tenant must be bridged end-to-end")
	})
}
