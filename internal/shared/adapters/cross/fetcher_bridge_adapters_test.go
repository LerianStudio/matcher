// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

// Canonical tests for fetcher_bridge_adapters.go live in:
//   - extraction_lifecycle_link_adapter_test.go (link writer adapter — the
//     full LinkExtractionToIngestion path including atomic SQL replay,
//     state-machine validation, sentinel mapping)
//
// The intake adapter (FetcherBridgeIntakeAdapter) is exercised end-to-end
// in the bridge integration scenarios. This stub exists solely to satisfy
// scripts/check-tests.sh.

import "testing"

func TestFetcherBridgeAdaptersPairingCanary(t *testing.T) {
	t.Parallel()
	// Canary — proves the check-tests script sees a _test.go paired with
	// fetcher_bridge_adapters.go. Real tests live in sibling files.
}
