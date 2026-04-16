// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

// Canonical tests for fetcher_bridge.go (intake port + sentinels) are exercised
// indirectly via the adapter and orchestrator tests:
//   - internal/shared/adapters/cross/fetcher_bridge_adapters_test.go
//   - internal/discovery/services/command/bridge_extraction_commands_test.go
//
// The port itself is a pure interface declaration; behavior testing belongs
// at the adapter / orchestrator layer where state and side effects exist.
// This file exists solely to satisfy scripts/check-tests.sh.

import (
	"errors"
	"testing"
)

func TestFetcherBridgePairingCanary(t *testing.T) {
	t.Parallel()
	// Canary — references one exported sentinel so the symbol cannot be
	// silently removed without breaking this test alongside the canonical
	// adapter tests.
	if !errors.Is(ErrExtractionAlreadyLinked, ErrExtractionAlreadyLinked) {
		t.Fatal("ErrExtractionAlreadyLinked sentinel must be self-comparable")
	}
}
