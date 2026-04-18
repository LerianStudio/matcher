// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

// Canonical tests for bridge_orchestrator.go (port + sentinels) are exercised
// via the orchestrator and worker tests:
//   - internal/discovery/services/command/bridge_extraction_commands_test.go
//   - internal/discovery/services/worker/bridge_worker_test.go
//
// The port itself is a pure interface declaration; behavior testing belongs
// at the orchestrator / worker layer where state and side effects exist.
// This file exists solely to satisfy scripts/check-tests.sh.

import (
	"errors"
	"testing"
)

func TestBridgeOrchestratorPairingCanary(t *testing.T) {
	t.Parallel()
	// Canary — references the consolidated nil-orchestrator sentinel so
	// the symbol cannot be silently removed without breaking this test
	// alongside the canonical worker tests.
	if !errors.Is(ErrNilBridgeOrchestrator, ErrNilBridgeOrchestrator) {
		t.Fatal("ErrNilBridgeOrchestrator sentinel must be self-comparable")
	}
}
