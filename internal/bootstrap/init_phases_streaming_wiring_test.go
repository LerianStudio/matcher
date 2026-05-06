// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// These tests verify that streaming-emitter wiring is *present* in the
// bootstrap init_* source files via substring matching. They do NOT
// exercise runtime behavior — neither the disabled-flag NoopEmitter
// fallback nor the dependency injection itself is asserted here.
//
// For runtime verification of the disabled-flag NoopEmitter fallback, see
// internal/streaming/bootstrap/producer_test.go::TestNewEmitterDisabledReturnsNonNilNoopEmitter.
//
// Why source-substring tests instead of runtime construction tests? See the
// header comment in init_governance_streaming_wiring_test.go.
package bootstrap

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInitServerModulesSourceUsesLocalStreamingBundleBeforeStateAssignment
// is a source-substring wiring test (NOT a runtime test): it reads
// init_phases.go and asserts initArchivalComponents is called with the
// LOCAL streamingBundle.Emitter rather than the
// state.modules.streamingBundle.Emitter (which would be nil at the call
// site because the state is assigned later in the same function).
func TestInitServerModulesSourceUsesLocalStreamingBundleBeforeStateAssignment(t *testing.T) {
	source, err := os.ReadFile("init_phases.go")
	require.NoError(t, err)

	content := string(source)
	archivalStart := strings.Index(content, "archivalWorker, archivalErr := initArchivalComponents")
	require.NotEqual(t, -1, archivalStart)
	archivalCall := content[archivalStart:]
	archivalEnd := strings.Index(archivalCall, "if archivalErr != nil")
	require.NotEqual(t, -1, archivalEnd)
	archivalCall = archivalCall[:archivalEnd]

	require.Contains(t, archivalCall, "modules.streamingBundle.Emitter")
	require.NotContains(t, archivalCall, "state.modules.streamingBundle.Emitter")
}
