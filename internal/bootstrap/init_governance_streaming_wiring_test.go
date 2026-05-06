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
// Why source-substring tests instead of runtime construction tests? The
// init_* helpers depend on a fully-stubbed Service value with PostgreSQL,
// Redis, RabbitMQ, S3, telemetry, and tenant-manager wiring — about 200
// lines of stub plumbing per init helper. The failure mode these wiring
// tests guard against (a code-author silently removing
// `WithStreamingEmitter` from a constructor call) is a pure source-text
// regression that string-matching catches at near-zero cost. A future
// refactor that converts a single init_* helper to a fully-stubbed
// runtime construction test would be welcome but is not gating any
// reviewer-facing risk today.
package bootstrap

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInitGovernanceModuleSourceWiresActorMappingStreamingEmitter is a
// source-substring wiring test (NOT a runtime test): it reads
// init_governance.go and asserts the actor-mapping use case is constructed
// with both WithActorMappingInfrastructure and
// WithActorMappingStreamingEmitter.
func TestInitGovernanceModuleSourceWiresActorMappingStreamingEmitter(t *testing.T) {
	source, err := os.ReadFile("init_governance.go")
	require.NoError(t, err)

	content := string(source)
	constructorStart := strings.Index(content, "governanceCommand.NewActorMappingUseCase")
	require.NotEqual(t, -1, constructorStart)
	constructorCall := content[constructorStart:]
	constructorEnd := strings.Index(constructorCall, ")\n\tif err != nil")
	require.NotEqual(t, -1, constructorEnd)
	constructorCall = constructorCall[:constructorEnd]

	require.Contains(t, constructorCall, "governanceCommand.WithActorMappingInfrastructure(provider)")
	require.Contains(t, constructorCall, "governanceCommand.WithActorMappingStreamingEmitter(streamEmitter)")
}
