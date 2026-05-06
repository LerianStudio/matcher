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

// TestInitModulesSourceWiresAuditConsumerStreamingEmitter is a
// source-substring wiring test (NOT a runtime test): it reads
// init_modules.go and asserts NewConsumer's ConsumerConfig literal carries
// both Infrastructure and StreamingEmitter fields.
func TestInitModulesSourceWiresAuditConsumerStreamingEmitter(t *testing.T) {
	source, err := os.ReadFile("init_modules.go")
	require.NoError(t, err)

	content := string(source)
	constructorStart := strings.Index(content, "governanceAudit.NewConsumer")
	require.NotEqual(t, -1, constructorStart)
	constructorCall := content[constructorStart:]
	constructorEnd := strings.Index(constructorCall, ")\n\tif err != nil")
	require.NotEqual(t, -1, constructorEnd)
	constructorCall = constructorCall[:constructorEnd]

	require.Contains(t, constructorCall, "Infrastructure:")
	require.Contains(t, constructorCall, "provider")
	require.Contains(t, constructorCall, "StreamingEmitter:")
	require.Contains(t, constructorCall, "streamEmitter")
}

// TestInitModulesSourcePassesTelemetryMetricsFactoryToStreamingProducer is a
// source-substring wiring test (NOT a runtime test) verifying the streaming
// producer is constructed with the telemetry MetricsFactory rather than a
// nil factory.
func TestInitModulesSourcePassesTelemetryMetricsFactoryToStreamingProducer(t *testing.T) {
	source, err := os.ReadFile("init_modules.go")
	require.NoError(t, err)

	content := string(source)
	require.Contains(t, content, "producerOptions.MetricsFactory = telemetry.MetricsFactory")
}
