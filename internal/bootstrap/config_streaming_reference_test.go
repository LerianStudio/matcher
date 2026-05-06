// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"os"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var streamingEnvKeys = []string{
	"STREAMING_ENABLED",
	"STREAMING_BROKERS",
	"STREAMING_CLIENT_ID",
	"STREAMING_CLOUDEVENTS_SOURCE",
	"STREAMING_EVENT_POLICIES",
	"STREAMING_COMPRESSION",
	"STREAMING_REQUIRED_ACKS",
}

func TestConfigMapDocumentsStreamingEnvKeys(t *testing.T) {
	raw, err := os.ReadFile("../../config/.config-map.example")
	require.NoError(t, err)

	content := string(raw)
	for _, key := range []string{
		"STREAMING_ENABLED",
		"STREAMING_BROKERS",
		"STREAMING_CLIENT_ID",
		"STREAMING_CLOUDEVENTS_SOURCE",
		"STREAMING_EVENT_POLICIES",
	} {
		require.Contains(t, content, key)
	}
}

func TestStreamingLoadConfigDisabledIgnoresInvalidPolicies(t *testing.T) {
	clearStreamingEnv(t)
	t.Setenv("STREAMING_ENABLED", "false")
	t.Setenv("STREAMING_EVENT_POLICIES", "malformed")

	cfg, _, err := streaming.LoadConfig()

	require.NoError(t, err)
	assert.False(t, cfg.Enabled)
	assert.Empty(t, cfg.PolicyOverrides)
}

func TestStreamingLoadConfigEnabledParsesRequiredSettings(t *testing.T) {
	clearStreamingEnv(t)
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "redpanda:9092, redpanda-2:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "matcher")
	t.Setenv("STREAMING_EVENT_POLICIES", "reconciliation_context.created.outbox=never")

	cfg, _, err := streaming.LoadConfig()

	require.NoError(t, err)
	assert.True(t, cfg.Enabled)
	assert.Equal(t, []string{"redpanda:9092", "redpanda-2:9092"}, cfg.Brokers)
	assert.Contains(t, cfg.PolicyOverrides, "reconciliation_context.created")
}

func TestStreamingLoadConfigEnabledDefaultsEmptyBrokerEnv(t *testing.T) {
	clearStreamingEnv(t)
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", " ")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "matcher")

	cfg, _, err := streaming.LoadConfig()

	require.NoError(t, err)
	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
}

func TestStreamingLoadConfigEnabledRejectsInvalidPolicies(t *testing.T) {
	clearStreamingEnv(t)
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "redpanda:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "matcher")
	t.Setenv("STREAMING_EVENT_POLICIES", "reconciliation_context.created.unknown=value")

	_, _, err := streaming.LoadConfig()

	assert.ErrorIs(t, err, streaming.ErrInvalidDeliveryPolicy)
}

func clearStreamingEnv(t *testing.T) {
	t.Helper()

	for _, key := range streamingEnvKeys {
		t.Setenv(key, "")
	}
}
