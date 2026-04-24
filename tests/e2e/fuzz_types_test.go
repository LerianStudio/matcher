//go:build e2e

package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFuzzConfig_Defaults(t *testing.T) { //nolint:paralleltest // env var mutation
	envVars := []string{
		"E2E_CLAUDE_ORACLE",
		"E2E_CLAUDE_MODEL",
		"E2E_FUZZ_SCENARIOS",
		"E2E_FUZZ_REPLAY",
		"E2E_FUZZ_ARTIFACT_DIR",
		"E2E_FUZZ_VERBOSE",
	}

	// Clear each env var for the scope of this test. Consumers treat empty
	// string as "unset" (see getEnv / getIntEnvFuzz). t.Setenv restores the
	// prior value automatically when the test ends.
	for _, key := range envVars {
		t.Setenv(key, "")
	}

	cfg := LoadFuzzConfig()

	require.NotNil(t, cfg)
	assert.False(t, cfg.ClaudeOracleEnabled)
	assert.Equal(t, "claude-sonnet-4-20250514", cfg.ClaudeModel)
	assert.Equal(t, defaultFuzzScenarioCount, cfg.ScenarioCount)
	assert.Empty(t, cfg.ReplayPath)
	assert.Equal(t, "fuzz_artifacts", cfg.ArtifactDir)
	assert.False(t, cfg.Verbose)
}

func TestLoadFuzzConfig_WithOverrides(t *testing.T) { //nolint:paralleltest // env var mutation
	overrides := map[string]string{
		"E2E_CLAUDE_ORACLE":     "true",
		"E2E_CLAUDE_MODEL":      "claude-opus-4-20250514",
		"E2E_FUZZ_SCENARIOS":    "50",
		"E2E_FUZZ_REPLAY":       "/tmp/replay.json",
		"E2E_FUZZ_ARTIFACT_DIR": "/tmp/artifacts",
		"E2E_FUZZ_VERBOSE":      "TRUE",
	}

	for key, val := range overrides {
		t.Setenv(key, val)
	}

	cfg := LoadFuzzConfig()

	require.NotNil(t, cfg)
	assert.True(t, cfg.ClaudeOracleEnabled)
	assert.Equal(t, "claude-opus-4-20250514", cfg.ClaudeModel)
	assert.Equal(t, 50, cfg.ScenarioCount)
	assert.Equal(t, "/tmp/replay.json", cfg.ReplayPath)
	assert.Equal(t, "/tmp/artifacts", cfg.ArtifactDir)
	assert.True(t, cfg.Verbose)
}

func TestLoadFuzzConfig_InvalidScenarioCount_FallsBackToDefault(t *testing.T) {
	t.Setenv("E2E_FUZZ_SCENARIOS", "not-a-number")

	cfg := LoadFuzzConfig()
	assert.Equal(t, defaultFuzzScenarioCount, cfg.ScenarioCount)
}

func TestGetIntEnvFuzz_ReturnsDefault_WhenUnset(t *testing.T) {
	// Empty string is treated as unset by getIntEnvFuzz (val != "" guard).
	t.Setenv("TEST_INT_FUZZ_UNSET", "")

	result := getIntEnvFuzz("TEST_INT_FUZZ_UNSET", 42)
	assert.Equal(t, 42, result)
}

func TestGetIntEnvFuzz_ParsesValidInt(t *testing.T) {
	t.Setenv("TEST_INT_FUZZ_VALID", "99")

	result := getIntEnvFuzz("TEST_INT_FUZZ_VALID", 42)
	assert.Equal(t, 99, result)
}

func TestGetIntEnvFuzz_ReturnsDefault_OnInvalidValue(t *testing.T) {
	t.Setenv("TEST_INT_FUZZ_INVALID", "abc")

	result := getIntEnvFuzz("TEST_INT_FUZZ_INVALID", 42)
	assert.Equal(t, 42, result)
}

func TestFuzzScheduleSpec_ToCreateRequest(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		Name:             "Test Schedule",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []FuzzItemSpec{
			{
				Name:          "flat-fee",
				Priority:      1,
				StructureType: "FLAT",
				Structure:     map[string]any{"amount": "5.00"},
			},
			{
				Name:          "pct-fee",
				Priority:      2,
				StructureType: "PERCENTAGE",
				Structure:     map[string]any{"rate": "0.015"},
			},
		},
	}

	req := spec.ToCreateRequest()

	assert.Equal(t, "Test Schedule", req.Name)
	assert.Equal(t, "USD", req.Currency)
	assert.Equal(t, "PARALLEL", req.ApplicationOrder)
	assert.Equal(t, 2, req.RoundingScale)
	assert.Equal(t, "HALF_UP", req.RoundingMode)
	require.Len(t, req.Items, 2)

	assert.Equal(t, "flat-fee", req.Items[0].Name)
	assert.Equal(t, 1, req.Items[0].Priority)
	assert.Equal(t, "FLAT", req.Items[0].StructureType)
	assert.Equal(t, "5.00", req.Items[0].Structure["amount"])

	assert.Equal(t, "pct-fee", req.Items[1].Name)
	assert.Equal(t, 2, req.Items[1].Priority)
	assert.Equal(t, "PERCENTAGE", req.Items[1].StructureType)
	assert.Equal(t, "0.015", req.Items[1].Structure["rate"])
}

func TestFuzzScheduleSpec_ToCreateRequest_EmptyItems(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		Name:             "Empty Schedule",
		Currency:         "BRL",
		ApplicationOrder: "CASCADING",
		RoundingScale:    4,
		RoundingMode:     "BANKERS",
		Items:            []FuzzItemSpec{},
	}

	req := spec.ToCreateRequest()

	assert.Equal(t, "Empty Schedule", req.Name)
	assert.Equal(t, "BRL", req.Currency)
	assert.Empty(t, req.Items)
}
