//go:build e2e

package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndLoadFuzzArtifact_RoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	artifact := &FuzzRunArtifact{
		RunID:     "test-run-001",
		Timestamp: "2026-02-11T10:00:00Z",
		Config: FuzzConfig{
			ClaudeOracleEnabled: false,
			ClaudeModel:         "test-model",
			ScenarioCount:       5,
			ArtifactDir:         dir,
		},
		Scenarios: []FuzzScenario{
			{
				ID:           "s001",
				Source:       "deterministic",
				Category:     "rounding",
				AttackVector: "tie-breaker test",
				GrossAmount:  "100.00",
				Schedule: FuzzScheduleSpec{
					Name:             "Test",
					Currency:         "USD",
					ApplicationOrder: "PARALLEL",
					RoundingScale:    2,
					RoundingMode:     "HALF_UP",
					Items: []FuzzItemSpec{
						{Name: "flat", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "5.00"}},
					},
				},
				Verdict: "pass",
			},
		},
		Summary: FuzzSummary{Total: 1, Passed: 1},
	}

	writtenPath, err := WriteFuzzArtifact(artifact, dir)
	require.NoError(t, err)
	assert.FileExists(t, writtenPath)
	assert.Contains(t, writtenPath, "test-run-001")

	loaded, err := LoadFuzzArtifact(writtenPath)
	require.NoError(t, err)
	assert.Equal(t, artifact.RunID, loaded.RunID)
	assert.Equal(t, artifact.Timestamp, loaded.Timestamp)
	assert.Equal(t, artifact.Config.ScenarioCount, loaded.Config.ScenarioCount)
	require.Len(t, loaded.Scenarios, 1)
	assert.Equal(t, "s001", loaded.Scenarios[0].ID)
	assert.Equal(t, "tie-breaker test", loaded.Scenarios[0].AttackVector)
	assert.Equal(t, 1, loaded.Summary.Passed)
}

func TestWriteFailureArtifact(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	scenario := &FuzzScenario{
		ID:           "fail-001",
		Source:       "deterministic",
		Category:     "tiered_boundary",
		AttackVector: "off-by-one at tier boundary",
		GrossAmount:  "1000.01",
		Verdict:      "fail",
		Discrepancies: []FuzzDiscrepancy{
			{Field: "totalFee", Expected: "50.00", Actual: "49.99", Delta: "0.01"},
		},
	}

	writtenPath, err := WriteFailureArtifact(scenario, dir)
	require.NoError(t, err)
	assert.FileExists(t, writtenPath)
	assert.Contains(t, filepath.Base(writtenPath), "fail_fail-001")
	assert.Contains(t, filepath.Dir(writtenPath), "failures")

	data, err := os.ReadFile(writtenPath)
	require.NoError(t, err)

	var loaded FuzzScenario

	err = json.Unmarshal(data, &loaded)
	require.NoError(t, err)
	assert.Equal(t, "fail-001", loaded.ID)
	assert.Equal(t, "fail", loaded.Verdict)
	require.Len(t, loaded.Discrepancies, 1)
	assert.Equal(t, "0.01", loaded.Discrepancies[0].Delta)
}

func TestLoadFuzzArtifact_FileNotFound(t *testing.T) {
	t.Parallel()

	_, err := LoadFuzzArtifact("/nonexistent/path/artifact.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read fuzz artifact")
}

func TestLoadFuzzScenarios_FromArtifact(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.json")

	artifact := FuzzRunArtifact{
		RunID: "run-123",
		Scenarios: []FuzzScenario{
			{ID: "s1", AttackVector: "test1"},
			{ID: "s2", AttackVector: "test2"},
		},
	}

	data, err := json.Marshal(artifact)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, fuzzFilePermissions))

	scenarios, err := LoadFuzzScenarios(path)
	require.NoError(t, err)
	assert.Len(t, scenarios, 2)
	assert.Equal(t, "s1", scenarios[0].ID)
	assert.Equal(t, "s2", scenarios[1].ID)
}

func TestLoadFuzzScenarios_FromArray(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "scenarios.json")

	scenarios := []FuzzScenario{
		{ID: "a1", AttackVector: "array-test-1"},
		{ID: "a2", AttackVector: "array-test-2"},
		{ID: "a3", AttackVector: "array-test-3"},
	}

	data, err := json.Marshal(scenarios)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, fuzzFilePermissions))

	loaded, err := LoadFuzzScenarios(path)
	require.NoError(t, err)
	assert.Len(t, loaded, 3)
	assert.Equal(t, "a1", loaded[0].ID)
}

func TestLoadFuzzScenarios_FromSingle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "single.json")

	scenario := FuzzScenario{ID: "single-1", AttackVector: "solo test"}

	data, err := json.Marshal(scenario)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, fuzzFilePermissions))

	loaded, err := LoadFuzzScenarios(path)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, "single-1", loaded[0].ID)
}

func TestLoadFuzzScenarios_UnrecognizedFormat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.json")

	require.NoError(t, os.WriteFile(path, []byte(`{"foo": "bar"}`), fuzzFilePermissions))

	_, err := LoadFuzzScenarios(path)
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnrecognizedFormat)
}

func TestSanitizeFilename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "simple alphanumeric passthrough",
			input:    "hello_world",
			maxLen:   50,
			expected: "hello_world",
		},
		{
			name:     "special characters replaced",
			input:    "off-by-one at tier!boundary",
			maxLen:   50,
			expected: "off_by_one_at_tier_boundary",
		},
		{
			name:     "truncation applied",
			input:    "this-is-a-very-long-name-that-exceeds-limit",
			maxLen:   10,
			expected: "this_is_a_",
		},
		{
			name:     "leading trailing underscores trimmed",
			input:    "!!!hello!!!",
			maxLen:   50,
			expected: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := sanitizeFilename(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
		})
	}
}
