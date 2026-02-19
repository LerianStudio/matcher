//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFuzzLogger_CreatesNonNil(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, false)
	require.NotNil(t, logger)
	assert.Empty(t, logger.Lines())
}

func TestNewFuzzLogger_Verbose(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, true)
	require.NotNil(t, logger)
	assert.True(t, logger.verbose)
}

func TestFuzzLogger_LogHeader(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, false)

	cfg := &FuzzConfig{
		ClaudeOracleEnabled: true,
		ScenarioCount:       30,
		ClaudeModel:         "test-model",
	}

	logger.LogHeader("run-abc", cfg)

	lines := logger.Lines()
	require.GreaterOrEqual(t, len(lines), 3)
	assert.Contains(t, lines[0], "run-abc")
	assert.Contains(t, lines[1], "claude_oracle=true")
	assert.Contains(t, lines[1], "scenarios=30")
	assert.Contains(t, lines[1], "test-model")
}

func TestFuzzLogger_LogScenarioPass(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, false)

	scenario := &FuzzScenario{
		ID:           "s001",
		AttackVector: "flat fee test",
		GrossAmount:  "100.00",
		Schedule: FuzzScheduleSpec{
			ApplicationOrder: "PARALLEL",
			RoundingScale:    2,
			RoundingMode:     "HALF_UP",
			Items: []FuzzItemSpec{
				{Name: "flat", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "5.00"}},
			},
		},
		GoOracleExpected: &FuzzExpectedResult{
			TotalFee:  "5.00",
			NetAmount: "95.00",
		},
		APIActual: &FuzzExpectedResult{
			TotalFee:  "5.00",
			NetAmount: "95.00",
		},
	}

	logger.LogScenarioPass(1, 10, scenario)

	lines := logger.Lines()
	require.NotEmpty(t, lines)

	joined := strings.Join(lines, "\n")
	assert.Contains(t, joined, "PASS")
	assert.Contains(t, joined, "flat fee test")
	assert.Contains(t, joined, "100.00")
}

func TestFuzzLogger_LogScenarioFail(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, false)

	scenario := &FuzzScenario{
		ID:           "s002",
		AttackVector: "rounding divergence",
		GrossAmount:  "1000.00",
		Schedule: FuzzScheduleSpec{
			ApplicationOrder: "PARALLEL",
			RoundingScale:    2,
			RoundingMode:     "BANKERS",
			Items: []FuzzItemSpec{
				{Name: "pct", Priority: 1, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.015"}},
			},
		},
		GoOracleExpected: &FuzzExpectedResult{TotalFee: "15.00", NetAmount: "985.00"},
		APIActual:        &FuzzExpectedResult{TotalFee: "14.99", NetAmount: "985.01"},
		Discrepancies: []FuzzDiscrepancy{
			{Field: "totalFee", Expected: "15.00", Actual: "14.99", Delta: "0.01"},
		},
	}

	logger.LogScenarioFail(2, 10, scenario)

	joined := strings.Join(logger.Lines(), "\n")
	assert.Contains(t, joined, "FAIL")
	assert.Contains(t, joined, "rounding divergence")
	assert.Contains(t, joined, "Delta")
}

func TestFuzzLogger_LogScenarioError(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, false)

	scenario := &FuzzScenario{
		ID:           "s003",
		AttackVector: "error scenario",
		ErrorMessage: "connection refused",
	}

	logger.LogScenarioError(3, 10, scenario)

	joined := strings.Join(logger.Lines(), "\n")
	assert.Contains(t, joined, "ERROR")
	assert.Contains(t, joined, "connection refused")
}

func TestFuzzLogger_LogSummary(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, false)

	summary := FuzzSummary{
		Total:               20,
		Passed:              18,
		Failed:              1,
		Skipped:             0,
		Errors:              1,
		OracleDisagreements: 0,
	}

	logger.LogSummary(summary, "/tmp/artifacts/run.json")

	joined := strings.Join(logger.Lines(), "\n")
	assert.Contains(t, joined, "Summary")
	assert.Contains(t, joined, "Total: 20")
	assert.Contains(t, joined, "Passed: 18")
	assert.Contains(t, joined, "Failed: 1")
	assert.Contains(t, joined, "/tmp/artifacts/run.json")
}

func TestFuzzLogger_Lines_Accumulates(t *testing.T) {
	t.Parallel()

	logger := NewFuzzLogger(t, false)

	assert.Empty(t, logger.Lines())

	cfg := &FuzzConfig{ScenarioCount: 5, ClaudeModel: "m"}
	logger.LogHeader("run-1", cfg)

	lineCount := len(logger.Lines())
	assert.Positive(t, lineCount)

	scenario := &FuzzScenario{
		ID: "s001", AttackVector: "test", GrossAmount: "100.00",
		Schedule: FuzzScheduleSpec{
			ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
			Items: []FuzzItemSpec{{Name: "f", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1"}}},
		},
		GoOracleExpected: &FuzzExpectedResult{TotalFee: "1", NetAmount: "99"},
		APIActual:        &FuzzExpectedResult{TotalFee: "1", NetAmount: "99"},
	}

	logger.LogScenarioPass(1, 1, scenario)

	assert.Greater(t, len(logger.Lines()), lineCount)
}

func TestFuzzLogger_WriteLogFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logger := NewFuzzLogger(t, false)

	cfg := &FuzzConfig{ScenarioCount: 1, ClaudeModel: "m"}
	logger.LogHeader("write-test", cfg)

	err := logger.WriteLogFile(dir, "write-test")
	require.NoError(t, err)

	files, err := filepath.Glob(filepath.Join(dir, "run_*_write-test.log"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	assert.Contains(t, string(data), "write-test")
}

func TestItemTypeSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		items    []FuzzItemSpec
		expected string
	}{
		{
			name:     "empty items",
			items:    []FuzzItemSpec{},
			expected: "(none)",
		},
		{
			name: "single type",
			items: []FuzzItemSpec{
				{StructureType: "FLAT"},
			},
			expected: "(FLAT)",
		},
		{
			name: "multiple distinct types",
			items: []FuzzItemSpec{
				{StructureType: "PERCENTAGE"},
				{StructureType: "FLAT"},
				{StructureType: "TIERED"},
			},
			expected: "(PERCENTAGE, FLAT, TIERED)",
		},
		{
			name: "duplicate types deduplicated",
			items: []FuzzItemSpec{
				{StructureType: "FLAT"},
				{StructureType: "FLAT"},
				{StructureType: "PERCENTAGE"},
			},
			expected: "(FLAT, PERCENTAGE)",
		},
		{
			name: "case normalization",
			items: []FuzzItemSpec{
				{StructureType: "flat"},
				{StructureType: "Flat"},
			},
			expected: "(FLAT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := itemTypeSummary(tt.items)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOracleVal_NilResult(t *testing.T) {
	t.Parallel()

	result := oracleVal(nil, func(r *FuzzExpectedResult) string { return r.TotalFee })
	assert.Equal(t, "?", result)
}

func TestOracleVal_NonNilResult(t *testing.T) {
	t.Parallel()

	expected := &FuzzExpectedResult{TotalFee: "42.00", NetAmount: "58.00"}

	totalFee := oracleVal(expected, func(r *FuzzExpectedResult) string { return r.TotalFee })
	assert.Equal(t, "42.00", totalFee)

	netAmount := oracleVal(expected, func(r *FuzzExpectedResult) string { return r.NetAmount })
	assert.Equal(t, "58.00", netAmount)
}
