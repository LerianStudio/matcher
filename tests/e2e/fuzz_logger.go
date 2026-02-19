//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// FuzzLogger handles structured logging for fuzz test runs.
// It writes to both testing.T and an optional log file.
type FuzzLogger struct {
	t       *testing.T
	verbose bool
	lines   []string // accumulated for writing to file
}

// NewFuzzLogger creates a fuzz logger.
func NewFuzzLogger(t *testing.T, verbose bool) *FuzzLogger {
	t.Helper()

	return &FuzzLogger{
		t:       t,
		verbose: verbose,
		lines:   make([]string, 0, 256), //nolint:mnd // reasonable pre-alloc for a test run
	}
}

// log writes a formatted line to both t.Logf and the accumulated lines buffer.
func (fl *FuzzLogger) log(format string, args ...any) {
	fl.t.Helper()

	line := fmt.Sprintf(format, args...)
	fl.t.Logf("%s", line)
	fl.lines = append(fl.lines, line)
}

// LogHeader logs the fuzz run header.
func (fl *FuzzLogger) LogHeader(runID string, cfg *FuzzConfig) {
	fl.t.Helper()

	fl.log("═══ Fee Schedule Fuzz Run %s ═══", runID)
	fl.log("Config: claude_oracle=%t go_oracle=true scenarios=%d model=%s",
		cfg.ClaudeOracleEnabled, cfg.ScenarioCount, cfg.ClaudeModel)
	fl.log("───────────────────────────────────────")
}

// LogScenarioPass logs a passing scenario.
func (fl *FuzzLogger) LogScenarioPass(index, total int, scenario *FuzzScenario) {
	fl.t.Helper()

	spec := &scenario.Schedule

	fl.log("[%d/%d] PASS  | %q", index, total, scenario.AttackVector)
	fl.log("  Schedule: %s, %d items (%s), scale=%d, %s",
		spec.ApplicationOrder,
		len(spec.Items),
		itemTypeSummary(spec.Items),
		spec.RoundingScale,
		spec.RoundingMode,
	)

	netAmount := oracleVal(scenario.GoOracleExpected, func(r *FuzzExpectedResult) string { return r.NetAmount })
	totalFee := oracleVal(scenario.GoOracleExpected, func(r *FuzzExpectedResult) string { return r.TotalFee })

	fl.log("  Gross: $%s -> Net: $%s (fee: $%s)",
		scenario.GrossAmount, netAmount, totalFee)

	goNet := oracleVal(scenario.GoOracleExpected, func(r *FuzzExpectedResult) string { return r.NetAmount })
	apiNet := oracleVal(scenario.APIActual, func(r *FuzzExpectedResult) string { return r.NetAmount })
	claudeNet := oracleVal(scenario.ClaudeExpected, func(r *FuzzExpectedResult) string { return r.NetAmount })

	if claudeNet != "?" {
		fl.log("  Oracles: go=%s api=%s claude=%s ✓", goNet, apiNet, claudeNet)
	} else {
		fl.log("  Oracles: go=%s api=%s ✓", goNet, apiNet)
	}
}

// LogScenarioFail logs a failing scenario with full detail.
func (fl *FuzzLogger) LogScenarioFail(index, total int, scenario *FuzzScenario) {
	fl.t.Helper()

	spec := &scenario.Schedule

	fl.log("[%d/%d] FAIL  | %q", index, total, scenario.AttackVector)
	fl.log("  Schedule: %s, %d items, scale=%d, %s",
		spec.ApplicationOrder, len(spec.Items), spec.RoundingScale, spec.RoundingMode)
	fl.log("  Gross: $%s", scenario.GrossAmount)

	fl.log("  ┌─────────────────────────────────────────┐")

	goVal := oracleVal(scenario.GoOracleExpected, func(r *FuzzExpectedResult) string { return r.NetAmount })
	claudeVal := oracleVal(scenario.ClaudeExpected, func(r *FuzzExpectedResult) string { return r.NetAmount })
	apiVal := oracleVal(scenario.APIActual, func(r *FuzzExpectedResult) string { return r.NetAmount })

	if claudeVal != "?" {
		fl.log("  │ Oracle Agreement: go=%s claude=%s", goVal, claudeVal)
	} else {
		fl.log("  │ Oracle Agreement: go=%s", goVal)
	}

	fl.log("  │ API Result:       api=%s", apiVal)

	for _, d := range scenario.Discrepancies {
		fl.log("  │ Delta: %s on %s", d.Delta, d.Field)
	}

	fl.log("  │")

	if scenario.ClaudeExpected != nil && scenario.ClaudeExpected.Reasoning != "" {
		fl.log("  │ Claude's reasoning:")
		fl.logIndentedReasoning("  │   ", scenario.ClaudeExpected.Reasoning)
		fl.log("  │")
	}

	if scenario.GoOracleExpected != nil && scenario.GoOracleExpected.Reasoning != "" {
		fl.log("  │ Go oracle reasoning:")
		fl.logIndentedReasoning("  │   ", scenario.GoOracleExpected.Reasoning)
		fl.log("  │")
	}

	fl.log("  └─────────────────────────────────────────┘")
}

// LogScenarioError logs a scenario that errored.
func (fl *FuzzLogger) LogScenarioError(index, total int, scenario *FuzzScenario) {
	fl.t.Helper()

	fl.log("[%d/%d] ERROR | %q", index, total, scenario.AttackVector)
	fl.log("  Error: %s", scenario.ErrorMessage)
}

// LogScenarioSkip logs a skipped scenario.
func (fl *FuzzLogger) LogScenarioSkip(index, total int, scenario *FuzzScenario) {
	fl.t.Helper()

	fl.log("[%d/%d] SKIP  | %q", index, total, scenario.AttackVector)
}

// LogSummary logs the final summary.
func (fl *FuzzLogger) LogSummary(summary FuzzSummary, artifactPath string) {
	fl.t.Helper()

	fl.log("═══ Summary ═══")
	fl.log("Total: %d | Passed: %d | Failed: %d | Skipped: %d | Errors: %d",
		summary.Total, summary.Passed, summary.Failed, summary.Skipped, summary.Errors)
	fl.log("Oracle disagreements: %d", summary.OracleDisagreements)
	fl.log("Artifacts: %s", artifactPath)
}

// WriteLogFile writes accumulated log lines to a file.
// The file is created at dir/run_{timestamp}_{runID}.log.
func (fl *FuzzLogger) WriteLogFile(dir, runID string) error {
	fl.t.Helper()

	if err := os.MkdirAll(dir, fuzzDirPermissions); err != nil {
		return fmt.Errorf("create log dir %s: %w", dir, err)
	}

	ts := time.Now().UTC().Format("20060102_150405")
	filename := fmt.Sprintf("run_%s_%s.log", ts, runID)
	path := filepath.Join(dir, filename)

	content := strings.Join(fl.lines, "\n") + "\n"

	if err := os.WriteFile(path, []byte(content), fuzzFilePermissions); err != nil {
		return fmt.Errorf("write log file %s: %w", path, err)
	}

	return nil
}

// Lines returns all accumulated log lines (for testing).
func (fl *FuzzLogger) Lines() []string {
	return fl.lines
}

// logIndentedReasoning splits multi-line reasoning and logs each line with the given prefix.
func (fl *FuzzLogger) logIndentedReasoning(prefix, reasoning string) {
	fl.t.Helper()

	for _, line := range strings.Split(strings.TrimRight(reasoning, "\n"), "\n") {
		fl.log("%s%s", prefix, line)
	}
}

// oracleVal safely extracts a string field from a nullable FuzzExpectedResult.
// Returns "?" if the result is nil.
func oracleVal(result *FuzzExpectedResult, getter func(*FuzzExpectedResult) string) string {
	if result == nil {
		return "?"
	}

	return getter(result)
}

// itemTypeSummary returns a parenthesized, comma-separated summary of distinct item
// structure types, e.g. "(PERCENTAGE, FLAT, TIERED)".
func itemTypeSummary(items []FuzzItemSpec) string {
	if len(items) == 0 {
		return "(none)"
	}

	// Preserve insertion order while deduplicating.
	seen := make(map[string]struct{}, len(items))
	types := make([]string, 0, len(items))

	for _, item := range items {
		upper := strings.ToUpper(item.StructureType)
		if _, exists := seen[upper]; !exists {
			seen[upper] = struct{}{}
			types = append(types, upper)
		}
	}

	return "(" + strings.Join(types, ", ") + ")"
}
