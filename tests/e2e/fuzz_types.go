//go:build e2e

package e2e

import (
	"os"
	"strconv"
	"strings"

	"github.com/LerianStudio/matcher/tests/client"
)

// Default fuzz configuration values.
const defaultFuzzScenarioCount = 20

// FuzzConfig holds configuration for fuzz test runs.
type FuzzConfig struct {
	ClaudeOracleEnabled bool   `json:"claudeOracleEnabled"`
	ClaudeModel         string `json:"claudeModel"`
	ScenarioCount       int    `json:"scenarioCount"`
	ReplayPath          string `json:"replayPath"`
	ArtifactDir         string `json:"artifactDir"`
	Verbose             bool   `json:"verbose"`
}

// LoadFuzzConfig loads fuzz configuration from environment variables.
func LoadFuzzConfig() *FuzzConfig {
	return &FuzzConfig{
		ClaudeOracleEnabled: strings.EqualFold(os.Getenv("E2E_CLAUDE_ORACLE"), "true"),
		ClaudeModel:         getEnv("E2E_CLAUDE_MODEL", "claude-sonnet-4-20250514"),
		ScenarioCount:       getIntEnvFuzz("E2E_FUZZ_SCENARIOS", defaultFuzzScenarioCount),
		ReplayPath:          os.Getenv("E2E_FUZZ_REPLAY"),
		ArtifactDir:         getEnv("E2E_FUZZ_ARTIFACT_DIR", "fuzz_artifacts"),
		Verbose:             strings.EqualFold(os.Getenv("E2E_FUZZ_VERBOSE"), "true"),
	}
}

// getIntEnvFuzz parses an integer from an environment variable with a default fallback.
func getIntEnvFuzz(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	}

	return defaultVal
}

// FuzzScheduleSpec is a fully self-contained fee schedule specification for a fuzz scenario.
// It mirrors client.CreateFeeScheduleRequest but is independent for serialization/replay.
type FuzzScheduleSpec struct {
	Name             string         `json:"name"`
	Currency         string         `json:"currency"`
	ApplicationOrder string         `json:"applicationOrder"`
	RoundingScale    int            `json:"roundingScale"`
	RoundingMode     string         `json:"roundingMode"`
	Items            []FuzzItemSpec `json:"items"`
}

// FuzzItemSpec defines a single fee item in a fuzz schedule.
type FuzzItemSpec struct {
	Name          string         `json:"name"`
	Priority      int            `json:"priority"`
	StructureType string         `json:"structureType"`
	Structure     map[string]any `json:"structure"`
}

// FuzzExpectedResult holds the expected calculation result from an oracle.
type FuzzExpectedResult struct {
	TotalFee  string           `json:"totalFee"`
	NetAmount string           `json:"netAmount"`
	ItemFees  []FuzzItemResult `json:"itemFees"`
	Reasoning string           `json:"reasoning,omitempty"`
}

// FuzzItemResult holds per-item expected fee result.
type FuzzItemResult struct {
	Name     string `json:"name"`
	Fee      string `json:"fee"`
	BaseUsed string `json:"baseUsed"`
}

// FuzzScenario represents a complete fuzz test scenario.
type FuzzScenario struct {
	ID               string              `json:"id"`
	Source           string              `json:"source"`
	Category         string              `json:"category"`
	AttackVector     string              `json:"attackVector"`
	Difficulty       int                 `json:"difficulty"`
	Schedule         FuzzScheduleSpec    `json:"schedule"`
	GrossAmount      string              `json:"grossAmount"`
	GoOracleExpected *FuzzExpectedResult `json:"goOracleExpected,omitempty"`
	ClaudeExpected   *FuzzExpectedResult `json:"claudeExpected,omitempty"`
	APIActual        *FuzzExpectedResult `json:"apiActual,omitempty"`
	Verdict          string              `json:"verdict"`
	Discrepancies    []FuzzDiscrepancy   `json:"discrepancies,omitempty"`
	ErrorMessage     string              `json:"errorMessage,omitempty"`
}

// FuzzDiscrepancy records a mismatch between oracle and API results.
type FuzzDiscrepancy struct {
	Field           string `json:"field"`
	Expected        string `json:"expected"`
	Actual          string `json:"actual"`
	Delta           string `json:"delta"`
	OracleAgreement string `json:"oracleAgreement"`
}

// FuzzRunArtifact is the complete serializable artifact for a fuzz run.
type FuzzRunArtifact struct {
	RunID     string         `json:"runId"`
	Timestamp string         `json:"timestamp"`
	Config    FuzzConfig     `json:"config"`
	Scenarios []FuzzScenario `json:"scenarios"`
	Summary   FuzzSummary    `json:"summary"`
}

// FuzzSummary holds aggregate results for a fuzz run.
type FuzzSummary struct {
	Total               int `json:"total"`
	Passed              int `json:"passed"`
	Failed              int `json:"failed"`
	Skipped             int `json:"skipped"`
	Errors              int `json:"errors"`
	OracleDisagreements int `json:"oracleDisagreements"`
}

// ToCreateRequest converts a FuzzScheduleSpec to a client.CreateFeeScheduleRequest.
func (spec FuzzScheduleSpec) ToCreateRequest() client.CreateFeeScheduleRequest {
	items := make([]client.CreateFeeScheduleItemRequest, len(spec.Items))
	for i, item := range spec.Items {
		items[i] = client.CreateFeeScheduleItemRequest{
			Name:          item.Name,
			Priority:      item.Priority,
			StructureType: item.StructureType,
			Structure:     item.Structure,
		}
	}

	return client.CreateFeeScheduleRequest{
		Name:             spec.Name,
		Currency:         spec.Currency,
		ApplicationOrder: spec.ApplicationOrder,
		RoundingScale:    spec.RoundingScale,
		RoundingMode:     spec.RoundingMode,
		Items:            items,
	}
}
