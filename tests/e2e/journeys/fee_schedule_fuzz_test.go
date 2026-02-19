//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
)

// compareFeeResults compares two FuzzExpectedResult values within an epsilon tolerance.
// Returns discrepancies found (empty slice = match).
func compareFeeResults(expected, actual *e2e.FuzzExpectedResult, epsilon decimal.Decimal) []e2e.FuzzDiscrepancy {
	var discrepancies []e2e.FuzzDiscrepancy

	// Compare totalFee.
	expTotal, _ := decimal.NewFromString(expected.TotalFee)
	actTotal, _ := decimal.NewFromString(actual.TotalFee)

	if expTotal.Sub(actTotal).Abs().GreaterThan(epsilon) {
		discrepancies = append(discrepancies, e2e.FuzzDiscrepancy{
			Field:           "totalFee",
			Expected:        expected.TotalFee,
			Actual:          actual.TotalFee,
			Delta:           expTotal.Sub(actTotal).String(),
			OracleAgreement: fmt.Sprintf("go=%s, api=%s", expected.TotalFee, actual.TotalFee),
		})
	}

	// Compare netAmount.
	expNet, _ := decimal.NewFromString(expected.NetAmount)
	actNet, _ := decimal.NewFromString(actual.NetAmount)

	if expNet.Sub(actNet).Abs().GreaterThan(epsilon) {
		discrepancies = append(discrepancies, e2e.FuzzDiscrepancy{
			Field:           "netAmount",
			Expected:        expected.NetAmount,
			Actual:          actual.NetAmount,
			Delta:           expNet.Sub(actNet).String(),
			OracleAgreement: fmt.Sprintf("go=%s, api=%s", expected.NetAmount, actual.NetAmount),
		})
	}

	// Build name→fee index from actual for per-item comparison.
	actualByName := make(map[string]e2e.FuzzItemResult, len(actual.ItemFees))
	for _, item := range actual.ItemFees {
		actualByName[item.Name] = item
	}

	for _, expItem := range expected.ItemFees {
		actItem, found := actualByName[expItem.Name]
		if !found {
			discrepancies = append(discrepancies, e2e.FuzzDiscrepancy{
				Field:           fmt.Sprintf("itemFee[%s]", expItem.Name),
				Expected:        expItem.Fee,
				Actual:          "(missing)",
				Delta:           expItem.Fee,
				OracleAgreement: fmt.Sprintf("go=%s, api=(missing)", expItem.Fee),
			})

			continue
		}

		expFee, _ := decimal.NewFromString(expItem.Fee)
		actFee, _ := decimal.NewFromString(actItem.Fee)

		if expFee.Sub(actFee).Abs().GreaterThan(epsilon) {
			discrepancies = append(discrepancies, e2e.FuzzDiscrepancy{
				Field:           fmt.Sprintf("itemFee[%s]", expItem.Name),
				Expected:        expItem.Fee,
				Actual:          actItem.Fee,
				Delta:           expFee.Sub(actFee).String(),
				OracleAgreement: fmt.Sprintf("go=%s, api=%s", expItem.Fee, actItem.Fee),
			})
		}
	}

	return discrepancies
}

// compareThreeWayResults compares Go oracle, Claude oracle, and API result.
// Returns discrepancies and whether the two oracles agree.
func compareThreeWayResults(
	goExpected, claudeExpected, apiActual *e2e.FuzzExpectedResult,
	epsilon decimal.Decimal,
) (discrepancies []e2e.FuzzDiscrepancy, oraclesAgree bool) {
	// Check Go oracle vs API.
	goVsAPI := compareFeeResults(goExpected, apiActual, epsilon)

	// Check Claude oracle vs API.
	claudeVsAPI := compareFeeResults(claudeExpected, apiActual, epsilon)

	// Check if Go and Claude agree with each other.
	goVsClaude := compareFeeResults(goExpected, claudeExpected, epsilon)
	oraclesAgree = len(goVsClaude) == 0

	// Merge discrepancies with three-way oracle agreement strings.
	seen := make(map[string]struct{})

	for _, disc := range goVsAPI {
		// Find matching Claude value for this field.
		claudeVal := claudeFieldValue(claudeExpected, disc.Field)

		disc.OracleAgreement = fmt.Sprintf("go=%s, claude=%s, api=%s", disc.Expected, claudeVal, disc.Actual)
		discrepancies = append(discrepancies, disc)
		seen[disc.Field] = struct{}{}
	}

	for _, disc := range claudeVsAPI {
		if _, exists := seen[disc.Field]; exists {
			continue // already captured from goVsAPI
		}

		goVal := goFieldValue(goExpected, disc.Field)

		disc.OracleAgreement = fmt.Sprintf("go=%s, claude=%s, api=%s", goVal, disc.Expected, disc.Actual)
		discrepancies = append(discrepancies, disc)
	}

	return discrepancies, oraclesAgree
}

// claudeFieldValue extracts a named field's value from a Claude expected result.
func claudeFieldValue(result *e2e.FuzzExpectedResult, field string) string {
	return fieldValueFromResult(result, field)
}

// goFieldValue extracts a named field's value from a Go oracle expected result.
func goFieldValue(result *e2e.FuzzExpectedResult, field string) string {
	return fieldValueFromResult(result, field)
}

// fieldValueFromResult extracts a named field from an expected result.
func fieldValueFromResult(result *e2e.FuzzExpectedResult, field string) string {
	if result == nil {
		return "?"
	}

	switch field {
	case "totalFee":
		return result.TotalFee
	case "netAmount":
		return result.NetAmount
	default:
		// Item fee fields like "itemFee[interchange]".
		for _, item := range result.ItemFees {
			if fmt.Sprintf("itemFee[%s]", item.Name) == field {
				return item.Fee
			}
		}

		return "?"
	}
}

// simResponseToExpected converts a client.SimulateFeeResponse to a FuzzExpectedResult.
func simResponseToExpected(sim *client.SimulateFeeResponse) *e2e.FuzzExpectedResult {
	itemFees := make([]e2e.FuzzItemResult, len(sim.Items))
	for i, item := range sim.Items {
		itemFees[i] = e2e.FuzzItemResult{
			Name:     item.Name,
			Fee:      item.Fee,
			BaseUsed: item.BaseUsed,
		}
	}

	return &e2e.FuzzExpectedResult{
		TotalFee:  sim.TotalFee,
		NetAmount: sim.NetAmount,
		ItemFees:  itemFees,
	}
}

// executeScenario runs a single fuzz scenario against the API and compares with the Go oracle.
// It populates the scenario's GoOracleExpected, APIActual, Verdict, Discrepancies, and ErrorMessage fields.
func executeScenario(
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	scenario *e2e.FuzzScenario,
	epsilon decimal.Decimal,
) {
	// Step 1: Compute Go oracle expected result.
	goExpected, err := e2e.GoOracleCalculate(scenario.Schedule, scenario.GrossAmount)
	if err != nil {
		scenario.Verdict = "ERROR"
		scenario.ErrorMessage = fmt.Sprintf("go oracle error: %v", err)

		return
	}

	scenario.GoOracleExpected = goExpected

	// Step 2: Create the fee schedule via API.
	createReq := scenario.Schedule.ToCreateRequest()
	createReq.Name = tc.UniqueName("fuzz-" + scenario.ID)

	schedule, err := apiClient.FeeSchedule.CreateFeeSchedule(ctx, createReq)
	if err != nil {
		scenario.Verdict = "ERROR"
		scenario.ErrorMessage = fmt.Sprintf("create fee schedule: %v", err)

		return
	}

	// Register cleanup to delete the schedule.
	defer func() {
		_ = apiClient.FeeSchedule.DeleteFeeSchedule(ctx, schedule.ID)
	}()

	// Step 3: Simulate via API.
	sim, err := apiClient.FeeSchedule.SimulateFeeSchedule(ctx, schedule.ID, client.SimulateFeeRequest{
		GrossAmount: scenario.GrossAmount,
		Currency:    scenario.Schedule.Currency,
	})
	if err != nil {
		scenario.Verdict = "ERROR"
		scenario.ErrorMessage = fmt.Sprintf("simulate fee schedule: %v", err)

		return
	}

	apiResult := simResponseToExpected(sim)
	scenario.APIActual = apiResult

	// Step 4: Compare.
	discrepancies := compareFeeResults(goExpected, apiResult, epsilon)
	scenario.Discrepancies = discrepancies

	if len(discrepancies) == 0 {
		scenario.Verdict = "PASS"

		return
	}

	scenario.Verdict = "FAIL"
}

// recordScenarioResult logs the scenario outcome and updates the summary counters.
func recordScenarioResult(
	scenario *e2e.FuzzScenario,
	index, total int,
	summary *e2e.FuzzSummary,
	logger *e2e.FuzzLogger,
	cfg *e2e.FuzzConfig,
	tc *e2e.TestContext,
) {
	switch scenario.Verdict {
	case "PASS":
		summary.Passed++

		logger.LogScenarioPass(index, total, scenario)

	case "FAIL":
		summary.Failed++

		logger.LogScenarioFail(index, total, scenario)

		// Write individual failure artifact.
		if _, writeErr := e2e.WriteFailureArtifact(scenario, cfg.ArtifactDir); writeErr != nil {
			tc.Logf("Warning: failed to write failure artifact for %s: %v", scenario.ID, writeErr)
		}

	case "ERROR":
		summary.Errors++

		logger.LogScenarioError(index, total, scenario)

	default:
		// Should not happen, but treat as skip.
		summary.Skipped++

		logger.LogScenarioSkip(index, total, scenario)
	}
}

// writeRunArtifacts writes the full run artifact and log file, returning the artifact path.
func writeRunArtifacts(
	tc *e2e.TestContext,
	cfg *e2e.FuzzConfig,
	logger *e2e.FuzzLogger,
	summary e2e.FuzzSummary,
	scenarios []e2e.FuzzScenario,
) string {
	artifact := &e2e.FuzzRunArtifact{
		RunID:     tc.RunID(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Config:    *cfg,
		Scenarios: scenarios,
		Summary:   summary,
	}

	artifactPath, writeErr := e2e.WriteFuzzArtifact(artifact, cfg.ArtifactDir)
	if writeErr != nil {
		tc.Logf("Warning: failed to write run artifact: %v", writeErr)

		artifactPath = "(write failed)"
	}

	// Write log file.
	if logErr := logger.WriteLogFile(cfg.ArtifactDir, tc.RunID()); logErr != nil {
		tc.Logf("Warning: failed to write log file: %v", logErr)
	}

	return artifactPath
}

// TestFeeSchedule_FuzzDeterministic runs all deterministic fuzz scenarios (Categories 1-4)
// against the fee schedule API, comparing each result with the Go oracle.
// Scenarios cover rounding torture, tiered boundaries, cascading stress, and convergence.
func TestFeeSchedule_FuzzDeterministic(t *testing.T) { //nolint:paralleltest // e2e test uses shared API server
	e2e.RunE2EWithTimeout(t, 10*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) { //nolint:thelper // test function, not helper
			ctx := context.Background()
			cfg := e2e.LoadFuzzConfig()

			logger := e2e.NewFuzzLogger(t, cfg.Verbose)
			logger.LogHeader(tc.RunID(), cfg)

			scenarios := e2e.GenerateAllDeterministicScenarios()
			total := len(scenarios)

			epsilon := decimal.NewFromFloat(0.01)

			var summary e2e.FuzzSummary

			summary.Total = total

			for i := range scenarios {
				scenario := &scenarios[i]

				executeScenario(ctx, tc, apiClient, scenario, epsilon)
				recordScenarioResult(scenario, i+1, total, &summary, logger, cfg, tc)
			}

			artifactPath := writeRunArtifacts(tc, cfg, logger, summary, scenarios)

			logger.LogSummary(summary, artifactPath)

			// Final assertion: fail the test if any scenarios failed.
			assert.Equal(t, 0, summary.Errors,
				"fuzz deterministic: %d scenarios had errors (API call failures)", summary.Errors)
			require.Equal(t, 0, summary.Failed,
				"fuzz deterministic: %d scenarios failed (Go oracle vs API mismatch)", summary.Failed)
		},
	)
}

// executeFuzzClaudeScenario runs a single Claude-generated scenario through the oracle comparison pipeline.
// It computes Go and Claude oracle expectations, creates and simulates via API, and records the verdict.
func executeFuzzClaudeScenario(
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	oracle *e2e.ClaudeOracle,
	scenario *e2e.FuzzScenario,
	index, total int,
	epsilon decimal.Decimal,
	summary *e2e.FuzzSummary,
	logger *e2e.FuzzLogger,
	cfg *e2e.FuzzConfig,
) (shouldGenerateVariations bool) {
	// Step 1: Compute Go oracle expected.
	goExpected, goErr := e2e.GoOracleCalculate(scenario.Schedule, scenario.GrossAmount)
	if goErr != nil {
		scenario.Verdict = "ERROR"
		scenario.ErrorMessage = fmt.Sprintf("go oracle error: %v", goErr)
		summary.Errors++

		logger.LogScenarioError(index, total, scenario)

		return false
	}

	scenario.GoOracleExpected = goExpected

	// Step 2: Compute Claude oracle expected.
	claudeExpected, claudeErr := oracle.ComputeExpected(ctx, scenario.Schedule, scenario.GrossAmount)
	if claudeErr != nil {
		tc.Logf("[%d/%d] Warning: Claude oracle failed: %v — falling back to Go-only", index, total, claudeErr)

		claudeExpected = nil
	}

	scenario.ClaudeExpected = claudeExpected

	// Step 3: Create and simulate via API.
	apiResult := createAndSimulateFeeSchedule(ctx, tc, apiClient, scenario, index, total, summary, logger)
	if apiResult == nil {
		return false
	}

	scenario.APIActual = apiResult

	// Step 4: Compare results.
	return compareAndRecordClaudeResults(
		scenario, goExpected, claudeExpected, apiResult,
		index, total, epsilon, summary, logger, cfg, tc,
	)
}

// createAndSimulateFeeSchedule creates a fee schedule and simulates it, returning the API result.
// Returns nil if an error occurred (scenario is marked as ERROR).
func createAndSimulateFeeSchedule(
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	scenario *e2e.FuzzScenario,
	index, total int,
	summary *e2e.FuzzSummary,
	logger *e2e.FuzzLogger,
) *e2e.FuzzExpectedResult {
	createReq := scenario.Schedule.ToCreateRequest()
	createReq.Name = tc.UniqueName("fuzz-" + scenario.ID)

	schedule, createErr := apiClient.FeeSchedule.CreateFeeSchedule(ctx, createReq)
	if createErr != nil {
		scenario.Verdict = "ERROR"
		scenario.ErrorMessage = fmt.Sprintf("create fee schedule: %v", createErr)
		summary.Errors++

		logger.LogScenarioError(index, total, scenario)

		return nil
	}

	sim, simErr := apiClient.FeeSchedule.SimulateFeeSchedule(ctx, schedule.ID, client.SimulateFeeRequest{
		GrossAmount: scenario.GrossAmount,
		Currency:    scenario.Schedule.Currency,
	})

	// Cleanup regardless of outcome.
	_ = apiClient.FeeSchedule.DeleteFeeSchedule(ctx, schedule.ID)

	if simErr != nil {
		scenario.Verdict = "ERROR"
		scenario.ErrorMessage = fmt.Sprintf("simulate fee schedule: %v", simErr)
		summary.Errors++

		logger.LogScenarioError(index, total, scenario)

		return nil
	}

	return simResponseToExpected(sim)
}

// compareAndRecordClaudeResults compares oracle results against the API and records the verdict.
// Returns true if variations should be generated (both oracles agree but API differs).
func compareAndRecordClaudeResults(
	scenario *e2e.FuzzScenario,
	goExpected, claudeExpected, apiResult *e2e.FuzzExpectedResult,
	index, total int,
	epsilon decimal.Decimal,
	summary *e2e.FuzzSummary,
	logger *e2e.FuzzLogger,
	cfg *e2e.FuzzConfig,
	tc *e2e.TestContext,
) bool {
	if claudeExpected != nil {
		return compareThreeWayAndRecord(
			scenario, goExpected, claudeExpected, apiResult,
			index, total, epsilon, summary, logger, cfg, tc,
		)
	}

	// Claude oracle unavailable, compare Go vs API only.
	discrepancies := compareFeeResults(goExpected, apiResult, epsilon)
	scenario.Discrepancies = discrepancies

	if len(discrepancies) == 0 {
		scenario.Verdict = "PASS"
		summary.Passed++

		logger.LogScenarioPass(index, total, scenario)
	} else {
		scenario.Verdict = "FAIL"
		summary.Failed++

		logger.LogScenarioFail(index, total, scenario)

		if _, wErr := e2e.WriteFailureArtifact(scenario, cfg.ArtifactDir); wErr != nil {
			tc.Logf("Warning: failed to write failure artifact for %s: %v", scenario.ID, wErr)
		}
	}

	return false
}

// compareThreeWayAndRecord performs the three-way comparison and records verdict.
// Returns true if variations should be generated.
func compareThreeWayAndRecord(
	scenario *e2e.FuzzScenario,
	goExpected, claudeExpected, apiResult *e2e.FuzzExpectedResult,
	index, total int,
	epsilon decimal.Decimal,
	summary *e2e.FuzzSummary,
	logger *e2e.FuzzLogger,
	cfg *e2e.FuzzConfig,
	tc *e2e.TestContext,
) bool {
	discrepancies, oraclesAgree := compareThreeWayResults(goExpected, claudeExpected, apiResult, epsilon)
	scenario.Discrepancies = discrepancies

	if len(discrepancies) == 0 {
		scenario.Verdict = "PASS"
		summary.Passed++

		logger.LogScenarioPass(index, total, scenario)

		return false
	}

	scenario.Verdict = "FAIL"
	summary.Failed++

	logger.LogScenarioFail(index, total, scenario)

	if _, wErr := e2e.WriteFailureArtifact(scenario, cfg.ArtifactDir); wErr != nil {
		tc.Logf("Warning: failed to write failure artifact for %s: %v", scenario.ID, wErr)
	}

	if oraclesAgree {
		// Both oracles agree, API differs → definite bug.
		tc.Logf("[%d/%d] Both oracles agree but API differs — generating variations...", index, total)

		return true
	}

	// Oracles disagree — softer signal.
	summary.OracleDisagreements++

	tc.Logf("[%d/%d] Oracle disagreement (go vs claude) — possible oracle bug", index, total)

	return false
}

// generateAndRunVariations generates variation scenarios from a failed scenario and runs them.
func generateAndRunVariations(
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	oracle *e2e.ClaudeOracle,
	originalScenario *e2e.FuzzScenario,
	apiResult *e2e.FuzzExpectedResult,
	epsilon decimal.Decimal,
	summary *e2e.FuzzSummary,
	logger *e2e.FuzzLogger,
	baseTotal int,
) []e2e.FuzzScenario {
	vars, varErr := oracle.GenerateVariations(ctx, *originalScenario, apiResult, 3) // small batch of variations
	if varErr != nil {
		tc.Logf("Warning: failed to generate variations: %v", varErr)

		return nil
	}

	for i := range vars {
		varScenario := &vars[i]
		summary.Total++

		executeScenario(ctx, tc, apiClient, varScenario, epsilon)

		varIdx := baseTotal + i + 1

		switch varScenario.Verdict {
		case "PASS":
			summary.Passed++

			logger.LogScenarioPass(varIdx, summary.Total, varScenario)

		case "FAIL":
			summary.Failed++

			logger.LogScenarioFail(varIdx, summary.Total, varScenario)

		case "ERROR":
			summary.Errors++

			logger.LogScenarioError(varIdx, summary.Total, varScenario)

		default:
			summary.Skipped++

			logger.LogScenarioSkip(varIdx, summary.Total, varScenario)
		}
	}

	return vars
}

// TestFeeSchedule_FuzzClaude runs Claude-generated adversarial scenarios against the API,
// comparing with both the Go oracle and Claude's own expected calculations.
// When both oracles agree but the API differs, it signals a definite bug.
// When oracles disagree, it signals a softer concern (possible oracle bug).
func TestFeeSchedule_FuzzClaude(t *testing.T) { //nolint:paralleltest // e2e test uses shared API server
	if os.Getenv("E2E_CLAUDE_ORACLE") == "" || os.Getenv("E2E_CLAUDE_ORACLE") != "true" {
		t.Skip("E2E_CLAUDE_ORACLE not enabled")
	}

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}

	e2e.RunE2EWithTimeout(t, 15*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) { //nolint:thelper // test function, not helper
			ctx := context.Background()
			cfg := e2e.LoadFuzzConfig()

			logger := e2e.NewFuzzLogger(t, cfg.Verbose)
			logger.LogHeader(tc.RunID(), cfg)

			oracle := e2e.NewClaudeOracle(apiKey, cfg.ClaudeModel)

			// Generate adversarial scenarios.
			tc.Logf("Generating %d adversarial scenarios via Claude...", cfg.ScenarioCount)

			scenarios, err := oracle.GenerateAdversarialScenarios(ctx, cfg.ScenarioCount)
			require.NoError(t, err, "failed to generate adversarial scenarios")
			tc.Logf("Generated %d scenarios", len(scenarios))

			total := len(scenarios)
			epsilon := decimal.NewFromFloat(0.01)

			var summary e2e.FuzzSummary

			summary.Total = total

			// Collect variation scenarios to run after initial pass.
			var variations []e2e.FuzzScenario

			for i := range scenarios {
				scenario := &scenarios[i]

				shouldVary := executeFuzzClaudeScenario(
					ctx, tc, apiClient, oracle, scenario,
					i+1, total, epsilon, &summary, logger, cfg,
				)

				if shouldVary && scenario.APIActual != nil {
					vars := generateAndRunVariations(
						ctx, tc, apiClient, oracle, scenario, scenario.APIActual,
						epsilon, &summary, logger, total,
					)

					variations = append(variations, vars...)
				}
			}

			// Merge variations into scenarios for artifact.
			if len(variations) > 0 {
				scenarios = append(scenarios, variations...)
			}

			artifactPath := writeRunArtifacts(tc, cfg, logger, summary, scenarios)

			logger.LogSummary(summary, artifactPath)

			// Final assertions.
			assert.Equal(t, 0, summary.Errors,
				"fuzz claude: %d scenarios had errors", summary.Errors)

			if summary.OracleDisagreements > 0 {
				tc.Logf("Note: %d oracle disagreements detected (Go vs Claude) — review artifacts for analysis",
					summary.OracleDisagreements)
			}

			require.Equal(t, 0, summary.Failed,
				"fuzz claude: %d scenarios failed (oracle vs API mismatch)", summary.Failed)
		},
	)
}

// TestFeeSchedule_FuzzReplay replays scenarios from an artifact file.
// Set E2E_FUZZ_REPLAY to the path of a fuzz artifact or scenario JSON file.
func TestFeeSchedule_FuzzReplay(t *testing.T) { //nolint:paralleltest // e2e test uses shared API server
	replayPath := os.Getenv("E2E_FUZZ_REPLAY")
	if replayPath == "" {
		t.Skip("E2E_FUZZ_REPLAY not set")
	}

	e2e.RunE2EWithTimeout(t, 10*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) { //nolint:thelper // test function, not helper
			ctx := context.Background()
			cfg := e2e.LoadFuzzConfig()

			logger := e2e.NewFuzzLogger(t, cfg.Verbose)
			logger.LogHeader(tc.RunID(), cfg)

			tc.Logf("Loading replay scenarios from %s", replayPath)

			scenarios, err := e2e.LoadFuzzScenarios(replayPath)
			require.NoError(t, err, "failed to load replay scenarios")
			tc.Logf("Loaded %d scenarios for replay", len(scenarios))

			total := len(scenarios)
			epsilon := decimal.NewFromFloat(0.01)

			var summary e2e.FuzzSummary

			summary.Total = total

			for i := range scenarios {
				scenario := &scenarios[i]

				// Reset previous results so we re-evaluate cleanly.
				scenario.GoOracleExpected = nil
				scenario.APIActual = nil
				scenario.Verdict = ""
				scenario.Discrepancies = nil
				scenario.ErrorMessage = ""

				executeScenario(ctx, tc, apiClient, scenario, epsilon)
				recordScenarioResult(scenario, i+1, total, &summary, logger, cfg, tc)
			}

			artifactPath := writeRunArtifacts(tc, cfg, logger, summary, scenarios)

			logger.LogSummary(summary, artifactPath)

			assert.Equal(t, 0, summary.Errors,
				"fuzz replay: %d scenarios had errors", summary.Errors)
			require.Equal(t, 0, summary.Failed,
				"fuzz replay: %d scenarios failed (Go oracle vs API mismatch)", summary.Failed)
		},
	)
}

// TestFeeSchedule_FuzzRegressions runs all permanent regression scenarios.
// Regressions are loaded from fuzz_artifacts/regressions/*.json and ALWAYS run
// (no environment variable gating) — once a bug is found and fixed, the scenario
// that caught it becomes a permanent regression guard.
func TestFeeSchedule_FuzzRegressions(t *testing.T) { //nolint:paralleltest // e2e test uses shared API server
	e2e.RunE2EWithTimeout(t, 5*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) { //nolint:thelper // test function, not helper
			ctx := context.Background()
			cfg := e2e.LoadFuzzConfig()

			logger := e2e.NewFuzzLogger(t, cfg.Verbose)
			logger.LogHeader(tc.RunID(), cfg)

			scenarios, err := e2e.LoadRegressionScenarios(cfg.ArtifactDir)
			if err != nil {
				tc.Logf("Warning: failed to load regression scenarios: %v", err)
			}

			if len(scenarios) == 0 {
				t.Skip("no regression scenarios found")
			}

			tc.Logf("Loaded %d regression scenarios", len(scenarios))

			total := len(scenarios)
			epsilon := decimal.NewFromFloat(0.01)

			var summary e2e.FuzzSummary

			summary.Total = total

			for i := range scenarios {
				scenario := &scenarios[i]

				// Reset previous results for clean re-evaluation.
				scenario.GoOracleExpected = nil
				scenario.APIActual = nil
				scenario.Verdict = ""
				scenario.Discrepancies = nil
				scenario.ErrorMessage = ""

				executeScenario(ctx, tc, apiClient, scenario, epsilon)
				recordScenarioResult(scenario, i+1, total, &summary, logger, cfg, tc)
			}

			artifactPath := writeRunArtifacts(tc, cfg, logger, summary, scenarios)

			logger.LogSummary(summary, artifactPath)

			assert.Equal(t, 0, summary.Errors,
				"fuzz regressions: %d scenarios had errors", summary.Errors)
			require.Equal(t, 0, summary.Failed,
				"fuzz regressions: %d regression scenarios failed — a previously-fixed bug may have regressed", summary.Failed)
		},
	)
}
