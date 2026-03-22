//go:build integration

package matching

import (
	"testing"
)

// TestFeeRuleNormalization_PipelineIntegration verifies that the fee normalization
// pipeline works end-to-end when fee rules (not just fee schedules) are configured.
//
// The matching engine loads fee rules via loadFeeRulesAndSchedules in
// internal/matching/services/command/match_group_run_commands.go.
//
// The full scenario requires:
//  1. A reconciliation context with fee normalization enabled
//  2. At least one source on each side (LEFT/RIGHT)
//  3. A fee schedule with a valid rate structure
//  4. A fee rule bound to that schedule with predicates (e.g., currency=USD)
//  5. Ingested transactions on both sides with fee metadata
//  6. A match run that exercises fee verification through the rule pipeline
//
// This test ensures the rule-based fee lookup path is exercised during
// match execution, producing fee variance records when applicable.
//
// TODO(fee-rules): Implement this test when a full integration test harness for fee
// rules is available. The prerequisite chain (context → source × 2 → fee schedule →
// fee rule with predicates → ingested transactions → match run) requires either:
//   - A shared test helper that provisions the complete fee rule prerequisite stack, or
//   - A docker-compose-backed E2E harness that seeds the database programmatically.
//
// The unit-test coverage for fee normalization error paths lives in:
//   - match_group_commands_run_test.go: TestRunMatch_FeeNormalizationEnabledButNoFeeRules
//   - match_group_commands_run_test.go: TestRunMatch_FeeRulesReferenceMissingSchedules
//   - match_group_commands_helpers_test.go: TestLoadFeeRulesAndSchedules_*
//
// See setupFeeVariancePrereqs in fee_verification_test.go for the persistence-level
// equivalent; this test should exercise the orchestration layer (RunMatch) instead.
func TestFeeRuleNormalization_PipelineIntegration(t *testing.T) {
	t.Parallel()

	t.Skip("blocked: integration test harness does not yet provide helpers for the full " +
		"fee rule prerequisite chain (context → source × 2 → schedule → rule → " +
		"ingested transactions → match run). " +
		"Unit-test coverage for error branches exists in match_group_commands_run_test.go " +
		"and match_group_commands_helpers_test.go.")
}
