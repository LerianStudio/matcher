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
func TestFeeRuleNormalization_PipelineIntegration(t *testing.T) {
	t.Parallel()

	t.Skip("requires fee rule harness setup: " +
		"the integration test harness does not yet provide helpers for the full " +
		"fee rule prerequisite chain (context → source × 2 → schedule → rule → " +
		"ingested transactions → match run). " +
		"See setupFeeVariancePrereqs in fee_verification_test.go for the persistence-level " +
		"equivalent; this test should exercise the orchestration layer (RunMatch) instead.")
}
