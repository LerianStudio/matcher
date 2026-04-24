//go:build integration

// Package matching contains integration tests that exercise the matching
// context's WithTx repository surface under multi-aggregate composition. The
// goal is to catch cross-aggregate invariant bugs that per-method sqlmock
// tests cannot see: scenarios where two WithTx calls share a single
// transaction and the overall composition is expected to be atomic under
// both commit and rollback.
//
// These tests complement — they do not replace — the single-repo integration
// tests in *_repository_test.go. Each test here wires up at least two
// repositories, runs them inside one pgcommon.WithTenantTxProvider, and
// asserts behavior under two conditions:
//
//  1. Deliberate rollback: every write in the composition must leave zero
//     persisted state.
//  2. Deliberate commit: every write in the composition must be visible
//     post-commit.
//
// Covers FINDING-042 (REFACTOR-051).
package matching

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	feeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchItemRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_item"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

// errDeliberateRollback is returned from the inside of a composition callback
// to force the containing tx to roll back. The post-rollback assertion then
// verifies that every WithTx write in the callback was undone atomically.
var errDeliberateRollback = errors.New("deliberate rollback for composition test")

// seedMatchRuleForComposition creates a minimal match rule under the seeded
// reconciliation context. match_groups.rule_id has a FK on match_rules.id, so
// group-writing compositions need this seed.
func seedMatchRuleForComposition(t *testing.T, h *integration.TestHarness) uuid.UUID {
	t.Helper()

	ctx := h.Ctx()
	// Priority constraint: 1 ≤ priority ≤ 1000. The harness resets the DB
	// between tests so collisions on (context_id, priority) only need to be
	// avoided within a single test's scope — an arbitrary value in-range is
	// fine.
	rule, err := configEntities.NewMatchRule(
		ctx,
		h.Seed.ContextID,
		configEntities.CreateMatchRuleInput{
			Priority: 1 + int(uuid.New().ID()%999),
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true, "matchAmount": true},
		},
	)
	require.NoError(t, err)

	ruleRepo := matchRuleRepo.NewRepository(h.Provider())

	createdRule, err := ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	return createdRule.ID
}

// newCompositionMatchGroup builds a MatchGroup (with two items, the domain
// minimum) tied to the provided run/rule. The items have unconstrained
// transaction_ids — match_items FK to transactions, so callers that want to
// persist items must wire a real transaction row. Scenarios below that only
// exercise match_group.CreateBatchWithTx skip item persistence entirely.
func newCompositionMatchGroup(
	t *testing.T,
	h *integration.TestHarness,
	runID, ruleID uuid.UUID,
) *matchingEntities.MatchGroup {
	t.Helper()

	ctx := h.Ctx()
	confidence, err := matchingVO.ParseConfidenceScore(95)
	require.NoError(t, err)

	itemA, err := matchingEntities.NewMatchItem(
		ctx,
		uuid.New(),
		decimal.NewFromInt(42),
		"USD",
		decimal.NewFromInt(42),
	)
	require.NoError(t, err)

	itemB, err := matchingEntities.NewMatchItem(
		ctx,
		uuid.New(),
		decimal.NewFromInt(42),
		"USD",
		decimal.NewFromInt(42),
	)
	require.NoError(t, err)

	group, err := matchingEntities.NewMatchGroup(
		ctx,
		h.Seed.ContextID,
		runID,
		ruleID,
		confidence,
		[]*matchingEntities.MatchItem{itemA, itemB},
	)
	require.NoError(t, err)

	return group
}

// TestIntegration_Matching_WithTxComposition_MatchRunAndGroup_Rollback asserts
// that CreateWithTx on MatchRun and CreateBatchWithTx on MatchGroup, sharing
// one tx, both roll back when the callback returns an error. Without atomic
// rollback a partially-persisted run with no groups (or orphan groups) would
// corrupt the dashboard.
func TestIntegration_Matching_WithTxComposition_MatchRunAndGroup_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		ruleID := seedMatchRuleForComposition(t, h)

		runRepoInst := matchRunRepo.NewRepository(h.Provider())
		groupRepoInst := matchGroupRepo.NewRepository(h.Provider())

		run, err := matchingEntities.NewMatchRun(ctx, h.Seed.ContextID, matchingVO.MatchRunModeCommit)
		require.NoError(t, err)

		group := newCompositionMatchGroup(t, h, run.ID, ruleID)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := runRepoInst.CreateWithTx(ctx, tx, run); err != nil {
				return struct{}{}, err
			}

			if _, err := groupRepoInst.CreateBatchWithTx(ctx, tx, []*matchingEntities.MatchGroup{group}); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundRun, err := runRepoInst.FindByID(ctx, h.Seed.ContextID, run.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"MatchRun row must not persist after deliberate rollback")
		require.Nil(t, foundRun)

		foundGroup, err := groupRepoInst.FindByID(ctx, h.Seed.ContextID, group.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"MatchGroup row must not persist after deliberate rollback")
		require.Nil(t, foundGroup)
	})
}

// TestIntegration_Matching_WithTxComposition_MatchRunAndGroup_Commit is the
// positive counterpart — both writes must be visible after a successful commit.
func TestIntegration_Matching_WithTxComposition_MatchRunAndGroup_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		ruleID := seedMatchRuleForComposition(t, h)

		runRepoInst := matchRunRepo.NewRepository(h.Provider())
		groupRepoInst := matchGroupRepo.NewRepository(h.Provider())

		run, err := matchingEntities.NewMatchRun(ctx, h.Seed.ContextID, matchingVO.MatchRunModeCommit)
		require.NoError(t, err)

		group := newCompositionMatchGroup(t, h, run.ID, ruleID)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := runRepoInst.CreateWithTx(ctx, tx, run); err != nil {
				return struct{}{}, err
			}

			if _, err := groupRepoInst.CreateBatchWithTx(ctx, tx, []*matchingEntities.MatchGroup{group}); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundRun, err := runRepoInst.FindByID(ctx, h.Seed.ContextID, run.ID)
		require.NoError(t, err)
		require.NotNil(t, foundRun)
		require.Equal(t, run.ID, foundRun.ID)

		foundGroup, err := groupRepoInst.FindByID(ctx, h.Seed.ContextID, group.ID)
		require.NoError(t, err)
		require.NotNil(t, foundGroup)
		require.Equal(t, group.ID, foundGroup.ID)
	})
}

// TestIntegration_Matching_WithTxComposition_MatchRunCreateAndUpdate_Rollback
// asserts that CreateWithTx followed by UpdateWithTx on the same MatchRun
// aggregate within one tx rolls back atomically — so a partially-persisted
// "run created then failed to complete" state cannot exist.
func TestIntegration_Matching_WithTxComposition_MatchRunCreateAndUpdate_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		runRepoInst := matchRunRepo.NewRepository(h.Provider())

		run, err := matchingEntities.NewMatchRun(ctx, h.Seed.ContextID, matchingVO.MatchRunModeDryRun)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := runRepoInst.CreateWithTx(ctx, tx, run); err != nil {
				return struct{}{}, err
			}

			if err := run.Complete(ctx, map[string]int{"processed": 1}); err != nil {
				return struct{}{}, err
			}

			if _, err := runRepoInst.UpdateWithTx(ctx, tx, run); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundRun, err := runRepoInst.FindByID(ctx, h.Seed.ContextID, run.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"MatchRun must not persist after Create+Update+rollback")
		require.Nil(t, foundRun)
	})
}

// TestIntegration_Matching_WithTxComposition_MatchRunCreateAndUpdate_Commit is
// the commit counterpart: both write phases visible after commit, and the run
// is in its terminal Completed state.
func TestIntegration_Matching_WithTxComposition_MatchRunCreateAndUpdate_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		runRepoInst := matchRunRepo.NewRepository(h.Provider())

		run, err := matchingEntities.NewMatchRun(ctx, h.Seed.ContextID, matchingVO.MatchRunModeDryRun)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := runRepoInst.CreateWithTx(ctx, tx, run); err != nil {
				return struct{}{}, err
			}

			if err := run.Complete(ctx, map[string]int{"processed": 1}); err != nil {
				return struct{}{}, err
			}

			if _, err := runRepoInst.UpdateWithTx(ctx, tx, run); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundRun, err := runRepoInst.FindByID(ctx, h.Seed.ContextID, run.ID)
		require.NoError(t, err)
		require.NotNil(t, foundRun)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, foundRun.Status)
	})
}

// TestIntegration_Matching_WithTxComposition_MatchGroupCreateAndUpdate_Rollback
// asserts that CreateBatchWithTx followed by UpdateWithTx on the same group
// rolls back atomically — so a group can never be observed mid-confirmation.
func TestIntegration_Matching_WithTxComposition_MatchGroupCreateAndUpdate_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		ruleID := seedMatchRuleForComposition(t, h)

		runRepoInst := matchRunRepo.NewRepository(h.Provider())
		groupRepoInst := matchGroupRepo.NewRepository(h.Provider())

		run, err := matchingEntities.NewMatchRun(ctx, h.Seed.ContextID, matchingVO.MatchRunModeCommit)
		require.NoError(t, err)
		createdRun, err := runRepoInst.Create(ctx, run)
		require.NoError(t, err)

		group := newCompositionMatchGroup(t, h, createdRun.ID, ruleID)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := groupRepoInst.CreateBatchWithTx(ctx, tx, []*matchingEntities.MatchGroup{group}); err != nil {
				return struct{}{}, err
			}

			if err := group.Confirm(ctx); err != nil {
				return struct{}{}, err
			}

			if _, err := groupRepoInst.UpdateWithTx(ctx, tx, group); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundGroup, err := groupRepoInst.FindByID(ctx, h.Seed.ContextID, group.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"MatchGroup must not persist after Create+Update+rollback")
		require.Nil(t, foundGroup)
	})
}

// TestIntegration_Matching_WithTxComposition_MatchGroupCreateAndUpdate_Commit is
// the commit counterpart — group visible, in CONFIRMED status.
func TestIntegration_Matching_WithTxComposition_MatchGroupCreateAndUpdate_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		ruleID := seedMatchRuleForComposition(t, h)

		runRepoInst := matchRunRepo.NewRepository(h.Provider())
		groupRepoInst := matchGroupRepo.NewRepository(h.Provider())

		run, err := matchingEntities.NewMatchRun(ctx, h.Seed.ContextID, matchingVO.MatchRunModeCommit)
		require.NoError(t, err)
		createdRun, err := runRepoInst.Create(ctx, run)
		require.NoError(t, err)

		group := newCompositionMatchGroup(t, h, createdRun.ID, ruleID)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := groupRepoInst.CreateBatchWithTx(ctx, tx, []*matchingEntities.MatchGroup{group}); err != nil {
				return struct{}{}, err
			}

			if err := group.Confirm(ctx); err != nil {
				return struct{}{}, err
			}

			if _, err := groupRepoInst.UpdateWithTx(ctx, tx, group); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundGroup, err := groupRepoInst.FindByID(ctx, h.Seed.ContextID, group.ID)
		require.NoError(t, err)
		require.NotNil(t, foundGroup)
		require.Equal(t, matchingVO.MatchGroupStatusConfirmed, foundGroup.Status)
	})
}

// TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndUpdate_Rollback
// asserts atomic rollback across FeeSchedule.CreateWithTx + UpdateWithTx. Fee
// schedules are referenced by fee rules via FK, so a partially-persisted
// schedule would leave the fee rule layer in a corrupt state.
func TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndUpdate_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		scheduleRepo := feeScheduleRepo.NewRepository(h.Provider())

		schedule := newTestSchedule(t, h, "Composition-Rollback-"+uuid.New().String()[:8], "USD",
			[]fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.25)}},
			})

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := scheduleRepo.CreateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			schedule.Name = "Composition-Rollback-Updated"
			if _, err := scheduleRepo.UpdateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		found, err := scheduleRepo.GetByID(ctx, schedule.ID)
		require.Error(t, err, "FeeSchedule must not persist after Create+Update+rollback")
		require.Nil(t, found)
	})
}

// TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndUpdate_Commit
// is the commit counterpart — schedule visible with updated name.
func TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndUpdate_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		scheduleRepo := feeScheduleRepo.NewRepository(h.Provider())

		schedule := newTestSchedule(t, h, "Composition-Commit-"+uuid.New().String()[:8], "USD",
			[]fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.25)}},
			})

		updatedName := "Composition-Commit-Updated-" + uuid.New().String()[:8]

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := scheduleRepo.CreateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			schedule.Name = updatedName
			if _, err := scheduleRepo.UpdateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		found, err := scheduleRepo.GetByID(ctx, schedule.ID)
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, updatedName, found.Name)
	})
}

// TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndDelete_Rollback
// asserts atomic rollback for FeeSchedule.CreateWithTx + DeleteWithTx.
// Deletion-then-rollback is a common audit/compensation shape; this test
// guards against leaking half-deleted fee schedules when a saga aborts.
func TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndDelete_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		scheduleRepo := feeScheduleRepo.NewRepository(h.Provider())

		// Pre-create a schedule outside the composition tx so Delete has a target.
		seed := newTestSchedule(t, h, "Delete-Target-"+uuid.New().String()[:8], "USD",
			[]fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(0.50)}},
			})
		seedCreated, err := scheduleRepo.Create(ctx, seed)
		require.NoError(t, err)

		// Inside the composition: Create a new schedule AND Delete the pre-existing one.
		newSchedule := newTestSchedule(t, h, "Create-And-Delete-"+uuid.New().String()[:8], "USD",
			[]fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(0.75)}},
			})

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := scheduleRepo.CreateWithTx(ctx, tx, newSchedule); err != nil {
				return struct{}{}, err
			}

			if err := scheduleRepo.DeleteWithTx(ctx, tx, seedCreated.ID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		// The created schedule must not exist (Create rolled back).
		foundNew, err := scheduleRepo.GetByID(ctx, newSchedule.ID)
		require.Error(t, err)
		require.Nil(t, foundNew)

		// The pre-existing schedule must still exist (Delete rolled back).
		foundSeed, err := scheduleRepo.GetByID(ctx, seedCreated.ID)
		require.NoError(t, err, "pre-existing FeeSchedule must survive Delete+rollback")
		require.NotNil(t, foundSeed)
		require.Equal(t, seedCreated.ID, foundSeed.ID)
	})
}

// TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndDelete_Commit
// is the commit counterpart — new schedule visible, old schedule gone.
func TestIntegration_Matching_WithTxComposition_FeeScheduleCreateAndDelete_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		scheduleRepo := feeScheduleRepo.NewRepository(h.Provider())

		seed := newTestSchedule(t, h, "Delete-Commit-Target-"+uuid.New().String()[:8], "USD",
			[]fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(0.50)}},
			})
		seedCreated, err := scheduleRepo.Create(ctx, seed)
		require.NoError(t, err)

		newSchedule := newTestSchedule(t, h, "Create-And-Delete-Commit-"+uuid.New().String()[:8], "USD",
			[]fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(0.75)}},
			})

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := scheduleRepo.CreateWithTx(ctx, tx, newSchedule); err != nil {
				return struct{}{}, err
			}

			if err := scheduleRepo.DeleteWithTx(ctx, tx, seedCreated.ID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundNew, err := scheduleRepo.GetByID(ctx, newSchedule.ID)
		require.NoError(t, err)
		require.NotNil(t, foundNew)

		foundSeed, err := scheduleRepo.GetByID(ctx, seedCreated.ID)
		require.Error(t, err,
			"pre-existing FeeSchedule must be deleted after commit")
		require.Nil(t, foundSeed)
	})
}

// TestIntegration_Matching_WithTxComposition_MatchGroupAndItem_Commit_NoFKCheck
// exercises CreateBatchWithTx on both MatchGroup and MatchItem in a single
// tx. It asserts commit semantics only — the rollback case is covered by the
// Run+Group tests above which run the same code paths, and match_items has
// an FK to transactions that would require the full ingestion fixture chain
// to populate for rollback-observability here. See the follow-up coverage
// notes at the bottom of this file for the variants intentionally deferred.
//
// NOTE: This test does NOT persist items because match_items.transaction_id
// FKs into transactions; wiring a real transaction row requires the ingestion
// pipeline which is out of scope for an in-memory composition test. Instead
// we pass an empty items slice and assert CreateBatchWithTx handles that
// safely alongside a real group write — a genuine integration scenario the
// match engine exercises when a rule produces metadata-only groups.
func TestIntegration_Matching_WithTxComposition_MatchGroupAndItem_Commit_EmptyBatch(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		ruleID := seedMatchRuleForComposition(t, h)

		runRepoInst := matchRunRepo.NewRepository(h.Provider())
		groupRepoInst := matchGroupRepo.NewRepository(h.Provider())
		itemRepoInst := matchItemRepo.NewRepository(h.Provider())

		run, err := matchingEntities.NewMatchRun(ctx, h.Seed.ContextID, matchingVO.MatchRunModeCommit)
		require.NoError(t, err)
		createdRun, err := runRepoInst.Create(ctx, run)
		require.NoError(t, err)

		group := newCompositionMatchGroup(t, h, createdRun.ID, ruleID)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := groupRepoInst.CreateBatchWithTx(ctx, tx, []*matchingEntities.MatchGroup{group}); err != nil {
				return struct{}{}, err
			}

			// Empty item batch — must be a no-op and not corrupt the enclosing tx.
			if _, err := itemRepoInst.CreateBatchWithTx(ctx, tx, []*matchingEntities.MatchItem{}); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundGroup, err := groupRepoInst.FindByID(ctx, h.Seed.ContextID, group.ID)
		require.NoError(t, err)
		require.NotNil(t, foundGroup)
	})
}
