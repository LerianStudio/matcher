//go:build integration

package configuration

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	feeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

func createFeeRuleTestContext(t *testing.T, h *integration.TestHarness) uuid.UUID {
	t.Helper()

	repo := contextRepo.NewRepository(h.Provider())
	ctx := h.Ctx()

	entity, err := entities.NewReconciliationContext(ctx, h.Seed.TenantID, entities.CreateReconciliationContextInput{
		Name:     "Fee Rule Context " + uuid.New().String()[:8],
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.NoError(t, err)

	created, err := repo.Create(ctx, entity)
	require.NoError(t, err)

	return created.ID
}

func createFeeRuleTestSchedule(t *testing.T, h *integration.TestHarness, name string) *fee.FeeSchedule {
	t.Helper()

	repo := feeScheduleRepo.NewRepository(h.Provider())
	ctx := h.Ctx()

	schedule, err := fee.NewFeeSchedule(ctx, fee.NewFeeScheduleInput{
		TenantID:         h.Seed.TenantID,
		Name:             name + " " + uuid.New().String()[:8],
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItemInput{{
			Name:      "Processing Fee",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.25)},
		}},
	})
	require.NoError(t, err)

	created, err := repo.Create(ctx, schedule)
	require.NoError(t, err)

	return created
}

func TestFeeRuleRepository_CreateFindUpdateDelete(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := configFeeRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()
		contextID := createFeeRuleTestContext(t, h)
		scheduleA := createFeeRuleTestSchedule(t, h, "Rule Schedule A")
		scheduleB := createFeeRuleTestSchedule(t, h, "Rule Schedule B")

		rule, err := fee.NewFeeRule(ctx, contextID, scheduleA.ID, fee.MatchingSideAny, "Rule A", 2, []fee.FieldPredicate{{
			Field:    "institution",
			Operator: fee.PredicateOperatorEquals,
			Value:    "Itau",
		}})
		require.NoError(t, err)

		err = repo.Create(ctx, rule)
		require.NoError(t, err)

		fetched, err := repo.FindByID(ctx, rule.ID)
		require.NoError(t, err)
		require.Equal(t, rule.ID, fetched.ID)
		require.Equal(t, fee.MatchingSideAny, fetched.Side)
		require.Equal(t, 2, fetched.Priority)
		require.Equal(t, scheduleA.ID, fetched.FeeScheduleID)
		require.Len(t, fetched.Predicates, 1)
		require.Equal(t, fee.FieldPredicate{
			Field:    "institution",
			Operator: fee.PredicateOperatorEquals,
			Value:    "Itau",
		}, fetched.Predicates[0])

		secondRule, err := fee.NewFeeRule(ctx, contextID, scheduleB.ID, fee.MatchingSideRight, "Rule B", 1, nil)
		require.NoError(t, err)
		err = repo.Create(ctx, secondRule)
		require.NoError(t, err)

		listed, err := repo.FindByContextID(ctx, contextID)
		require.NoError(t, err)
		require.Len(t, listed, 2)
		require.Equal(t, secondRule.ID, listed[0].ID)
		require.Equal(t, rule.ID, listed[1].ID)

		newName := "Rule A Updated"
		newPriority := 0
		newScheduleID := scheduleB.ID.String()
		err = fetched.Update(ctx, fee.UpdateFeeRuleInput{
			Name:          &newName,
			Priority:      &newPriority,
			FeeScheduleID: &newScheduleID,
		})
		require.NoError(t, err)

		err = repo.Update(ctx, fetched)
		require.NoError(t, err)

		updated, err := repo.FindByID(ctx, rule.ID)
		require.NoError(t, err)
		require.Equal(t, newName, updated.Name)
		require.Equal(t, newPriority, updated.Priority)
		require.Equal(t, fee.MatchingSideAny, updated.Side)
		require.Equal(t, scheduleB.ID, updated.FeeScheduleID)
		require.Len(t, updated.Predicates, 1)
		require.Equal(t, fetched.Predicates, updated.Predicates)

		err = repo.Delete(ctx, contextID, secondRule.ID)
		require.NoError(t, err)

		_, err = repo.FindByID(ctx, secondRule.ID)
		require.ErrorIs(t, err, fee.ErrFeeRuleNotFound)
	})
}

func TestFeeRuleRepository_RejectsMissingFeeSchedule(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := configFeeRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()
		contextID := createFeeRuleTestContext(t, h)

		rule, err := fee.NewFeeRule(ctx, contextID, uuid.New(), fee.MatchingSideAny, "Missing Schedule", 1, nil)
		require.NoError(t, err)

		err = repo.Create(ctx, rule)
		require.Error(t, err)
		require.Contains(t, err.Error(), "insert fee rule")
	})
}

func TestFeeRuleRepository_RejectsDuplicatePriorityAndNameWithinContext(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := configFeeRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()
		contextID := createFeeRuleTestContext(t, h)
		schedule := createFeeRuleTestSchedule(t, h, "Duplicate Guard")

		baseRule, err := fee.NewFeeRule(ctx, contextID, schedule.ID, fee.MatchingSideAny, "Unique Rule", 10, nil)
		require.NoError(t, err)
		require.NoError(t, repo.Create(ctx, baseRule))

		t.Run("duplicate priority", func(t *testing.T) {
			rule, err := fee.NewFeeRule(ctx, contextID, schedule.ID, fee.MatchingSideLeft, "Another Rule", 10, nil)
			require.NoError(t, err)

			err = repo.Create(ctx, rule)
			require.Error(t, err)
			assertPgConstraintName(t, err, "uq_fee_rules_context_priority")
		})

		t.Run("duplicate name", func(t *testing.T) {
			rule, err := fee.NewFeeRule(ctx, contextID, schedule.ID, fee.MatchingSideRight, "Unique Rule", 11, nil)
			require.NoError(t, err)

			err = repo.Create(ctx, rule)
			require.Error(t, err)
			assertPgConstraintName(t, err, "uq_fee_rules_context_name")
		})
	})
}

func TestFeeRuleRepository_AllowsDuplicatePriorityAndNameAcrossContexts(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := configFeeRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()
		contextA := createFeeRuleTestContext(t, h)
		contextB := createFeeRuleTestContext(t, h)
		schedule := createFeeRuleTestSchedule(t, h, "Shared Schedule")

		ruleA, err := fee.NewFeeRule(ctx, contextA, schedule.ID, fee.MatchingSideAny, "Shared Rule", 7, nil)
		require.NoError(t, err)
		require.NoError(t, repo.Create(ctx, ruleA))

		ruleB, err := fee.NewFeeRule(ctx, contextB, schedule.ID, fee.MatchingSideLeft, "Shared Rule", 7, nil)
		require.NoError(t, err)
		require.NoError(t, repo.Create(ctx, ruleB))
	})
}

func TestFeeRuleRepository_ConcurrentDuplicatePriority(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := configFeeRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()
		contextID := createFeeRuleTestContext(t, h)
		schedule := createFeeRuleTestSchedule(t, h, "Concurrent Duplicate Priority")

		buildRule := func(name string) *fee.FeeRule {
			rule, err := fee.NewFeeRule(ctx, contextID, schedule.ID, fee.MatchingSideAny, name, 5, nil)
			require.NoError(t, err)
			return rule
		}

		rules := []*fee.FeeRule{
			buildRule("Concurrent Rule A"),
			buildRule("Concurrent Rule B"),
		}

		errs := make([]error, len(rules))
		var wg sync.WaitGroup
		wg.Add(len(rules))

		for i, rule := range rules {
			go func(index int, r *fee.FeeRule) {
				defer wg.Done()
				errs[index] = repo.Create(ctx, r)
			}(i, rule)
		}

		wg.Wait()

		successes := 0
		constraintFailures := 0
		for _, err := range errs {
			if err == nil {
				successes++
				continue
			}

			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, "23505", pgErr.Code)
			require.Equal(t, "uq_fee_rules_context_priority", pgErr.ConstraintName)
			constraintFailures++
		}

		require.Equal(t, 1, successes)
		require.Equal(t, 1, constraintFailures)
	})
}

// TestFeeRuleRepository_CrossTenantIsolation verifies that fee rules created
// in one tenant's schema are invisible to another tenant. This is H21 from
// the fee-rules-per-field feature review.
//
// The matcher uses PostgreSQL schema-based tenant isolation: each tenant
// operates within its own search_path set by WithTenantTxProvider. Creating
// a separate schema requires DDL (CREATE SCHEMA + table replication), which
// the standard test harness does not support out-of-the-box.
//
// When multi-schema harness support is available, this test should:
//  1. Create tenant A schema and insert a fee rule there.
//  2. Switch to tenant B schema via context value.
//  3. Assert FindByID(tenantA_rule.ID) returns ErrFeeRuleNotFound.
//  4. Assert FindByContextID(tenantA_context.ID) returns empty slice.
func TestFeeRuleRepository_CrossTenantIsolation(t *testing.T) {
	t.Parallel()

	t.Skip("requires multi-schema harness: the shared integration harness operates " +
		"in a single 'public' schema; cross-tenant isolation for fee rules needs " +
		"separate PostgreSQL schemas with replicated DDL — see " +
		"tests/integration/auth/tenant_isolation_test.go for the pattern")
}

func assertPgConstraintName(t *testing.T, err error, expected string) {
	t.Helper()

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, expected, pgErr.ConstraintName)
	require.Equal(t, "23505", pgErr.Code)
}
