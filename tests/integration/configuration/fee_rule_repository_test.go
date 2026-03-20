//go:build integration

package configuration

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	feeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

func createFeeRuleTestContext(t *testing.T, h *integration.TestHarness) uuid.UUID {
	t.Helper()

	repo := contextRepo.NewRepository(h.Provider())
	ctx := h.Ctx()

	entity, err := entities.NewReconciliationContext(ctx, h.Seed.TenantID, entities.CreateReconciliationContextInput{
		Name:     "Fee Rule Context " + uuid.New().String()[:8],
		Type:     value_objects.ContextTypeOneToOne,
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
		require.Equal(t, scheduleA.ID, fetched.FeeScheduleID)

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
		require.Equal(t, scheduleB.ID, updated.FeeScheduleID)

		err = repo.Delete(ctx, secondRule.ID)
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

func assertPgConstraintName(t *testing.T, err error, expected string) {
	t.Helper()

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, expected, pgErr.ConstraintName)
	require.Equal(t, "23505", pgErr.Code)
}
