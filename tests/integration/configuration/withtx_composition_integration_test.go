//go:build integration

// Package configuration contains integration tests for the configuration
// context's WithTx repository surface under multi-aggregate composition.
//
// Configuration aggregates (Context / Source / MatchRule / FeeRule / FieldMap /
// Schedule) are composition-heavy because onboarding workflows (CreateContext
// with sources + rules, CloneContext, UpdateContext with cascading fee rule
// updates) write across 2–5 aggregates inside a single tx. These tests verify
// that atomicity holds under both commit and deliberate rollback.
//
// Covers FINDING-042 (REFACTOR-051).
package configuration

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	feeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	fieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	scheduleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/schedule"
	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	feeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

// errDeliberateRollback forces the composition callback to abort, triggering
// tx rollback so the test can verify atomic undo across all repos involved.
var errDeliberateRollback = errors.New("deliberate rollback for composition test")

// newExactMatchRule builds a minimal valid EXACT-type match rule bound to the
// given context. Priority is randomized to avoid the UNIQUE(context_id,
// priority) constraint across tests that share the seeded context.
func newExactMatchRule(t *testing.T, ctx context.Context, contextIDVal uuid.UUID, priority int) *entities.MatchRule {
	t.Helper()

	rule, err := entities.NewMatchRule(
		ctx,
		contextIDVal,
		entities.CreateMatchRuleInput{
			Priority: priority,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true, "matchAmount": true},
		},
	)
	require.NoError(t, err)

	return rule
}

// TestIntegration_Configuration_WithTxComposition_ContextCreateAndUpdate_Rollback
// asserts that CreateWithTx + UpdateWithTx on ReconciliationContext inside a
// single tx roll back atomically. Context status transitions (DRAFT → ACTIVE
// on creation) are common; losing either half would leave a context in a
// dangling state.
func TestIntegration_Configuration_WithTxComposition_ContextCreateAndUpdate_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		cRepo := contextRepo.NewRepository(h.Provider())

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Composition-CreateUpdate-" + uuid.New().String()[:8],
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := cRepo.CreateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			// Transition from DRAFT → ACTIVE within the same tx.
			if err := entity.Activate(ctx); err != nil {
				return struct{}{}, err
			}

			if _, err := cRepo.UpdateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		found, err := cRepo.FindByID(ctx, entity.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"ReconciliationContext must not persist after Create+Update+rollback")
		require.Nil(t, found)
	})
}

// TestIntegration_Configuration_WithTxComposition_ContextCreateAndUpdate_Commit
// is the commit counterpart — the context is persisted in ACTIVE status.
func TestIntegration_Configuration_WithTxComposition_ContextCreateAndUpdate_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		cRepo := contextRepo.NewRepository(h.Provider())

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Composition-CreateUpdateCommit-" + uuid.New().String()[:8],
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := cRepo.CreateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			if err := entity.Activate(ctx); err != nil {
				return struct{}{}, err
			}

			if _, err := cRepo.UpdateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		found, err := cRepo.FindByID(ctx, entity.ID)
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, configVO.ContextStatusActive, found.Status)
	})
}

// TestIntegration_Configuration_WithTxComposition_ContextAndSource_Rollback
// asserts that a new context + its first source, created atomically via
// Context.CreateWithTx + Source.CreateWithTx, roll back together. This is
// the critical onboarding path: partial context without source would leave
// a context no user could use.
func TestIntegration_Configuration_WithTxComposition_ContextAndSource_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		cRepo := contextRepo.NewRepository(h.Provider())
		sRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		contextEntity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Composition-CtxSource-" + uuid.New().String()[:8],
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		sourceEntity, err := entities.NewReconciliationSource(
			ctx,
			contextEntity.ID,
			entities.CreateReconciliationSourceInput{
				Name:   "Composition-Source-" + uuid.New().String()[:8],
				Type:   configVO.SourceTypeBank,
				Side:   fee.MatchingSideRight,
				Config: map[string]any{"format": "csv"},
			},
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := cRepo.CreateWithTx(ctx, tx, contextEntity); err != nil {
				return struct{}{}, err
			}

			if _, err := sRepo.CreateWithTx(ctx, tx, sourceEntity); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundCtx, err := cRepo.FindByID(ctx, contextEntity.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"ReconciliationContext must not persist after rollback")
		require.Nil(t, foundCtx)

		foundSrc, err := sRepo.FindByID(ctx, contextEntity.ID, sourceEntity.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"ReconciliationSource must not persist after rollback")
		require.Nil(t, foundSrc)
	})
}

// TestIntegration_Configuration_WithTxComposition_ContextAndSource_Commit is
// the commit counterpart — both context and source visible.
func TestIntegration_Configuration_WithTxComposition_ContextAndSource_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		cRepo := contextRepo.NewRepository(h.Provider())
		sRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		contextEntity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Composition-CtxSourceCommit-" + uuid.New().String()[:8],
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		sourceEntity, err := entities.NewReconciliationSource(
			ctx,
			contextEntity.ID,
			entities.CreateReconciliationSourceInput{
				Name:   "Composition-SourceCommit-" + uuid.New().String()[:8],
				Type:   configVO.SourceTypeBank,
				Side:   fee.MatchingSideRight,
				Config: map[string]any{"format": "csv"},
			},
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := cRepo.CreateWithTx(ctx, tx, contextEntity); err != nil {
				return struct{}{}, err
			}

			if _, err := sRepo.CreateWithTx(ctx, tx, sourceEntity); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundCtx, err := cRepo.FindByID(ctx, contextEntity.ID)
		require.NoError(t, err)
		require.NotNil(t, foundCtx)

		foundSrc, err := sRepo.FindByID(ctx, contextEntity.ID, sourceEntity.ID)
		require.NoError(t, err)
		require.NotNil(t, foundSrc)
	})
}

// TestIntegration_Configuration_WithTxComposition_SourceAndFieldMap_Rollback
// asserts that a new source + its field map roll back atomically. This is
// the onboarding path for an already-created context, and losing only the
// field map would render the source unparseable.
func TestIntegration_Configuration_WithTxComposition_SourceAndFieldMap_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		sRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		fmRepo := fieldMapRepo.NewRepository(h.Provider())

		source, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Composition-SrcFM-" + uuid.New().String()[:8],
				Type:   configVO.SourceTypeBank,
				Side:   fee.MatchingSideRight,
				Config: map[string]any{"format": "csv"},
			},
		)
		require.NoError(t, err)

		fieldMap, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			source.ID,
			shared.CreateFieldMapInput{Mapping: map[string]any{
				"external_id": "id",
				"amount":      "amount",
				"currency":    "currency",
			}},
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := sRepo.CreateWithTx(ctx, tx, source); err != nil {
				return struct{}{}, err
			}

			if _, err := fmRepo.CreateWithTx(ctx, tx, fieldMap); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundSrc, err := sRepo.FindByID(ctx, h.Seed.ContextID, source.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"ReconciliationSource must not persist after rollback")
		require.Nil(t, foundSrc)

		foundFM, err := fmRepo.FindByID(ctx, fieldMap.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"FieldMap must not persist after rollback")
		require.Nil(t, foundFM)
	})
}

// TestIntegration_Configuration_WithTxComposition_SourceAndFieldMap_Commit is
// the commit counterpart — source and field map both visible.
func TestIntegration_Configuration_WithTxComposition_SourceAndFieldMap_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		sRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		fmRepo := fieldMapRepo.NewRepository(h.Provider())

		source, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Composition-SrcFMCommit-" + uuid.New().String()[:8],
				Type:   configVO.SourceTypeBank,
				Side:   fee.MatchingSideRight,
				Config: map[string]any{"format": "csv"},
			},
		)
		require.NoError(t, err)

		fieldMap, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			source.ID,
			shared.CreateFieldMapInput{Mapping: map[string]any{
				"external_id": "id",
				"amount":      "amount",
				"currency":    "currency",
			}},
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := sRepo.CreateWithTx(ctx, tx, source); err != nil {
				return struct{}{}, err
			}

			if _, err := fmRepo.CreateWithTx(ctx, tx, fieldMap); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundSrc, err := sRepo.FindByID(ctx, h.Seed.ContextID, source.ID)
		require.NoError(t, err)
		require.NotNil(t, foundSrc)

		foundFM, err := fmRepo.FindByID(ctx, fieldMap.ID)
		require.NoError(t, err)
		require.NotNil(t, foundFM)
	})
}

// TestIntegration_Configuration_WithTxComposition_SourceCreateAndUpdate_Rollback
// asserts atomic rollback for Source.CreateWithTx + Source.UpdateWithTx on
// the same aggregate — the "create then immediately rename" pattern, common
// during context cloning.
func TestIntegration_Configuration_WithTxComposition_SourceCreateAndUpdate_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		sRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		source, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Composition-Orig-" + uuid.New().String()[:8],
				Type:   configVO.SourceTypeBank,
				Side:   fee.MatchingSideRight,
				Config: map[string]any{"format": "csv"},
			},
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := sRepo.CreateWithTx(ctx, tx, source); err != nil {
				return struct{}{}, err
			}

			newName := "Renamed-" + uuid.New().String()[:8]
			if err := source.Update(ctx, entities.UpdateReconciliationSourceInput{Name: &newName}); err != nil {
				return struct{}{}, err
			}

			if _, err := sRepo.UpdateWithTx(ctx, tx, source); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundSrc, err := sRepo.FindByID(ctx, h.Seed.ContextID, source.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"ReconciliationSource must not persist after Create+Update+rollback")
		require.Nil(t, foundSrc)
	})
}

// TestIntegration_Configuration_WithTxComposition_SourceCreateAndUpdate_Commit
// is the commit counterpart — source visible with updated name.
func TestIntegration_Configuration_WithTxComposition_SourceCreateAndUpdate_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		sRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		source, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Composition-OrigCommit-" + uuid.New().String()[:8],
				Type:   configVO.SourceTypeBank,
				Side:   fee.MatchingSideRight,
				Config: map[string]any{"format": "csv"},
			},
		)
		require.NoError(t, err)

		newName := "RenamedCommit-" + uuid.New().String()[:8]

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := sRepo.CreateWithTx(ctx, tx, source); err != nil {
				return struct{}{}, err
			}

			if err := source.Update(ctx, entities.UpdateReconciliationSourceInput{Name: &newName}); err != nil {
				return struct{}{}, err
			}

			if _, err := sRepo.UpdateWithTx(ctx, tx, source); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundSrc, err := sRepo.FindByID(ctx, h.Seed.ContextID, source.ID)
		require.NoError(t, err)
		require.NotNil(t, foundSrc)
		require.Equal(t, newName, foundSrc.Name)
	})
}

// TestIntegration_Configuration_WithTxComposition_FieldMapAndMatchRule_Rollback
// asserts that FieldMap.CreateWithTx + MatchRule.CreateWithTx composed in one
// tx roll back atomically. Both aggregates reference the context, and
// creating rules without field maps (or vice versa) leaves the context in
// an unmatcheable state.
func TestIntegration_Configuration_WithTxComposition_FieldMapAndMatchRule_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		fmRepo := fieldMapRepo.NewRepository(h.Provider())
		mrRepo := matchRuleRepo.NewRepository(h.Provider())

		fieldMap, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			shared.CreateFieldMapInput{Mapping: map[string]any{
				"external_id": "id",
				"amount":      "amount",
			}},
		)
		require.NoError(t, err)

		rule := newExactMatchRule(t, ctx, h.Seed.ContextID, 1+int(uuid.New().ID()%999))

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := fmRepo.CreateWithTx(ctx, tx, fieldMap); err != nil {
				return struct{}{}, err
			}

			if _, err := mrRepo.CreateWithTx(ctx, tx, rule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundFM, err := fmRepo.FindByID(ctx, fieldMap.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"FieldMap must not persist after rollback")
		require.Nil(t, foundFM)

		foundRule, err := mrRepo.FindByID(ctx, h.Seed.ContextID, rule.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"MatchRule must not persist after rollback")
		require.Nil(t, foundRule)
	})
}

// TestIntegration_Configuration_WithTxComposition_FieldMapAndMatchRule_Commit
// is the commit counterpart — both visible.
func TestIntegration_Configuration_WithTxComposition_FieldMapAndMatchRule_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		fmRepo := fieldMapRepo.NewRepository(h.Provider())
		mrRepo := matchRuleRepo.NewRepository(h.Provider())

		// Different source to avoid UNIQUE(context_id, source_id) collision
		// with other tests that exercise the seeded source.
		sRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)
		tmpSrc, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "tmp-" + uuid.New().String()[:8],
				Type:   configVO.SourceTypeBank,
				Side:   fee.MatchingSideRight,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		createdSrc, err := sRepo.Create(ctx, tmpSrc)
		require.NoError(t, err)

		fieldMap, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			createdSrc.ID,
			shared.CreateFieldMapInput{Mapping: map[string]any{
				"external_id": "id",
				"amount":      "amount",
			}},
		)
		require.NoError(t, err)

		rule := newExactMatchRule(t, ctx, h.Seed.ContextID, 1+int(uuid.New().ID()%999))

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := fmRepo.CreateWithTx(ctx, tx, fieldMap); err != nil {
				return struct{}{}, err
			}

			if _, err := mrRepo.CreateWithTx(ctx, tx, rule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundFM, err := fmRepo.FindByID(ctx, fieldMap.ID)
		require.NoError(t, err)
		require.NotNil(t, foundFM)

		foundRule, err := mrRepo.FindByID(ctx, h.Seed.ContextID, rule.ID)
		require.NoError(t, err)
		require.NotNil(t, foundRule)
	})
}

// TestIntegration_Configuration_WithTxComposition_FeeScheduleAndFeeRule_Rollback
// asserts atomic rollback across matching.FeeSchedule.CreateWithTx +
// configuration.FeeRule.CreateWithTx. Fee rules FK into fee schedules; a
// partially-persisted fee schedule with no rule (or a rule pointing at a
// rolled-back schedule) breaks fee matching.
//
// This test crosses a module boundary (matching fee_schedule repo +
// configuration fee_rule repo) — the scenario our engine depends on for
// "configure NET fee reconciliation" workflows.
func TestIntegration_Configuration_WithTxComposition_FeeScheduleAndFeeRule_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		fsRepo := feeScheduleRepo.NewRepository(h.Provider())
		frRepo := feeRuleRepo.NewRepository(h.Provider())

		schedule, err := fee.NewFeeSchedule(ctx, fee.NewFeeScheduleInput{
			TenantID:         h.Seed.TenantID,
			Name:             "CompositionFR-" + uuid.New().String()[:8],
			Currency:         "USD",
			ApplicationOrder: fee.ApplicationOrderParallel,
			RoundingScale:    2,
			RoundingMode:     fee.RoundingModeHalfUp,
			Items: []fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(0.25)}},
			},
		})
		require.NoError(t, err)

		feeRule, err := fee.NewFeeRule(
			ctx,
			h.Seed.ContextID,
			schedule.ID,
			fee.MatchingSideRight,
			"CompositionFR-"+uuid.New().String()[:8],
			50+int(uuid.New().ID()%1000),
			nil,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := fsRepo.CreateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			if err := frRepo.CreateWithTx(ctx, tx, feeRule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		foundSchedule, err := fsRepo.GetByID(ctx, schedule.ID)
		require.Error(t, err,
			"FeeSchedule must not persist after rollback")
		require.Nil(t, foundSchedule)

		foundRule, err := frRepo.FindByID(ctx, feeRule.ID)
		require.Error(t, err,
			"FeeRule must not persist after rollback")
		require.Nil(t, foundRule)
	})
}

// TestIntegration_Configuration_WithTxComposition_FeeScheduleAndFeeRule_Commit
// is the commit counterpart — both visible.
func TestIntegration_Configuration_WithTxComposition_FeeScheduleAndFeeRule_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		fsRepo := feeScheduleRepo.NewRepository(h.Provider())
		frRepo := feeRuleRepo.NewRepository(h.Provider())

		schedule, err := fee.NewFeeSchedule(ctx, fee.NewFeeScheduleInput{
			TenantID:         h.Seed.TenantID,
			Name:             "CompositionFRCommit-" + uuid.New().String()[:8],
			Currency:         "USD",
			ApplicationOrder: fee.ApplicationOrderParallel,
			RoundingScale:    2,
			RoundingMode:     fee.RoundingModeHalfUp,
			Items: []fee.FeeScheduleItemInput{
				{Name: "Flat", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromFloat(0.25)}},
			},
		})
		require.NoError(t, err)

		feeRule, err := fee.NewFeeRule(
			ctx,
			h.Seed.ContextID,
			schedule.ID,
			fee.MatchingSideRight,
			"CompositionFRCommit-"+uuid.New().String()[:8],
			2000+int(uuid.New().ID()%1000),
			nil,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := fsRepo.CreateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			if err := frRepo.CreateWithTx(ctx, tx, feeRule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundSchedule, err := fsRepo.GetByID(ctx, schedule.ID)
		require.NoError(t, err)
		require.NotNil(t, foundSchedule)

		foundRule, err := frRepo.FindByID(ctx, feeRule.ID)
		require.NoError(t, err)
		require.NotNil(t, foundRule)
	})
}

// TestIntegration_Configuration_WithTxComposition_ScheduleCreateAndUpdate_Rollback
// asserts atomic rollback across Schedule.CreateWithTx + Schedule.UpdateWithTx
// on the same aggregate. NextRunAt is recomputed on update; a partial rollback
// would leave the schedule with an incorrect next-run time.
func TestIntegration_Configuration_WithTxComposition_ScheduleCreateAndUpdate_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		sRepo := scheduleRepo.NewRepository(h.Provider())

		schedule, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{CronExpression: "0 0 * * *"},
		)
		require.NoError(t, err)

		newCron := "0 6 * * *"

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := sRepo.CreateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			if err := schedule.Update(ctx, entities.UpdateScheduleInput{CronExpression: &newCron}); err != nil {
				return struct{}{}, err
			}

			if _, err := sRepo.UpdateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		found, err := sRepo.FindByID(ctx, schedule.ID)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"ReconciliationSchedule must not persist after Create+Update+rollback")
		require.Nil(t, found)
	})
}

// TestIntegration_Configuration_WithTxComposition_ScheduleCreateAndUpdate_Commit
// is the commit counterpart — schedule visible with updated cron expression.
func TestIntegration_Configuration_WithTxComposition_ScheduleCreateAndUpdate_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		sRepo := scheduleRepo.NewRepository(h.Provider())

		schedule, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{CronExpression: "0 0 * * *"},
		)
		require.NoError(t, err)

		newCron := "0 6 * * *"

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := sRepo.CreateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			if err := schedule.Update(ctx, entities.UpdateScheduleInput{CronExpression: &newCron}); err != nil {
				return struct{}{}, err
			}

			if _, err := sRepo.UpdateWithTx(ctx, tx, schedule); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		found, err := sRepo.FindByID(ctx, schedule.ID)
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, newCron, found.CronExpression)
	})
}
