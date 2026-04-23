//go:build integration

package configuration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	fieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	feeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

// buildCloneUseCase constructs a configCommand.UseCase wired to real Postgres
// repositories and with the InfrastructureProvider set for transactional clone.
func buildCloneUseCase(t *testing.T, h *integration.TestHarness) *configCommand.UseCase {
	t.Helper()

	provider := h.Provider()

	ctxRepo := contextRepo.NewRepository(provider)

	srcRepo, err := sourceRepo.NewRepository(provider)
	require.NoError(t, err)

	fmRepo := fieldMapRepo.NewRepository(provider)
	mrRepo := matchRuleRepo.NewRepository(provider)
	frRepo := configFeeRuleRepo.NewRepository(provider)

	uc, err := configCommand.NewUseCase(
		ctxRepo, srcRepo, fmRepo, mrRepo,
		configCommand.WithFeeRuleRepository(frRepo),
		configCommand.WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	return uc
}

// setupSourceContextWithChildren creates a reconciliation context in ACTIVE
// status together with one source, one field map, and one match rule. It
// returns the context ID so callers can use it as clone source.
func setupSourceContextWithChildren(t *testing.T, h *integration.TestHarness, uc *configCommand.UseCase) uuid.UUID {
	t.Helper()

	ctx := h.Ctx()
	provider := h.Provider()

	// --- context ---
	created, err := uc.CreateContext(ctx, h.Seed.TenantID, entities.CreateReconciliationContextInput{
		Name:     "Source Context " + uuid.New().String()[:8],
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.NoError(t, err)

	// Activate so clone sees an ACTIVE source context.
	status := value_objects.ContextStatusActive
	_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{Status: &status})
	require.NoError(t, err)

	// --- source ---
	srcRepo, err := sourceRepo.NewRepository(provider)
	require.NoError(t, err)

	src, err := entities.NewReconciliationSource(ctx, created.ID, entities.CreateReconciliationSourceInput{
		Name:   "Test Source",
		Type:   value_objects.SourceTypeLedger,
		Side:   fee.MatchingSideLeft,
		Config: map[string]any{},
	})
	require.NoError(t, err)

	createdSrc, err := srcRepo.Create(ctx, src)
	require.NoError(t, err)

	// --- field map ---
	fmRepo := fieldMapRepo.NewRepository(provider)

	fm, err := shared.NewFieldMap(ctx, created.ID, createdSrc.ID, shared.CreateFieldMapInput{
		Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
		},
	})
	require.NoError(t, err)

	_, err = fmRepo.Create(ctx, fm)
	require.NoError(t, err)

	// --- match rule ---
	mrRepo := matchRuleRepo.NewRepository(provider)

	rule, err := entities.NewMatchRule(ctx, created.ID, entities.CreateMatchRuleInput{
		Priority: 1,
		Type:     shared.RuleTypeExact,
		Config:   map[string]any{"matchAmount": true, "matchCurrency": true},
	})
	require.NoError(t, err)

	_, err = mrRepo.Create(ctx, rule)
	require.NoError(t, err)

	// --- fee schedule + fee rule ---
	fsRepo := feeScheduleRepo.NewRepository(provider)

	createdSchedule, err := fsRepo.Create(ctx, mustNewFeeSchedule(t, h.Seed.TenantID, "Clone Schedule"))
	require.NoError(t, err)

	frRepo := configFeeRuleRepo.NewRepository(provider)
	feeRule, err := fee.NewFeeRule(ctx, created.ID, createdSchedule.ID, fee.MatchingSideAny, "Clone Fee Rule", 1, []fee.FieldPredicate{
		{Field: "channel", Operator: fee.PredicateOperatorEquals, Value: "wire"},
		{Field: "brand", Operator: fee.PredicateOperatorIn, Values: []string{"visa", "mastercard"}},
	})
	require.NoError(t, err)

	err = frRepo.Create(ctx, feeRule)
	require.NoError(t, err)

	return created.ID
}

func mustNewFeeSchedule(t *testing.T, tenantID uuid.UUID, name string) *fee.FeeSchedule {
	t.Helper()

	schedule, err := fee.NewFeeSchedule(context.Background(), fee.NewFeeScheduleInput{
		TenantID:         tenantID,
		Name:             name + " " + uuid.New().String()[:8],
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItemInput{{
			Name:      "Processing Fee",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.50)},
		}},
	})
	require.NoError(t, err)

	return schedule
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCloneContext_FullClone(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := buildCloneUseCase(t, h)
		sourceContextID := setupSourceContextWithChildren(t, h, uc)
		ctx := h.Ctx()

		// Fetch the source context so we can compare type/interval later.
		ctxRepo := contextRepo.NewRepository(h.Provider())
		sourceContext, err := ctxRepo.FindByID(ctx, sourceContextID)
		require.NoError(t, err)
		frRepo := configFeeRuleRepo.NewRepository(h.Provider())
		sourceRules, err := frRepo.FindByContextID(ctx, sourceContextID)
		require.NoError(t, err)
		require.Len(t, sourceRules, 1)

		newName := "Cloned Full " + uuid.New().String()[:8]

		result, err := uc.CloneContext(ctx, configCommand.CloneContextInput{
			SourceContextID: sourceContextID,
			NewName:         newName,
			IncludeSources:  true,
			IncludeRules:    true,
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.Context)

		// Different ID.
		require.NotEqual(t, sourceContextID, result.Context.ID)

		// Same structural attributes.
		require.Equal(t, sourceContext.Type, result.Context.Type)
		require.Equal(t, sourceContext.Interval, result.Context.Interval)

		// New name applied.
		require.Equal(t, newName, result.Context.Name)

		// Clone creates an ACTIVE context.
		require.Equal(t, value_objects.ContextStatusActive, result.Context.Status)

		// Children were cloned.
		require.GreaterOrEqual(t, result.SourcesCloned, 1)
		require.GreaterOrEqual(t, result.RulesCloned, 1)
		require.GreaterOrEqual(t, result.FeeRulesCloned, 1)
		require.GreaterOrEqual(t, result.FieldMapsCloned, 1)

		clonedRules, err := frRepo.FindByContextID(ctx, result.Context.ID)
		require.NoError(t, err)
		require.Len(t, clonedRules, 1)
		require.Equal(t, sourceRules[0].Side, clonedRules[0].Side)
		require.Equal(t, sourceRules[0].Priority, clonedRules[0].Priority)
		require.Equal(t, sourceRules[0].Name, clonedRules[0].Name)
		require.Equal(t, sourceRules[0].FeeScheduleID, clonedRules[0].FeeScheduleID)
		require.Equal(t, sourceRules[0].Predicates, clonedRules[0].Predicates)
	})
}

func TestCloneContext_OnlySources(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := buildCloneUseCase(t, h)
		sourceContextID := setupSourceContextWithChildren(t, h, uc)
		ctx := h.Ctx()

		result, err := uc.CloneContext(ctx, configCommand.CloneContextInput{
			SourceContextID: sourceContextID,
			NewName:         "Cloned Sources Only " + uuid.New().String()[:8],
			IncludeSources:  true,
			IncludeRules:    false,
		})
		require.NoError(t, err)
		require.NotNil(t, result)

		// Sources and field maps cloned.
		require.GreaterOrEqual(t, result.SourcesCloned, 1)
		require.GreaterOrEqual(t, result.FieldMapsCloned, 1)

		// Rules NOT cloned.
		require.Equal(t, 0, result.RulesCloned)
		require.Equal(t, 0, result.FeeRulesCloned)
	})
}

func TestCloneContext_OnlyRules(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := buildCloneUseCase(t, h)
		sourceContextID := setupSourceContextWithChildren(t, h, uc)
		ctx := h.Ctx()

		result, err := uc.CloneContext(ctx, configCommand.CloneContextInput{
			SourceContextID: sourceContextID,
			NewName:         "Cloned Rules Only " + uuid.New().String()[:8],
			IncludeSources:  false,
			IncludeRules:    true,
		})
		require.NoError(t, err)
		require.NotNil(t, result)

		// Rules cloned.
		require.GreaterOrEqual(t, result.RulesCloned, 1)
		require.GreaterOrEqual(t, result.FeeRulesCloned, 1)

		// Sources NOT cloned.
		require.Equal(t, 0, result.SourcesCloned)
		require.Equal(t, 0, result.FieldMapsCloned)
	})
}

func TestCloneContext_EmptyClone(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := buildCloneUseCase(t, h)
		sourceContextID := setupSourceContextWithChildren(t, h, uc)
		ctx := h.Ctx()

		result, err := uc.CloneContext(ctx, configCommand.CloneContextInput{
			SourceContextID: sourceContextID,
			NewName:         "Cloned Empty " + uuid.New().String()[:8],
			IncludeSources:  false,
			IncludeRules:    false,
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.Context)

		// Only the context itself was created — no children.
		require.Equal(t, 0, result.SourcesCloned)
		require.Equal(t, 0, result.RulesCloned)
		require.Equal(t, 0, result.FeeRulesCloned)
		require.Equal(t, 0, result.FieldMapsCloned)
	})
}

// TestCloneContext_SourceSidePreservation verifies that cloned sources preserve
// their Side field (LEFT/RIGHT). This is H23 from the fee-rules-per-field review.
func TestCloneContext_SourceSidePreservation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := buildCloneUseCase(t, h)
		ctx := h.Ctx()
		provider := h.Provider()

		// --- Build a context with TWO sources: one LEFT, one RIGHT ---
		created, err := uc.CreateContext(ctx, h.Seed.TenantID, entities.CreateReconciliationContextInput{
			Name:     "Side Preservation " + uuid.New().String()[:8],
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		})
		require.NoError(t, err)

		status := value_objects.ContextStatusActive
		_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{Status: &status})
		require.NoError(t, err)

		srcRepo, err := sourceRepo.NewRepository(provider)
		require.NoError(t, err)

		leftSrc, err := entities.NewReconciliationSource(ctx, created.ID, entities.CreateReconciliationSourceInput{
			Name:   "Left Source",
			Type:   value_objects.SourceTypeLedger,
			Side:   fee.MatchingSideLeft,
			Config: map[string]any{},
		})
		require.NoError(t, err)

		createdLeft, err := srcRepo.Create(ctx, leftSrc)
		require.NoError(t, err)

		rightSrc, err := entities.NewReconciliationSource(ctx, created.ID, entities.CreateReconciliationSourceInput{
			Name:   "Right Source",
			Type:   value_objects.SourceTypeBank,
			Side:   fee.MatchingSideRight,
			Config: map[string]any{},
		})
		require.NoError(t, err)

		createdRight, err := srcRepo.Create(ctx, rightSrc)
		require.NoError(t, err)

		// Also add a field map so clone actually carries sources.
		fmRepo := fieldMapRepo.NewRepository(provider)

		fmLeft, err := shared.NewFieldMap(ctx, created.ID, createdLeft.ID, shared.CreateFieldMapInput{
			Mapping: map[string]any{"amount": "amount"},
		})
		require.NoError(t, err)
		_, err = fmRepo.Create(ctx, fmLeft)
		require.NoError(t, err)

		fmRight, err := shared.NewFieldMap(ctx, created.ID, createdRight.ID, shared.CreateFieldMapInput{
			Mapping: map[string]any{"amount": "amt"},
		})
		require.NoError(t, err)
		_, err = fmRepo.Create(ctx, fmRight)
		require.NoError(t, err)

		// --- Clone ---
		result, err := uc.CloneContext(ctx, configCommand.CloneContextInput{
			SourceContextID: created.ID,
			NewName:         "Cloned Sides " + uuid.New().String()[:8],
			IncludeSources:  true,
			IncludeRules:    false,
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.GreaterOrEqual(t, result.SourcesCloned, 2)

		// --- Verify cloned sources preserve Side ---
		clonedSources, _, err := srcRepo.FindByContextID(ctx, result.Context.ID, "", 100)
		require.NoError(t, err)
		require.Len(t, clonedSources, 2)

		// Build a map of name → side for easy assertion.
		sideByName := make(map[string]fee.MatchingSide, len(clonedSources))
		for _, s := range clonedSources {
			sideByName[s.Name] = s.Side
		}

		require.Equal(t, fee.MatchingSideLeft, sideByName["Left Source"],
			"cloned LEFT source should preserve Side=LEFT")
		require.Equal(t, fee.MatchingSideRight, sideByName["Right Source"],
			"cloned RIGHT source should preserve Side=RIGHT")
	})
}

func TestCloneContext_RequiresNewName(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := buildCloneUseCase(t, h)
		sourceContextID := setupSourceContextWithChildren(t, h, uc)
		ctx := h.Ctx()

		_, err := uc.CloneContext(ctx, configCommand.CloneContextInput{
			SourceContextID: sourceContextID,
			NewName:         "", // intentionally empty
			IncludeSources:  true,
			IncludeRules:    true,
		})
		require.ErrorIs(t, err, configCommand.ErrCloneNameRequired)
	})
}

func TestCloneContext_NonExistentSource(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := buildCloneUseCase(t, h)
		ctx := h.Ctx()

		_, err := uc.CloneContext(ctx, configCommand.CloneContextInput{
			SourceContextID: uuid.New(), // does not exist
			NewName:         "Should Fail " + uuid.New().String()[:8],
			IncludeSources:  true,
			IncludeRules:    true,
		})
		require.Error(t, err)
	})
}
