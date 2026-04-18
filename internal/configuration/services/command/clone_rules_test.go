//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// ---------------------------------------------------------------------------
// Transactional-read stub for match rules (FindByContextIDWithTx)
// ---------------------------------------------------------------------------

type matchRuleRepoTxFinderStub struct {
	*matchRuleRepoTxStub
	findByContextIDWithTxFn func(context.Context, *sql.Tx, uuid.UUID, string, int) (entities.MatchRules, libHTTP.CursorPagination, error)
}

var _ matchRuleTxFinder = (*matchRuleRepoTxFinderStub)(nil)

func (stub *matchRuleRepoTxFinderStub) FindByContextIDWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if stub.findByContextIDWithTxFn != nil {
		return stub.findByContextIDWithTxFn(ctx, tx, contextID, cursor, limit)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextNotImplemented
}

// ---------------------------------------------------------------------------
// Transactional-read stub for fee rules (FindByContextIDWithTx)
// ---------------------------------------------------------------------------

type feeRuleRepoTxStub struct {
	*feeRuleRepoStub
	createWithTxFn          func(context.Context, *sql.Tx, *fee.FeeRule) error
	findByContextIDWithTxFn func(context.Context, *sql.Tx, uuid.UUID) ([]*fee.FeeRule, error)
}

var (
	_ feeRuleTxCreator = (*feeRuleRepoTxStub)(nil)
	_ feeRuleTxFinder  = (*feeRuleRepoTxStub)(nil)
)

func (stub *feeRuleRepoTxStub) CreateWithTx(ctx context.Context, tx *sql.Tx, rule *fee.FeeRule) error {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, rule)
	}

	return errCreateNotImplemented
}

func (stub *feeRuleRepoTxStub) FindByContextIDWithTx(ctx context.Context, tx *sql.Tx, contextID uuid.UUID) ([]*fee.FeeRule, error) {
	if stub.findByContextIDWithTxFn != nil {
		return stub.findByContextIDWithTxFn(ctx, tx, contextID)
	}

	return nil, errFindFeeRulesNotImplemented
}

// ===========================================================================
// cloneMatchRules (non-transactional path)
// ===========================================================================

func TestCloneMatchRules_Success(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()

	rules := entities.MatchRules{
		{ID: uuid.New(), ContextID: sourceCtxID, Priority: 1, Type: value_objects.RuleTypeExact, Config: map[string]any{"matchAmount": true}},
		{ID: uuid.New(), ContextID: sourceCtxID, Priority: 2, Type: value_objects.RuleTypeTolerance, Config: map[string]any{"absTolerance": 0.01}},
	}

	createdRules := make([]*entities.MatchRule, 0, 2)

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, ctxID uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			assert.Equal(t, sourceCtxID, ctxID)
			return rules, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, rule *entities.MatchRule) (*entities.MatchRule, error) {
			createdRules = append(createdRules, rule)
			return rule, nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
	}

	cloned, err := uc.cloneMatchRules(context.Background(), sourceCtxID, newCtxID)

	require.NoError(t, err)
	assert.Equal(t, 2, cloned)
	require.Len(t, createdRules, 2)

	for _, rule := range createdRules {
		assert.Equal(t, newCtxID, rule.ContextID)
		assert.NotEqual(t, uuid.Nil, rule.ID)
		assert.False(t, rule.CreatedAt.IsZero())
	}

	assert.Equal(t, 1, createdRules[0].Priority)
	assert.Equal(t, 2, createdRules[1].Priority)
}

func TestCloneMatchRules_NoRulesReturnsZero(t *testing.T) {
	t.Parallel()

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
	}

	cloned, err := uc.cloneMatchRules(context.Background(), uuid.New(), uuid.New())

	require.NoError(t, err)
	assert.Equal(t, 0, cloned)
}

func TestCloneMatchRules_FetchError(t *testing.T) {
	t.Parallel()

	errFetch := errors.New("database connection lost")

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errFetch
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
	}

	cloned, err := uc.cloneMatchRules(context.Background(), uuid.New(), uuid.New())

	require.Error(t, err)
	assert.Equal(t, 0, cloned)
	assert.ErrorIs(t, err, errFetch)
}

func TestCloneMatchRules_CreateErrorPartialClone(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()
	errCreate := errors.New("constraint violation")

	callCount := 0

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return entities.MatchRules{
				{ID: uuid.New(), ContextID: sourceCtxID, Priority: 1, Type: value_objects.RuleTypeExact, Config: map[string]any{}},
				{ID: uuid.New(), ContextID: sourceCtxID, Priority: 2, Type: value_objects.RuleTypeTolerance, Config: map[string]any{}},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, _ *entities.MatchRule) (*entities.MatchRule, error) {
			callCount++
			if callCount == 2 {
				return nil, errCreate
			}

			return &entities.MatchRule{}, nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
	}

	cloned, err := uc.cloneMatchRules(context.Background(), sourceCtxID, newCtxID)

	require.Error(t, err)
	assert.Equal(t, 1, cloned)
	assert.ErrorIs(t, err, errCreate)
	assert.Contains(t, err.Error(), "creating cloned rule (priority 2)")
}

func TestCloneMatchRules_ClonesConfigDeepCopy(t *testing.T) {
	t.Parallel()

	originalConfig := map[string]any{
		"nested": map[string]any{"deep": "value"},
	}

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return entities.MatchRules{
				{ID: uuid.New(), ContextID: uuid.New(), Priority: 1, Type: value_objects.RuleTypeExact, Config: originalConfig},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, rule *entities.MatchRule) (*entities.MatchRule, error) {
			// Mutate the cloned config to verify deep copy
			rule.Config["nested"].(map[string]any)["deep"] = "mutated"
			return rule, nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
	}

	_, err := uc.cloneMatchRules(context.Background(), uuid.New(), uuid.New())

	require.NoError(t, err)
	// Original must be untouched
	assert.Equal(t, "value", originalConfig["nested"].(map[string]any)["deep"])
}

// ===========================================================================
// cloneMatchRulesWithTx (transactional path)
// ===========================================================================

func TestCloneMatchRulesWithTx_UsesTxCreator(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()
	txCreateCalls := 0

	ruleRepoTx := &matchRuleRepoTxFinderStub{
		matchRuleRepoTxStub: &matchRuleRepoTxStub{
			matchRuleRepoStub: &matchRuleRepoStub{},
			createWithTxFn: func(_ context.Context, _ *sql.Tx, rule *entities.MatchRule) (*entities.MatchRule, error) {
				txCreateCalls++
				assert.Equal(t, newCtxID, rule.ContextID)
				return rule, nil
			},
		},
		findByContextIDWithTxFn: func(_ context.Context, _ *sql.Tx, ctxID uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			assert.Equal(t, sourceCtxID, ctxID)
			return entities.MatchRules{
				{ID: uuid.New(), ContextID: sourceCtxID, Priority: 1, Type: value_objects.RuleTypeExact, Config: map[string]any{}},
			}, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepoTx,
	}

	fakeTx := &sql.Tx{}

	cloned, err := uc.cloneMatchRulesWithTx(context.Background(), fakeTx, sourceCtxID, newCtxID)

	require.NoError(t, err)
	assert.Equal(t, 1, cloned)
	assert.Equal(t, 1, txCreateCalls)
}

func TestCloneMatchRulesWithTx_RepoDoesNotSupportTxCreate(t *testing.T) {
	t.Parallel()

	// Use a plain matchRuleRepoStub that does NOT implement matchRuleTxCreator.
	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return entities.MatchRules{
				{ID: uuid.New(), ContextID: uuid.New(), Priority: 1, Type: value_objects.RuleTypeExact, Config: map[string]any{}},
			}, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
	}

	fakeTx := &sql.Tx{}

	_, err := uc.cloneMatchRulesWithTx(context.Background(), fakeTx, uuid.New(), uuid.New())

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCloneProviderRequired)
}

// ===========================================================================
// cloneFeeRules (non-transactional path)
// ===========================================================================

func TestCloneFeeRules_Success(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()

	feeRuleID := uuid.New()
	scheduleID := uuid.New()

	feeRules := []*fee.FeeRule{
		{
			ID:            feeRuleID,
			ContextID:     sourceCtxID,
			FeeScheduleID: scheduleID,
			Side:          fee.MatchingSideLeft,
			Name:          "Fee Rule A",
			Priority:      1,
			Predicates: []fee.FieldPredicate{
				{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
			},
		},
	}

	var createdFeeRules []*fee.FeeRule

	feeRepo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, ctxID uuid.UUID) ([]*fee.FeeRule, error) {
			assert.Equal(t, sourceCtxID, ctxID)
			return feeRules, nil
		},
	}

	// We also need Create to work on the feeRuleRepoStub — but the stub's
	// Create method returns errCreateNotImplemented by default.
	// Use the feeRuleMockRepo from fee_rule_commands_test.go instead.
	mockFeeRepo := newFeeRuleMockRepo()

	// Add the source fee rules so FindByContextID works.
	mockFeeRepo.rules[feeRuleID] = feeRules[0]

	// Override: we need to capture created rules and also use FindByContextID
	// that filters by contextID.
	// Since feeRuleMockRepo.FindByContextID filters by ContextID, and the
	// source rule has sourceCtxID, we need a custom stub instead.
	createCalled := 0
	captureRepo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, ctxID uuid.UUID) ([]*fee.FeeRule, error) {
			if ctxID == sourceCtxID {
				return feeRules, nil
			}

			return nil, nil
		},
	}
	_ = feeRepo
	_ = createdFeeRules

	// We need a feeRuleRepo that supports both FindByContextID AND Create.
	// Use feeRuleMockRepo with a pre-seeded rule.
	seededRepo := newFeeRuleMockRepo()
	seededRepo.rules[feeRuleID] = feeRules[0]

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   seededRepo,
	}

	cloned, err := uc.cloneFeeRules(context.Background(), sourceCtxID, newCtxID)

	require.NoError(t, err)
	assert.Equal(t, 1, cloned)
	_ = captureRepo
	_ = createCalled

	// Verify the created rule has the new context ID.
	for _, rule := range seededRepo.rules {
		if rule.ContextID == newCtxID {
			assert.NotEqual(t, feeRuleID, rule.ID) // New ID generated
			assert.Equal(t, "Fee Rule A", rule.Name)
			assert.Equal(t, fee.MatchingSideLeft, rule.Side)
			assert.Equal(t, scheduleID, rule.FeeScheduleID)
		}
	}
}

func TestCloneFeeRules_NoRulesReturnsZero(t *testing.T) {
	t.Parallel()

	feeRepo := newFeeRuleMockRepo()

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   feeRepo,
	}

	cloned, err := uc.cloneFeeRules(context.Background(), uuid.New(), uuid.New())

	require.NoError(t, err)
	assert.Equal(t, 0, cloned)
}

func TestCloneFeeRules_FetchError(t *testing.T) {
	t.Parallel()

	errFetch := errors.New("fee rules fetch failed")
	feeRepo := newFeeRuleMockRepo()
	feeRepo.findErr = errFetch

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   feeRepo,
	}

	cloned, err := uc.cloneFeeRules(context.Background(), uuid.New(), uuid.New())

	require.Error(t, err)
	assert.Equal(t, 0, cloned)
	assert.Contains(t, err.Error(), "fetching fee rules")
}

func TestCloneFeeRules_CreateError(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	errCreate := errors.New("fee rule create failed")

	feeRepo := newFeeRuleMockRepo()
	feeRepo.rules[uuid.New()] = &fee.FeeRule{
		ID:        uuid.New(),
		ContextID: sourceCtxID,
		Name:      "Bad Rule",
		Priority:  1,
		Side:      fee.MatchingSideLeft,
	}
	feeRepo.createErr = errCreate

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   feeRepo,
	}

	cloned, err := uc.cloneFeeRules(context.Background(), sourceCtxID, uuid.New())

	require.Error(t, err)
	assert.Equal(t, 0, cloned)
	assert.Contains(t, err.Error(), "creating cloned fee rule")
}

func TestCloneFeeRules_SkipsNilRules(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()

	// Custom stub that returns a nil entry in the slice.
	feeRepo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*fee.FeeRule, error) {
			return []*fee.FeeRule{nil, nil}, nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   feeRepo,
	}

	cloned, err := uc.cloneFeeRules(context.Background(), sourceCtxID, uuid.New())

	require.NoError(t, err)
	assert.Equal(t, 0, cloned)
}

func TestCloneFeeRules_ClonesPredicatesDeepCopy(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()

	originalPredicates := []fee.FieldPredicate{
		{Field: "brand", Operator: fee.PredicateOperatorIn, Values: []string{"visa", "mastercard"}},
	}

	feeRuleID := uuid.New()
	feeRepo := newFeeRuleMockRepo()
	feeRepo.rules[feeRuleID] = &fee.FeeRule{
		ID:         feeRuleID,
		ContextID:  sourceCtxID,
		Name:       "Predicate Rule",
		Priority:   1,
		Side:       fee.MatchingSideAny,
		Predicates: originalPredicates,
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   feeRepo,
	}

	cloned, err := uc.cloneFeeRules(context.Background(), sourceCtxID, newCtxID)

	require.NoError(t, err)
	assert.Equal(t, 1, cloned)

	// Mutate the cloned fee rule predicates and ensure the original is untouched.
	for _, rule := range feeRepo.rules {
		if rule.ContextID == newCtxID && len(rule.Predicates) > 0 {
			rule.Predicates[0].Values[0] = "elo"
		}
	}

	assert.Equal(t, "visa", originalPredicates[0].Values[0])
}

// ===========================================================================
// cloneFeeRulesWithTx (transactional path)
// ===========================================================================

func TestCloneFeeRulesWithTx_UsesTxCreatorAndFinder(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()
	txCreateCalls := 0

	repo := &feeRuleRepoTxStub{
		feeRuleRepoStub: &feeRuleRepoStub{},
		findByContextIDWithTxFn: func(_ context.Context, _ *sql.Tx, ctxID uuid.UUID) ([]*fee.FeeRule, error) {
			return []*fee.FeeRule{
				{ID: uuid.New(), ContextID: ctxID, Name: "Fee Rule", Priority: 1, Side: fee.MatchingSideLeft},
			}, nil
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
			txCreateCalls++
			assert.Equal(t, newCtxID, rule.ContextID)
			return nil
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   repo,
	}

	fakeTx := &sql.Tx{}

	cloned, err := uc.cloneFeeRulesWithTx(context.Background(), fakeTx, sourceCtxID, newCtxID)

	require.NoError(t, err)
	assert.Equal(t, 1, cloned)
	assert.Equal(t, 1, txCreateCalls)
}

func TestCloneFeeRulesWithTx_CreateWithTxError(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	errTxCreate := errors.New("fee rule tx create error")

	// Use the full tx stub with both finder and creator controlled.
	repo := &feeRuleRepoTxStub{
		feeRuleRepoStub: &feeRuleRepoStub{},
		findByContextIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID) ([]*fee.FeeRule, error) {
			return []*fee.FeeRule{
				{ID: uuid.New(), ContextID: sourceCtxID, Name: "Fee Rule", Priority: 1, Side: fee.MatchingSideLeft},
			}, nil
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *fee.FeeRule) error {
			return errTxCreate
		},
	}

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		feeRuleRepo:   repo,
	}

	fakeTx := &sql.Tx{}

	_, err := uc.cloneFeeRulesWithTx(context.Background(), fakeTx, sourceCtxID, uuid.New())

	require.Error(t, err)
	assert.ErrorIs(t, err, errTxCreate)
	assert.Contains(t, err.Error(), "creating cloned fee rule")
}

// ===========================================================================
// fetchAllRules (pagination)
// ===========================================================================

func TestFetchAllRules_Pagination(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	page1 := entities.MatchRules{
		{ID: uuid.New(), ContextID: sourceCtxID, Priority: 1},
	}
	page2 := entities.MatchRules{
		{ID: uuid.New(), ContextID: sourceCtxID, Priority: 2},
	}

	callCount := 0

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, cursor string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			callCount++
			if cursor == "" {
				return page1, libHTTP.CursorPagination{Next: "cursor-page-2"}, nil
			}

			return page2, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{matchRuleRepo: ruleRepo}

	rules, err := uc.fetchAllRules(context.Background(), sourceCtxID)

	require.NoError(t, err)
	assert.Len(t, rules, 2)
	assert.Equal(t, 2, callCount)
}

func TestFetchAllRules_FetchError(t *testing.T) {
	t.Parallel()

	errFetch := errors.New("page fetch failed")
	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errFetch
		},
	}

	uc := &UseCase{matchRuleRepo: ruleRepo}

	rules, err := uc.fetchAllRules(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Nil(t, rules)
	assert.ErrorIs(t, err, errFetch)
}

// ===========================================================================
// fetchAllRulesWithOptionalTx
// ===========================================================================

func TestFetchAllRulesWithOptionalTx_NilTxFallsBack(t *testing.T) {
	t.Parallel()

	called := false
	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			called = true
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{matchRuleRepo: ruleRepo}

	_, err := uc.fetchAllRulesWithOptionalTx(context.Background(), nil, uuid.New())

	require.NoError(t, err)
	assert.True(t, called)
}

func TestFetchAllRulesWithOptionalTx_TxFinderUsed(t *testing.T) {
	t.Parallel()

	txFinderCalled := false

	repo := &matchRuleRepoTxFinderStub{
		matchRuleRepoTxStub: &matchRuleRepoTxStub{
			matchRuleRepoStub: &matchRuleRepoStub{},
		},
		findByContextIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			txFinderCalled = true
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{matchRuleRepo: repo}
	fakeTx := &sql.Tx{}

	_, err := uc.fetchAllRulesWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, txFinderCalled)
}

func TestFetchAllRulesWithOptionalTx_TxWithoutFinderFallsBack(t *testing.T) {
	t.Parallel()

	// matchRuleRepoStub does NOT implement matchRuleTxFinder → fallback to non-tx.
	fallbackCalled := false
	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			fallbackCalled = true
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{matchRuleRepo: ruleRepo}
	fakeTx := &sql.Tx{}

	_, err := uc.fetchAllRulesWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, fallbackCalled)
}

// ===========================================================================
// findFeeRulesWithOptionalTx
// ===========================================================================

func TestFindFeeRulesWithOptionalTx_NilTxFallsBack(t *testing.T) {
	t.Parallel()

	called := false
	repo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*fee.FeeRule, error) {
			called = true
			return nil, nil
		},
	}

	uc := &UseCase{feeRuleRepo: repo}

	_, err := uc.findFeeRulesWithOptionalTx(context.Background(), nil, uuid.New())

	require.NoError(t, err)
	assert.True(t, called)
}

func TestFindFeeRulesWithOptionalTx_TxFinderUsed(t *testing.T) {
	t.Parallel()

	txCalled := false
	repo := &feeRuleRepoTxStub{
		feeRuleRepoStub: &feeRuleRepoStub{},
		findByContextIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID) ([]*fee.FeeRule, error) {
			txCalled = true
			return nil, nil
		},
	}

	uc := &UseCase{feeRuleRepo: repo}
	fakeTx := &sql.Tx{}

	_, err := uc.findFeeRulesWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, txCalled)
}

func TestFindFeeRulesWithOptionalTx_TxWithoutFinderFallsBack(t *testing.T) {
	t.Parallel()

	// feeRuleRepoStub does NOT implement feeRuleTxFinder → fallback.
	fallbackCalled := false
	repo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*fee.FeeRule, error) {
			fallbackCalled = true
			return nil, nil
		},
	}

	uc := &UseCase{feeRuleRepo: repo}
	fakeTx := &sql.Tx{}

	_, err := uc.findFeeRulesWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, fallbackCalled)
}

// ===========================================================================
// clonePredicates
// ===========================================================================

func TestClonePredicates_NilReturnsNil(t *testing.T) {
	t.Parallel()

	assert.Nil(t, clonePredicates(nil))
}

func TestClonePredicates_EmptyReturnsNil(t *testing.T) {
	t.Parallel()

	assert.Nil(t, clonePredicates([]fee.FieldPredicate{}))
}

func TestClonePredicates_DeepCopyValues(t *testing.T) {
	t.Parallel()

	original := []fee.FieldPredicate{
		{Field: "channel", Operator: fee.PredicateOperatorEquals, Value: "wire"},
		{Field: "brand", Operator: fee.PredicateOperatorIn, Values: []string{"visa", "mastercard"}},
		{Field: "empty_values", Operator: fee.PredicateOperatorEquals, Value: "test"},
	}

	cloned := clonePredicates(original)

	require.Len(t, cloned, 3)
	assert.Equal(t, original, cloned)

	// Mutate clone Values, verify original unchanged.
	cloned[1].Values[0] = "elo"
	assert.Equal(t, "visa", original[1].Values[0])

	// Mutate clone scalar, verify original unchanged.
	cloned[0].Value = "pix"
	assert.Equal(t, "wire", original[0].Value)
}
