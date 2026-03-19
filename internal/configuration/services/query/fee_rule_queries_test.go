//go:build unit

package query

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// --- Fee rule mock repository for query tests ---

// feeRuleMockRepoQ is a configurable mock of the FeeRuleRepository interface.
type feeRuleMockRepoQ struct {
	rules   map[uuid.UUID]*fee.FeeRule
	findErr error
}

var _ repositories.FeeRuleRepository = (*feeRuleMockRepoQ)(nil)

func newFeeRuleMockRepoQ() *feeRuleMockRepoQ {
	return &feeRuleMockRepoQ{rules: make(map[uuid.UUID]*fee.FeeRule)}
}

func (m *feeRuleMockRepoQ) Create(_ context.Context, rule *fee.FeeRule) error {
	m.rules[rule.ID] = rule
	return nil
}

func (m *feeRuleMockRepoQ) CreateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	m.rules[rule.ID] = rule
	return nil
}

func (m *feeRuleMockRepoQ) FindByID(_ context.Context, id uuid.UUID) (*fee.FeeRule, error) {
	if m.findErr != nil {
		return nil, m.findErr
	}

	rule, ok := m.rules[id]
	if !ok {
		return nil, nil
	}

	return rule, nil
}

func (m *feeRuleMockRepoQ) FindByContextID(_ context.Context, contextID uuid.UUID) ([]*fee.FeeRule, error) {
	if m.findErr != nil {
		return nil, m.findErr
	}

	result := make([]*fee.FeeRule, 0, len(m.rules))

	for _, rule := range m.rules {
		if rule.ContextID == contextID {
			result = append(result, rule)
		}
	}

	return result, nil
}

func (m *feeRuleMockRepoQ) Update(_ context.Context, rule *fee.FeeRule) error {
	m.rules[rule.ID] = rule
	return nil
}

func (m *feeRuleMockRepoQ) UpdateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	m.rules[rule.ID] = rule
	return nil
}

func (m *feeRuleMockRepoQ) Delete(_ context.Context, id uuid.UUID) error {
	delete(m.rules, id)
	return nil
}

func (m *feeRuleMockRepoQ) DeleteWithTx(_ context.Context, _ *sql.Tx, id uuid.UUID) error {
	delete(m.rules, id)
	return nil
}

// --- Helpers ---

// newQueryUseCaseWithFeeRuleRepo creates a query UseCase with the fee rule repo option.
func newQueryUseCaseWithFeeRuleRepo(t *testing.T, repo repositories.FeeRuleRepository) *UseCase {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(func() { ctrl.Finish() })

	uc, err := NewUseCase(
		mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
		WithFeeRuleRepository(repo),
	)
	require.NoError(t, err)

	return uc
}

// seedFeeRuleQ inserts a rule into the mock and returns it.
func seedFeeRuleQ(t *testing.T, repo *feeRuleMockRepoQ, contextID uuid.UUID) *fee.FeeRule {
	t.Helper()

	ctx := context.Background()

	rule, err := fee.NewFeeRule(
		ctx,
		contextID,
		uuid.New(),
		fee.MatchingSideLeft,
		"seeded-rule",
		1,
		[]fee.FieldPredicate{
			{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
		},
	)
	require.NoError(t, err)

	repo.rules[rule.ID] = rule

	return rule
}

// --- GetFeeRule tests ---

func TestGetFeeRule_Success(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepoQ()
	uc := newQueryUseCaseWithFeeRuleRepo(t, repo)
	existing := seedFeeRuleQ(t, repo, uuid.New())

	result, err := uc.GetFeeRule(context.Background(), existing.ID)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, existing.ID, result.ID)
	assert.Equal(t, existing.Name, result.Name)
}

func TestGetFeeRule_NilRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uc, err := NewUseCase(
		mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
	)
	require.NoError(t, err)

	result, getErr := uc.GetFeeRule(context.Background(), uuid.New())

	assert.Nil(t, result)
	assert.ErrorIs(t, getErr, ErrNilFeeRuleRepository)
}

func TestGetFeeRule_RepoError(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepoQ()
	repo.findErr = errors.New("connection reset")

	uc := newQueryUseCaseWithFeeRuleRepo(t, repo)

	result, err := uc.GetFeeRule(context.Background(), uuid.New())

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "finding fee rule")
}

func TestGetFeeRule_NotFound_ReturnsNil(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepoQ()
	uc := newQueryUseCaseWithFeeRuleRepo(t, repo)

	result, err := uc.GetFeeRule(context.Background(), uuid.New())

	require.NoError(t, err)
	assert.Nil(t, result)
}

// --- ListFeeRules tests ---

func TestListFeeRules_Success(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepoQ()
	uc := newQueryUseCaseWithFeeRuleRepo(t, repo)

	contextID := uuid.New()
	seedFeeRuleQ(t, repo, contextID)
	seedFeeRuleQ(t, repo, contextID)

	// Add a rule from a different context (should not appear).
	seedFeeRuleQ(t, repo, uuid.New())

	result, err := uc.ListFeeRules(context.Background(), contextID)

	require.NoError(t, err)
	assert.Len(t, result, 2)

	for _, rule := range result {
		assert.Equal(t, contextID, rule.ContextID)
	}
}

func TestListFeeRules_NilRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uc, err := NewUseCase(
		mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
	)
	require.NoError(t, err)

	result, listErr := uc.ListFeeRules(context.Background(), uuid.New())

	assert.Nil(t, result)
	assert.ErrorIs(t, listErr, ErrNilFeeRuleRepository)
}

func TestListFeeRules_RepoError(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepoQ()
	repo.findErr = errors.New("table not found")

	uc := newQueryUseCaseWithFeeRuleRepo(t, repo)

	result, err := uc.ListFeeRules(context.Background(), uuid.New())

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "listing fee rules")
}

func TestListFeeRules_EmptyResult(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepoQ()
	uc := newQueryUseCaseWithFeeRuleRepo(t, repo)

	result, err := uc.ListFeeRules(context.Background(), uuid.New())

	require.NoError(t, err)
	assert.Empty(t, result)
}
