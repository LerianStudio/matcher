//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Stubs for required UseCase constructor dependencies are defined in
// schedule_commands_test.go (stubSourceRepo, stubFieldMapRepo, stubMatchRuleRepo, mockCtxRepo).

// --- Fee rule mock repository ---

// feeRuleMockRepo is a configurable mock of the FeeRuleRepository interface.
type feeRuleMockRepo struct {
	rules     map[uuid.UUID]*fee.FeeRule
	createErr error
	findErr   error
	updateErr error
	deleteErr error
}

var _ repositories.FeeRuleRepository = (*feeRuleMockRepo)(nil)

func newFeeRuleMockRepo() *feeRuleMockRepo {
	return &feeRuleMockRepo{rules: make(map[uuid.UUID]*fee.FeeRule)}
}

func (m *feeRuleMockRepo) Create(_ context.Context, rule *fee.FeeRule) error {
	if m.createErr != nil {
		return m.createErr
	}

	m.rules[rule.ID] = rule

	return nil
}

func (m *feeRuleMockRepo) CreateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	return m.Create(context.Background(), rule)
}

func (m *feeRuleMockRepo) FindByID(_ context.Context, id uuid.UUID) (*fee.FeeRule, error) {
	if m.findErr != nil {
		return nil, m.findErr
	}

	rule, ok := m.rules[id]
	if !ok {
		return nil, nil
	}

	return rule, nil
}

func (m *feeRuleMockRepo) FindByContextID(_ context.Context, contextID uuid.UUID) ([]*fee.FeeRule, error) {
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

func (m *feeRuleMockRepo) Update(_ context.Context, rule *fee.FeeRule) error {
	if m.updateErr != nil {
		return m.updateErr
	}

	m.rules[rule.ID] = rule

	return nil
}

func (m *feeRuleMockRepo) UpdateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	return m.Update(context.Background(), rule)
}

func (m *feeRuleMockRepo) Delete(_ context.Context, id uuid.UUID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}

	delete(m.rules, id)

	return nil
}

func (m *feeRuleMockRepo) DeleteWithTx(_ context.Context, _ *sql.Tx, id uuid.UUID) error {
	return m.Delete(context.Background(), id)
}

// --- Helper ---

// newUseCaseWithFeeRuleRepo creates a UseCase with the fee rule repo option injected.
func newUseCaseWithFeeRuleRepo(repo repositories.FeeRuleRepository) *UseCase {
	uc, _ := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
		WithFeeRuleRepository(repo),
	)

	return uc
}

// validPredicates returns a minimal valid predicate slice for testing.
func validPredicates() []fee.FieldPredicate {
	return []fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
	}
}

// seedRule inserts a rule into the mock repo and returns it.
func seedRule(t *testing.T, repo *feeRuleMockRepo) *fee.FeeRule {
	t.Helper()

	ctx := context.Background()

	rule, err := fee.NewFeeRule(
		ctx,
		uuid.New(),
		uuid.New(),
		fee.MatchingSideLeft,
		"seeded-rule",
		1,
		validPredicates(),
	)
	require.NoError(t, err)

	repo.rules[rule.ID] = rule

	return rule
}

// --- CreateFeeRule tests ---

func TestCreateFeeRule_Success(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)

	result, err := uc.CreateFeeRule(
		context.Background(),
		uuid.New(),
		"LEFT",
		uuid.New(),
		"my-rule",
		1,
		validPredicates(),
	)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "my-rule", result.Name)
	assert.Equal(t, fee.MatchingSideLeft, result.Side)
	assert.Equal(t, 1, result.Priority)
	assert.Len(t, repo.rules, 1)
}

func TestCreateFeeRule_NilRepo(t *testing.T) {
	t.Parallel()

	// Build a UseCase without setting the feeRuleRepo option.
	uc, err := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
	)
	require.NoError(t, err)

	result, err := uc.CreateFeeRule(
		context.Background(),
		uuid.New(),
		"LEFT",
		uuid.New(),
		"my-rule",
		1,
		validPredicates(),
	)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrNilFeeRuleRepository)
}

func TestCreateFeeRule_RepoError(t *testing.T) {
	t.Parallel()

	repoErr := errors.New("pg connection refused")
	repo := newFeeRuleMockRepo()
	repo.createErr = repoErr

	uc := newUseCaseWithFeeRuleRepo(repo)

	result, err := uc.CreateFeeRule(
		context.Background(),
		uuid.New(),
		"LEFT",
		uuid.New(),
		"my-rule",
		1,
		validPredicates(),
	)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "creating fee rule")
}

func TestCreateFeeRule_ConstraintError_DuplicatePriority(t *testing.T) {
	t.Parallel()

	pgErr := &pgconn.PgError{
		Code:           "23505",
		ConstraintName: constraintFeeRulePriority,
	}
	repo := newFeeRuleMockRepo()
	repo.createErr = pgErr

	uc := newUseCaseWithFeeRuleRepo(repo)

	result, err := uc.CreateFeeRule(
		context.Background(),
		uuid.New(),
		"LEFT",
		uuid.New(),
		"my-rule",
		1,
		validPredicates(),
	)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDuplicateFeeRulePriority)
}

func TestCreateFeeRule_ConstraintError_DuplicateName(t *testing.T) {
	t.Parallel()

	pgErr := &pgconn.PgError{
		Code:           "23505",
		ConstraintName: constraintFeeRuleName,
	}
	repo := newFeeRuleMockRepo()
	repo.createErr = pgErr

	uc := newUseCaseWithFeeRuleRepo(repo)

	result, err := uc.CreateFeeRule(
		context.Background(),
		uuid.New(),
		"LEFT",
		uuid.New(),
		"my-rule",
		1,
		validPredicates(),
	)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDuplicateFeeRuleName)
}

func TestCreateFeeRule_InvalidSide(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)

	result, err := uc.CreateFeeRule(
		context.Background(),
		uuid.New(),
		"INVALID_SIDE",
		uuid.New(),
		"my-rule",
		1,
		validPredicates(),
	)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorIs(t, err, fee.ErrInvalidMatchingSide)
}

func TestCreateFeeRule_NilReceiver(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	result, err := uc.CreateFeeRule(
		context.Background(),
		uuid.New(),
		"LEFT",
		uuid.New(),
		"my-rule",
		1,
		validPredicates(),
	)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrNilFeeRuleRepository)
}

// --- UpdateFeeRule tests ---

func TestUpdateFeeRule_Success(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	newName := "updated-name"

	result, err := uc.UpdateFeeRule(
		context.Background(),
		existing.ID,
		nil,     // side unchanged
		nil,     // feeScheduleID unchanged
		&newName,
		nil, // priority unchanged
		nil, // predicates unchanged
	)

	require.NoError(t, err)
	assert.Equal(t, "updated-name", result.Name)
}

func TestUpdateFeeRule_NotFound(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)

	newName := "x"

	result, err := uc.UpdateFeeRule(
		context.Background(),
		uuid.New(), // does not exist
		nil,
		nil,
		&newName,
		nil,
		nil,
	)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, fee.ErrFeeRuleNotFound)
}

func TestUpdateFeeRule_NilRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
	)
	require.NoError(t, err)

	newName := "x"

	result, updateErr := uc.UpdateFeeRule(
		context.Background(),
		uuid.New(),
		nil,
		nil,
		&newName,
		nil,
		nil,
	)

	assert.Nil(t, result)
	assert.ErrorIs(t, updateErr, ErrNilFeeRuleRepository)
}

func TestUpdateFeeRule_ValidationError_InvalidSide(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	badSide := "BANANA"

	result, err := uc.UpdateFeeRule(
		context.Background(),
		existing.ID,
		&badSide,
		nil,
		nil,
		nil,
		nil,
	)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorIs(t, err, fee.ErrInvalidMatchingSide)
}

func TestUpdateFeeRule_ValidationError_EmptyName(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	emptyName := ""

	result, err := uc.UpdateFeeRule(
		context.Background(),
		existing.ID,
		nil,
		nil,
		&emptyName,
		nil,
		nil,
	)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorIs(t, err, fee.ErrFeeRuleNameRequired)
}

func TestUpdateFeeRule_ValidationError_NegativePriority(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	negativePriority := -1

	result, err := uc.UpdateFeeRule(
		context.Background(),
		existing.ID,
		nil,
		nil,
		nil,
		&negativePriority,
		nil,
	)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorIs(t, err, fee.ErrFeeRulePriorityNegative)
}

func TestUpdateFeeRule_FindByIDError(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	repo.findErr = errors.New("database timeout")

	uc := newUseCaseWithFeeRuleRepo(repo)

	newName := "x"

	result, err := uc.UpdateFeeRule(
		context.Background(),
		uuid.New(),
		nil,
		nil,
		&newName,
		nil,
		nil,
	)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "finding fee rule")
}

func TestUpdateFeeRule_RepoUpdateError(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	// Allow FindByID to succeed, then fail on Update.
	repo.updateErr = errors.New("disk full")

	newName := "updated"

	result, err := uc.UpdateFeeRule(
		context.Background(),
		existing.ID,
		nil,
		nil,
		&newName,
		nil,
		nil,
	)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "updating fee rule")
}

func TestUpdateFeeRule_ConstraintError(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	repo.updateErr = &pgconn.PgError{
		Code:           "23505",
		ConstraintName: constraintFeeRuleName,
	}

	newName := "duplicate-name"

	result, err := uc.UpdateFeeRule(
		context.Background(),
		existing.ID,
		nil,
		nil,
		&newName,
		nil,
		nil,
	)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDuplicateFeeRuleName)
}

// --- DeleteFeeRule tests ---

func TestDeleteFeeRule_Success(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	err := uc.DeleteFeeRule(context.Background(), existing.ID)

	assert.NoError(t, err)
	assert.Empty(t, repo.rules)
}

func TestDeleteFeeRule_NotFound(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)

	err := uc.DeleteFeeRule(context.Background(), uuid.New())

	assert.ErrorIs(t, err, fee.ErrFeeRuleNotFound)
}

func TestDeleteFeeRule_NilRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
	)
	require.NoError(t, err)

	deleteErr := uc.DeleteFeeRule(context.Background(), uuid.New())

	assert.ErrorIs(t, deleteErr, ErrNilFeeRuleRepository)
}

func TestDeleteFeeRule_FindByIDError(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	repo.findErr = errors.New("network error")

	uc := newUseCaseWithFeeRuleRepo(repo)

	err := uc.DeleteFeeRule(context.Background(), uuid.New())

	assert.Error(t, err)
	assert.ErrorContains(t, err, "finding fee rule")
}

func TestDeleteFeeRule_RepoDeleteError(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()
	uc := newUseCaseWithFeeRuleRepo(repo)
	existing := seedRule(t, repo)

	repo.deleteErr = errors.New("permission denied")

	err := uc.DeleteFeeRule(context.Background(), existing.ID)

	assert.Error(t, err)
	assert.ErrorContains(t, err, "deleting fee rule")
}

// --- mapFeeRuleConstraintError tests ---

func TestMapFeeRuleConstraintError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected error
	}{
		{
			name:     "non-pg error returns nil",
			err:      errors.New("generic error"),
			expected: nil,
		},
		{
			name: "pg error non-unique returns nil",
			err: &pgconn.PgError{
				Code:           "23503", // foreign_key_violation
				ConstraintName: constraintFeeRulePriority,
			},
			expected: nil,
		},
		{
			name: "priority constraint maps correctly",
			err: &pgconn.PgError{
				Code:           "23505",
				ConstraintName: constraintFeeRulePriority,
			},
			expected: ErrDuplicateFeeRulePriority,
		},
		{
			name: "name constraint maps correctly",
			err: &pgconn.PgError{
				Code:           "23505",
				ConstraintName: constraintFeeRuleName,
			},
			expected: ErrDuplicateFeeRuleName,
		},
		{
			name: "unknown constraint returns nil",
			err: &pgconn.PgError{
				Code:           "23505",
				ConstraintName: "uq_something_else",
			},
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := mapFeeRuleConstraintError(tc.err)

			if tc.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.ErrorIs(t, result, tc.expected)
			}
		})
	}
}

// --- WithFeeRuleRepository option test ---

func TestWithFeeRuleRepository_NilIsIgnored(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
		WithFeeRuleRepository(nil),
	)
	require.NoError(t, err)

	// The repo should remain nil when nil is passed.
	assert.Nil(t, uc.feeRuleRepo)
}

func TestWithFeeRuleRepository_SetsRepo(t *testing.T) {
	t.Parallel()

	repo := newFeeRuleMockRepo()

	uc, err := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
		WithFeeRuleRepository(repo),
	)
	require.NoError(t, err)

	assert.NotNil(t, uc.feeRuleRepo)
}
