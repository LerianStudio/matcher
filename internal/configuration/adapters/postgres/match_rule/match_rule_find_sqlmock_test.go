//go:build unit

package match_rule

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupMockRepo(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func testMatchRuleCols() []string {
	return []string{"id", "context_id", "priority", "type", "config", "created_at", "updated_at"}
}

// --- FindByID Tests ---

func TestRepository_FindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE context_id").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnRows(
			sqlmock.NewRows(testMatchRuleCols()).
				AddRow(ruleID.String(), contextID.String(), 1, shared.RuleTypeExact.String(), []byte(`{"field":"amount"}`), now, now),
		)
	mock.ExpectCommit()

	result, err := repo.FindByID(context.Background(), contextID, ruleID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, ruleID, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, 1, result.Priority)
}

func TestRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE context_id").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByID(context.Background(), contextID, ruleID)
	require.Error(t, err)
	assert.Nil(t, result)
}

func TestRepository_FindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE context_id").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	result, err := repo.FindByID(context.Background(), contextID, ruleID)
	require.Error(t, err)
	assert.Nil(t, result)
}

// --- FindByPriority Tests ---

func TestRepository_FindByPriority_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE context_id .+ AND priority").
		WithArgs(contextID.String(), 1).
		WillReturnRows(
			sqlmock.NewRows(testMatchRuleCols()).
				AddRow(ruleID.String(), contextID.String(), 1, shared.RuleTypeExact.String(), []byte(`{}`), now, now),
		)
	mock.ExpectCommit()

	result, err := repo.FindByPriority(context.Background(), contextID, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.Priority)
}

func TestRepository_FindByPriority_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE context_id .+ AND priority").
		WithArgs(contextID.String(), 999).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByPriority(context.Background(), contextID, 999)
	require.Error(t, err)
	assert.Nil(t, result)
}

// --- FindByContextID Tests ---

func TestRepository_FindByContextID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	rule1ID := uuid.New()
	rule2ID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE").
		WillReturnRows(
			sqlmock.NewRows(testMatchRuleCols()).
				AddRow(rule1ID.String(), contextID.String(), 1, shared.RuleTypeExact.String(), []byte(`{}`), now, now).
				AddRow(rule2ID.String(), contextID.String(), 2, shared.RuleTypeTolerance.String(), []byte(`{}`), now, now),
		)
	mock.ExpectCommit()

	rules, pagination, err := repo.FindByContextID(context.Background(), contextID, "", 10)
	require.NoError(t, err)
	assert.Len(t, rules, 2)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestRepository_FindByContextID_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE").
		WillReturnRows(sqlmock.NewRows(testMatchRuleCols()))
	mock.ExpectCommit()

	rules, _, err := repo.FindByContextID(context.Background(), contextID, "", 10)
	require.NoError(t, err)
	assert.Empty(t, rules)
}

func TestRepository_FindByContextID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	rules, _, err := repo.FindByContextID(context.Background(), contextID, "", 10)
	require.Error(t, err)
	assert.Nil(t, rules)
}

// --- FindByContextIDAndType Tests ---

func TestRepository_FindByContextIDAndType_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	rule1ID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE").
		WillReturnRows(
			sqlmock.NewRows(testMatchRuleCols()).
				AddRow(rule1ID.String(), contextID.String(), 1, shared.RuleTypeExact.String(), []byte(`{}`), now, now),
		)
	mock.ExpectCommit()

	rules, _, err := repo.FindByContextIDAndType(context.Background(), contextID, shared.RuleTypeExact, "", 10)
	require.NoError(t, err)
	assert.Len(t, rules, 1)
}

func TestRepository_FindByContextIDAndType_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM match_rules WHERE").
		WillReturnRows(sqlmock.NewRows(testMatchRuleCols()))
	mock.ExpectCommit()

	rules, _, err := repo.FindByContextIDAndType(context.Background(), contextID, shared.RuleTypeTolerance, "", 10)
	require.NoError(t, err)
	assert.Empty(t, rules)
}

// --- ReorderPriorities Tests ---

func TestRepository_ReorderPriorities_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	rule1ID := uuid.New()
	rule2ID := uuid.New()

	mock.ExpectBegin()
	// First: offset existing priorities.
	mock.ExpectExec("UPDATE match_rules SET priority").
		WillReturnResult(sqlmock.NewResult(0, 2))
	// Second: reorder with CASE.
	mock.ExpectExec("UPDATE match_rules SET priority = CASE").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	err := repo.ReorderPriorities(context.Background(), contextID, []uuid.UUID{rule1ID, rule2ID})
	require.NoError(t, err)
}

func TestRepository_ReorderPriorities_EmptyIDs(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepo(t)
	defer finish()

	err := repo.ReorderPriorities(context.Background(), uuid.New(), []uuid.UUID{})
	require.ErrorIs(t, err, ErrRuleIDsRequired)
}

func TestRepository_ReorderPriorities_RowsMismatch(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	rule1ID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_rules SET priority").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE match_rules SET priority = CASE").
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows affected, expected 1
	mock.ExpectRollback()

	err := repo.ReorderPriorities(context.Background(), contextID, []uuid.UUID{rule1ID})
	require.Error(t, err)
}

// --- Delete Tests ---

func TestRepository_Delete_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM match_rules WHERE").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(context.Background(), contextID, ruleID)
	require.NoError(t, err)
}

func TestRepository_Delete_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	contextID := uuid.New()
	ruleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM match_rules WHERE").
		WithArgs(contextID.String(), ruleID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0)) // No rows affected.
	mock.ExpectRollback()

	err := repo.Delete(context.Background(), contextID, ruleID)
	require.Error(t, err)
}
