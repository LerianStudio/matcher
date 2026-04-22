//go:build unit

package fee_rule

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// setupMockForTxTests creates a repository backed by sqlmock and returns the
// raw *sql.DB so callers can begin explicit transactions for WithTx tests.
func setupMockForTxTests(t *testing.T) (*Repository, *sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	return repo, db, mock, func() { db.Close() }
}

// ---- executeCreate (internal) ----

func TestExecuteCreate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO fee_rules").
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err = repo.executeCreate(ctx, tx, rule)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_InsertError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO fee_rules").
		WillReturnError(errors.New("duplicate key violation"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err = repo.executeCreate(ctx, tx, rule)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert fee rule")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_NilEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err = repo.executeCreate(ctx, tx, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeRuleEntityNil)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_EntityWithNilID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	rule := createTestFeeRule(t)
	rule.ID = uuid.Nil

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err = repo.executeCreate(ctx, tx, rule)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create fee rule model")
	require.NoError(t, mock.ExpectationsWereMet())
}

// ---- executeUpdate (internal) ----

func TestExecuteUpdate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_rules").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeUpdate(ctx, tx, rule)

	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_rules").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeUpdate(ctx, tx, rule)

	require.Error(t, err)
	assert.False(t, ok)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_rules").
		WillReturnError(errors.New("connection lost"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeUpdate(ctx, tx, rule)

	require.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "update fee rule")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_NilEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeUpdate(ctx, tx, nil)

	require.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "create fee rule model")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_rules").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeUpdate(ctx, tx, rule)

	require.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "get rows affected")
	require.NoError(t, mock.ExpectationsWereMet())
}

// ---- executeDelete (internal) ----

func TestExecuteDelete_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM fee_rules").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeDelete(ctx, tx, contextID, id)

	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteDelete_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM fee_rules").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeDelete(ctx, tx, contextID, id)

	require.Error(t, err)
	assert.False(t, ok)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteDelete_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM fee_rules").
		WillReturnError(errors.New("foreign key constraint"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeDelete(ctx, tx, contextID, id)

	require.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "delete fee rule")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteDelete_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM fee_rules").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	ok, err := repo.executeDelete(ctx, tx, contextID, id)

	require.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "get rows affected")
	require.NoError(t, mock.ExpectationsWereMet())
}

// ---- FindByContextIDWithTx ----

func TestFindByContextIDWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()
	contextID := uuid.New()
	id1 := uuid.New()
	scheduleID := uuid.New()
	predsJSON, err := json.Marshal([]fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
	})
	require.NoError(t, err)

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC")).
		WithArgs(contextID.String()).
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(id1.String(), contextID.String(), "LEFT", scheduleID.String(), "rule-1", 1, predsJSON, now, now))

	results, err := repo.FindByContextIDWithTx(ctx, tx, contextID)

	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, id1, results[0].ID)
	assert.Equal(t, 1, results[0].Priority)
}

func TestFindByContextIDWithTx_EmptyResult(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC")).
		WithArgs(contextID.String()).
		WillReturnRows(sqlmock.NewRows(feeRuleCols))

	results, err := repo.FindByContextIDWithTx(ctx, tx, contextID)

	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestFindByContextIDWithTx_QueryError(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC")).
		WithArgs(contextID.String()).
		WillReturnError(errors.New("network timeout"))

	results, err := repo.FindByContextIDWithTx(ctx, tx, contextID)

	require.Error(t, err)
	require.Nil(t, results)
	assert.Contains(t, err.Error(), "find fee rules by context with tx")
}

func TestFindByContextIDWithTx_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	results, err := repo.FindByContextIDWithTx(context.Background(), nil, uuid.New())

	require.Error(t, err)
	require.Nil(t, results)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByContextIDWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	results, err := repo.FindByContextIDWithTx(context.Background(), nil, uuid.New())

	require.Error(t, err)
	require.Nil(t, results)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByContextIDWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	results, err := repo.FindByContextIDWithTx(context.Background(), nil, uuid.New())

	require.Error(t, err)
	require.Nil(t, results)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

// ---- CreateWithTx (with real sql mock tx) ----

func TestCreateWithTx_SuccessWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO fee_rules")).
		WithArgs(
			rule.ID.String(),
			rule.ContextID.String(),
			string(rule.Side),
			rule.FeeScheduleID.String(),
			rule.Name,
			rule.Priority,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.CreateWithTx(ctx, tx, rule)

	require.NoError(t, err)
}

func TestCreateWithTx_InsertErrorWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO fee_rules")).
		WithArgs(
			rule.ID.String(),
			rule.ContextID.String(),
			string(rule.Side),
			rule.FeeScheduleID.String(),
			rule.Name,
			rule.Priority,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnError(errors.New("unique violation"))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.CreateWithTx(ctx, tx, rule)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create fee rule with tx")
}

// ---- UpdateWithTx (with real sql mock tx) ----

func TestUpdateWithTx_SuccessWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE fee_rules")).
		WithArgs(
			string(rule.Side),
			rule.FeeScheduleID.String(),
			rule.Name,
			rule.Priority,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			rule.ID.String(),
			rule.ContextID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateWithTx(ctx, tx, rule)

	require.NoError(t, err)
}

func TestUpdateWithTx_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE fee_rules")).
		WithArgs(
			string(rule.Side),
			rule.FeeScheduleID.String(),
			rule.Name,
			rule.Priority,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			rule.ID.String(),
			rule.ContextID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateWithTx(ctx, tx, rule)

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestUpdateWithTx_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE fee_rules")).
		WithArgs(
			string(rule.Side),
			rule.FeeScheduleID.String(),
			rule.Name,
			rule.Priority,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			rule.ID.String(),
			rule.ContextID.String(),
		).
		WillReturnError(errors.New("constraint violation"))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateWithTx(ctx, tx, rule)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update fee rule with tx")
}

// ---- DeleteWithTx (with real sql mock tx) ----

func TestDeleteWithTx_SuccessWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fee_rules WHERE id = $1 AND context_id = $2")).
		WithArgs(id.String(), contextID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, contextID, id)

	require.NoError(t, err)
}

func TestDeleteWithTx_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fee_rules WHERE id = $1 AND context_id = $2")).
		WithArgs(id.String(), contextID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, contextID, id)

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestDeleteWithTx_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fee_rules WHERE id = $1 AND context_id = $2")).
		WithArgs(id.String(), contextID.String()).
		WillReturnError(errors.New("deadlock"))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, contextID, id)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete fee rule with tx")
}

func TestDeleteWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	err := repo.DeleteWithTx(context.Background(), nil, uuid.New(), uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// ---- scanFeeRule edge cases ----

func TestScanFeeRule_InvalidID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	feeScheduleID := uuid.New()
	predsJSON, _ := json.Marshal([]fee.FieldPredicate{})

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(
				"not-a-uuid", uuid.New().String(), "LEFT",
				feeScheduleID.String(), "bad-id", 1,
				predsJSON, now, now,
			))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)

	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFeeRule(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid UUID")
}

func TestScanFeeRule_InvalidContextID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	feeScheduleID := uuid.New()
	predsJSON, _ := json.Marshal([]fee.FieldPredicate{})

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(
				id.String(), "invalid-ctx-id", "LEFT",
				feeScheduleID.String(), "bad-ctx", 1,
				predsJSON, now, now,
			))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)

	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFeeRule(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid UUID")
}

func TestScanFeeRule_InvalidFeeScheduleID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	predsJSON, _ := json.Marshal([]fee.FieldPredicate{})

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(
				id.String(), contextID.String(), "LEFT",
				"not-a-uuid", "bad-schedule", 1,
				predsJSON, now, now,
			))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)

	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFeeRule(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid UUID")
}

func TestScanFeeRule_EmptyPredicates(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	feeScheduleID := uuid.New()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(
				id.String(), contextID.String(), "ANY",
				feeScheduleID.String(), "no-preds", 0,
				[]byte("[]"), now, now,
			))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)

	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFeeRule(rows)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Predicates)
}

func TestScanFeeRule_AllSides(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		sideStr string
		side    fee.MatchingSide
	}{
		{"left", "LEFT", fee.MatchingSideLeft},
		{"right", "RIGHT", fee.MatchingSideRight},
		{"any", "ANY", fee.MatchingSideAny},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			id := uuid.New()
			contextID := uuid.New()
			feeScheduleID := uuid.New()
			predsJSON, _ := json.Marshal([]fee.FieldPredicate{})

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			mock.ExpectQuery("SELECT").
				WillReturnRows(sqlmock.NewRows(feeRuleCols).
					AddRow(
						id.String(), contextID.String(), tc.sideStr,
						feeScheduleID.String(), "side-test", 1,
						predsJSON, now, now,
					))

			rows, err := db.Query("SELECT")
			require.NoError(t, err)

			defer rows.Close()

			require.True(t, rows.Next())

			result, err := scanFeeRule(rows)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tc.side, result.Side)
		})
	}
}

// ---- FindByContextIDWithTx scan error ----

func TestFindByContextIDWithTx_ScanError(t *testing.T) {
	t.Parallel()

	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	// Return incorrect columns to trigger a scan error.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC")).
		WithArgs(contextID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id"}).
			AddRow("bad", "bad"))

	results, err := repo.FindByContextIDWithTx(ctx, tx, contextID)

	require.Error(t, err)
	require.Nil(t, results)
}

// ---- Update with NilProvider ----

func TestUpdate_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	err := repo.Update(context.Background(), createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}
