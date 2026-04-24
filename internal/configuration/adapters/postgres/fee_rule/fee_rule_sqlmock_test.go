// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

var (
	errTestQuery = errors.New("query error")
	errTestExec  = errors.New("exec error")
)

// setupMock initialises a sqlmock-backed Repository for unit tests.
func setupMock(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

// createTestFeeRule builds a valid fee.FeeRule entity for test assertions.
func createTestFeeRule(t *testing.T) *fee.FeeRule {
	t.Helper()

	now := time.Now().UTC()

	return &fee.FeeRule{
		ID:            uuid.New(),
		ContextID:     uuid.New(),
		Side:          fee.MatchingSideLeft,
		FeeScheduleID: uuid.New(),
		Name:          "test-fee-rule",
		Priority:      10,
		Predicates: []fee.FieldPredicate{
			{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// feeRuleColumns mirrors the column list used by the repository.
var feeRuleCols = []string{
	"id", "context_id", "side", "fee_schedule_id",
	"name", "priority", "predicates",
	"created_at", "updated_at",
}

// ---- NewRepository ----

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

// ---- Create ----

func TestCreate_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

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
			sqlmock.AnyArg(), // predicates JSON
			sqlmock.AnyArg(), // created_at
			sqlmock.AnyArg(), // updated_at
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := repo.Create(context.Background(), rule)

	require.NoError(t, err)
}

func TestCreate_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

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
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	err := repo.Create(context.Background(), rule)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create fee rule")
}

func TestCreate_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.Create(context.Background(), createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreate_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	err := repo.Create(context.Background(), createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreate_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.Create(context.Background(), nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeRuleEntityNil)
}

// ---- CreateWithTx ----

func TestCreateWithTx_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.CreateWithTx(context.Background(), nil, createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreateWithTx_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.CreateWithTx(context.Background(), nil, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeRuleEntityNil)
}

func TestCreateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.CreateWithTx(context.Background(), nil, createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

// ---- FindByID ----

func TestFindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	feeScheduleID := uuid.New()
	predicatesJSON, err := json.Marshal([]fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "EUR"},
	})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(
				id.String(),
				contextID.String(),
				"LEFT",
				feeScheduleID.String(),
				"euro-rule",
				5,
				predicatesJSON,
				now,
				now,
			))
	mock.ExpectCommit()

	result, err := repo.FindByID(context.Background(), id)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, fee.MatchingSideLeft, result.Side)
	assert.Equal(t, feeScheduleID, result.FeeScheduleID)
	assert.Equal(t, "euro-rule", result.Name)
	assert.Equal(t, 5, result.Priority)
	require.Len(t, result.Predicates, 1)
	assert.Equal(t, "EUR", result.Predicates[0].Value)
}

func TestFindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByID(context.Background(), id)

	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, fee.ErrFeeRuleNotFound)
}

func TestFindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnError(errTestQuery)
	mock.ExpectRollback()

	result, err := repo.FindByID(context.Background(), id)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "find fee rule by id")
}

func TestFindByID_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.FindByID(context.Background(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.FindByID(context.Background(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// ---- FindByContextID ----

func TestFindByContextID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	now := time.Now().UTC()
	contextID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()
	scheduleID1 := uuid.New()
	scheduleID2 := uuid.New()
	preds1, err := json.Marshal([]fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
	})
	require.NoError(t, err)

	preds2, err := json.Marshal([]fee.FieldPredicate{
		{Field: "type", Operator: fee.PredicateOperatorIn, Values: []string{"WIRE", "ACH"}},
	})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC")).
		WithArgs(contextID.String()).
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(id1.String(), contextID.String(), "LEFT", scheduleID1.String(), "rule-1", 1, preds1, now, now).
			AddRow(id2.String(), contextID.String(), "RIGHT", scheduleID2.String(), "rule-2", 2, preds2, now, now))
	mock.ExpectCommit()

	results, err := repo.FindByContextID(context.Background(), contextID)

	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, id1, results[0].ID)
	assert.Equal(t, 1, results[0].Priority)
	assert.Equal(t, id2, results[1].ID)
	assert.Equal(t, 2, results[1].Priority)
}

func TestFindByContextID_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC")).
		WithArgs(contextID.String()).
		WillReturnRows(sqlmock.NewRows(feeRuleCols))
	mock.ExpectCommit()

	results, err := repo.FindByContextID(context.Background(), contextID)

	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestFindByContextID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + feeRuleColumns + " FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC")).
		WithArgs(contextID.String()).
		WillReturnError(errTestQuery)
	mock.ExpectRollback()

	results, err := repo.FindByContextID(context.Background(), contextID)

	require.Error(t, err)
	require.Nil(t, results)
	assert.Contains(t, err.Error(), "find fee rules by context")
}

func TestFindByContextID_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	results, err := repo.FindByContextID(context.Background(), uuid.New())

	require.Error(t, err)
	require.Nil(t, results)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByContextID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	results, err := repo.FindByContextID(context.Background(), uuid.New())

	require.Error(t, err)
	require.Nil(t, results)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// ---- Update ----

func TestUpdate_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	rule := createTestFeeRule(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE fee_rules")).
		WithArgs(
			string(rule.Side),
			rule.FeeScheduleID.String(),
			rule.Name,
			rule.Priority,
			sqlmock.AnyArg(), // predicates JSON
			sqlmock.AnyArg(), // updated_at
			rule.ID.String(),
			rule.ContextID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Update(context.Background(), rule)

	require.NoError(t, err)
}

func TestUpdate_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

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
	mock.ExpectRollback()

	err := repo.Update(context.Background(), rule)

	require.Error(t, err)
	assert.ErrorIs(t, err, fee.ErrFeeRuleNotFound)
}

func TestUpdate_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

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
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	err := repo.Update(context.Background(), rule)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update fee rule")
}

func TestUpdate_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.Update(context.Background(), createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdate_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.Update(context.Background(), nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeRuleEntityNil)
}

// ---- UpdateWithTx ----

func TestUpdateWithTx_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.UpdateWithTx(context.Background(), nil, createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdateWithTx_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.UpdateWithTx(context.Background(), nil, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeRuleEntityNil)
}

func TestUpdateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.UpdateWithTx(context.Background(), nil, createTestFeeRule(t))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

// ---- Delete ----

func TestDelete_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fee_rules WHERE id = $1 AND context_id = $2")).
		WithArgs(id.String(), contextID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(context.Background(), contextID, id)

	require.NoError(t, err)
}

func TestDelete_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fee_rules WHERE id = $1 AND context_id = $2")).
		WithArgs(id.String(), contextID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(context.Background(), contextID, id)

	require.Error(t, err)
	assert.ErrorIs(t, err, fee.ErrFeeRuleNotFound)
}

func TestDelete_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fee_rules WHERE id = $1 AND context_id = $2")).
		WithArgs(id.String(), contextID.String()).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	err := repo.Delete(context.Background(), contextID, id)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete fee rule")
}

func TestDelete_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.Delete(context.Background(), uuid.New(), uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestDelete_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	err := repo.Delete(context.Background(), uuid.New(), uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// ---- DeleteWithTx ----

func TestDeleteWithTx_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.DeleteWithTx(context.Background(), nil, uuid.New(), uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestDeleteWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.DeleteWithTx(context.Background(), nil, uuid.New(), uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

// ---- scanFeeRule ----

func TestScanFeeRule_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	feeScheduleID := uuid.New()
	predicatesJSON, err := json.Marshal([]fee.FieldPredicate{
		{Field: "currency", Operator: fee.PredicateOperatorEquals, Value: "USD"},
	})
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(feeRuleCols).
			AddRow(
				id.String(), contextID.String(), "ANY",
				feeScheduleID.String(), "scan-test", 3,
				predicatesJSON, now, now,
			))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFeeRule(rows)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, fee.MatchingSideAny, result.Side)
	assert.Equal(t, "scan-test", result.Name)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanFeeRule_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Return fewer columns than expected to trigger a scan error.
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id"}).
			AddRow("aaa", "bbb"))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFeeRule(rows)

	require.Error(t, err)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanFeeRule_InvalidPredicatesJSON(t *testing.T) {
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
				id.String(), contextID.String(), "LEFT",
				feeScheduleID.String(), "bad-json", 1,
				[]byte(`{broken`), now, now,
			))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFeeRule(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "unmarshal predicates")
	require.NoError(t, mock.ExpectationsWereMet())
}

// ---- ProviderConnectionError ----

func TestProviderConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("connection failed")

	t.Run("Create", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{PostgresErr: connErr}
		repo := NewRepository(provider)

		err := repo.Create(context.Background(), createTestFeeRule(t))

		require.Error(t, err)
		require.ErrorIs(t, err, connErr)
	})

	t.Run("FindByID", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{PostgresErr: connErr}
		repo := NewRepository(provider)

		result, err := repo.FindByID(context.Background(), uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connErr)
	})

	t.Run("FindByContextID", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{PostgresErr: connErr}
		repo := NewRepository(provider)

		result, err := repo.FindByContextID(context.Background(), uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connErr)
	})

	t.Run("Update", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{PostgresErr: connErr}
		repo := NewRepository(provider)

		err := repo.Update(context.Background(), createTestFeeRule(t))

		require.Error(t, err)
		require.ErrorIs(t, err, connErr)
	})

	t.Run("Delete", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{PostgresErr: connErr}
		repo := NewRepository(provider)

		err := repo.Delete(context.Background(), uuid.New(), uuid.New())

		require.Error(t, err)
		require.ErrorIs(t, err, connErr)
	})
}
