//go:build unit

package adjustment

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

type stubAuditLogRepo struct {
	createWithTxFn func(ctx context.Context, tx *sql.Tx, auditLog *sharedDomain.AuditLog) (*sharedDomain.AuditLog, error)
}

func (s *stubAuditLogRepo) Create(_ context.Context, _ *sharedDomain.AuditLog) (*sharedDomain.AuditLog, error) {
	return nil, nil
}

func (s *stubAuditLogRepo) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	auditLog *sharedDomain.AuditLog,
) (*sharedDomain.AuditLog, error) {
	if s.createWithTxFn != nil {
		return s.createWithTxFn(ctx, tx, auditLog)
	}

	return auditLog, nil
}

func (s *stubAuditLogRepo) GetByID(_ context.Context, _ uuid.UUID) (*sharedDomain.AuditLog, error) {
	return nil, nil
}

func (s *stubAuditLogRepo) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *sharedHTTP.TimestampCursor,
	_ int,
) ([]*sharedDomain.AuditLog, string, error) {
	return nil, "", nil
}

func (s *stubAuditLogRepo) List(
	_ context.Context,
	_ sharedDomain.AuditLogFilter,
	_ *sharedHTTP.TimestampCursor,
	_ int,
) ([]*sharedDomain.AuditLog, string, error) {
	return nil, "", nil
}

func newTestAuditLog(t *testing.T) *sharedDomain.AuditLog {
	t.Helper()

	auditLog, err := sharedDomain.NewAuditLog(
		context.Background(),
		uuid.New(),
		"adjustment",
		uuid.New(),
		"CREATE",
		nil,
		[]byte(`{"ok":true}`),
	)
	require.NoError(t, err)

	return auditLog
}

// --- CreateWithTx tests ---

func TestRepository_CreateWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.CreateWithTx(context.Background(), nil, &matchingEntities.Adjustment{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_CreateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	_, err := repo.CreateWithTx(context.Background(), nil, &matchingEntities.Adjustment{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestRepository_CreateWithTx_NilAdjustment verifies that CreateWithTx rejects a nil adjustment.
// Two sqlmock instances exist: one from setupRepository (initializes the repository's provider)
// and a second explicit sqlmock.New used solely to create a real sql.Tx for the method call.
func TestRepository_CreateWithTx_NilAdjustment(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = repo.CreateWithTx(context.Background(), tx, nil)
	require.ErrorIs(t, err, ErrAdjustmentEntityNeeded)
}

func TestRepository_CreateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Test reason",
		"user@example.com",
	)
	require.NoError(t, err)

	_, err = repo.CreateWithTx(ctx, nil, adjustment)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_CreateWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
	)
	require.NoError(t, err)

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
	)

	selectQuery := regexp.QuoteMeta("SELECT " + columns + " FROM adjustments WHERE id=$1")

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustment.ID.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).WithArgs(adjustment.ID.String()).WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.CreateWithTx(ctx, tx, adjustment)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, adjustment.ID, result.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_CreateWithTx_InsertError(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
	)
	require.NoError(t, err)

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnError(sql.ErrConnDone)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = repo.CreateWithTx(ctx, tx, adjustment)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert adjustment")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_CreateWithAuditLog_NilAuditLogRepo(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
	)
	require.NoError(t, err)

	_, err = repo.CreateWithAuditLog(ctx, adjustment, newTestAuditLog(t))
	require.ErrorIs(t, err, ErrAuditLogRepoRequired)
}

func TestRepository_CreateWithAuditLog_NilAuditLog_RollsBackWithoutInsert(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()
	repo.auditLogRepo = &stubAuditLogRepo{}

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
	)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectRollback()

	_, err = repo.CreateWithAuditLog(ctx, adjustment, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAuditLogRequired)
}

func TestRepository_CreateWithAuditLogWithTx_NilTx(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()
	repo.auditLogRepo = &stubAuditLogRepo{}

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
	)
	require.NoError(t, err)

	_, err = repo.CreateWithAuditLogWithTx(ctx, nil, adjustment, newTestAuditLog(t))
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_CreateWithAuditLog_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	called := false
	repo.auditLogRepo = &stubAuditLogRepo{
		createWithTxFn: func(_ context.Context, tx *sql.Tx, auditLog *sharedDomain.AuditLog) (*sharedDomain.AuditLog, error) {
			called = true
			require.NotNil(t, tx)
			require.NotNil(t, auditLog)
			return auditLog, nil
		},
	}

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
	)
	require.NoError(t, err)

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
	)
	selectQuery := regexp.QuoteMeta("SELECT " + columns + " FROM adjustments WHERE id=$1")
	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustment.ID.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).WithArgs(adjustment.ID.String()).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.CreateWithAuditLog(ctx, adjustment, newTestAuditLog(t))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, called)
	assert.Equal(t, adjustment.ID, result.ID)
}

func TestRepository_CreateWithAuditLog_AuditInsertError_RollsBack(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	repo.auditLogRepo = &stubAuditLogRepo{
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *sharedDomain.AuditLog) (*sharedDomain.AuditLog, error) {
			return nil, errors.New("audit insert failed")
		},
	}

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
	)
	require.NoError(t, err)

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
	)
	selectQuery := regexp.QuoteMeta("SELECT " + columns + " FROM adjustments WHERE id=$1")
	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustment.ID.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).WithArgs(adjustment.ID.String()).WillReturnRows(rows)
	mock.ExpectRollback()

	_, err = repo.CreateWithAuditLog(ctx, adjustment, newTestAuditLog(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist audit log")
}

// --- adjustmentSortValue tests ---

func TestAdjustmentSortValue(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 6, 15, 12, 30, 45, 123456789, time.UTC)
	adjID := uuid.New()

	adj := &matchingEntities.Adjustment{
		ID:        adjID,
		Type:      matchingEntities.AdjustmentTypeBankFee,
		CreatedAt: now,
	}

	tests := []struct {
		name     string
		column   string
		expected string
	}{
		{
			name:     "created_at column",
			column:   sortColumnCreatedAt,
			expected: now.UTC().Format(time.RFC3339Nano),
		},
		{
			name:     "type column",
			column:   sortColumnType,
			expected: string(matchingEntities.AdjustmentTypeBankFee),
		},
		{
			name:     "default falls back to id",
			column:   "unknown_column",
			expected: adjID.String(),
		},
		{
			name:     "empty column falls back to id",
			column:   "",
			expected: adjID.String(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := adjustmentSortValue(adj, tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("nil adjustment returns empty string", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, adjustmentSortValue(nil, sortColumnCreatedAt))
	})
}

// --- ListByContextID with sort by created_at ---

func TestRepository_ListByContextID_SortByCreatedAt(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	adjustmentID := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustmentID.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
		now,
		now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	results, _, err := repo.ListByContextID(ctx, contextID, matchingRepositories.CursorFilter{
		Limit:  10,
		SortBy: "created_at",
	})
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

func TestRepository_ListByContextID_SortByType(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	adjustmentID := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustmentID.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user@example.com",
		now,
		now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	results, _, err := repo.ListByContextID(ctx, contextID, matchingRepositories.CursorFilter{
		Limit:  10,
		SortBy: "type",
	})
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

func TestRepository_ListByContextID_DefaultLimit(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	})

	mock.ExpectQuery("SELECT").WithArgs(contextID.String()).WillReturnRows(rows)

	// Limit 0 should use default 20
	results, _, err := repo.ListByContextID(ctx, contextID, matchingRepositories.CursorFilter{
		Limit: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestRepository_ListByContextID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectQuery("SELECT").
		WithArgs(contextID.String()).
		WillReturnError(sql.ErrConnDone)

	_, _, err := repo.ListByContextID(ctx, contextID, matchingRepositories.CursorFilter{
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query adjustments")
}

func TestRepository_ListByContextID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	// Return invalid data that will fail scan (invalid uuid in ID column)
	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		"not-a-uuid",
		contextID.String(),
		sql.NullString{},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user",
		time.Now().UTC(),
		time.Now().UTC(),
	)

	mock.ExpectQuery("SELECT").WithArgs(contextID.String()).WillReturnRows(rows)

	_, _, err := repo.ListByContextID(ctx, contextID, matchingRepositories.CursorFilter{
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse id")
}

func TestRepository_FindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	adjustmentID := uuid.New()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM adjustments WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), adjustmentID.String()).
		WillReturnError(sql.ErrConnDone)

	_, err := repo.FindByID(ctx, contextID, adjustmentID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "find adjustment transaction")
}

// --- ToEntity error paths ---

func TestPostgreSQLModel_ToEntity_InvalidAdjustmentType(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		Type:      "INVALID_TYPE",
		Direction: "DEBIT",
	}

	entity, err := model.ToEntity()
	require.Error(t, err)
	assert.Nil(t, entity)
	assert.ErrorIs(t, err, ErrInvalidAdjustmentType)
}

func TestPostgreSQLModel_ToEntity_InvalidAdjustmentDirection(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		Type:      "BANK_FEE",
		Direction: "INVALID_DIRECTION",
	}

	entity, err := model.ToEntity()
	require.Error(t, err)
	assert.Nil(t, entity)
	assert.ErrorIs(t, err, ErrInvalidAdjustmentDirection)
}

// --- Error coverage for remaining sentinel errors ---

func TestSentinelErrors_Extended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrTransactionRequired", ErrTransactionRequired, "transaction is required"},
		{"ErrInvalidAdjustmentType", ErrInvalidAdjustmentType, "invalid adjustment type"},
		{"ErrInvalidAdjustmentDirection", ErrInvalidAdjustmentDirection, "invalid adjustment direction"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestRepository_ListByMatchGroupID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM adjustments WHERE context_id=$1 AND match_group_id=$2 ORDER BY created_at ASC",
	)

	// Return invalid data (non-uuid in ID) to trigger scan error
	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid",
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Test",
		"Reason",
		"user",
		time.Now().UTC(),
		time.Now().UTC(),
	)

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), matchGroupID.String()).
		WillReturnRows(rows)

	_, err := repo.ListByMatchGroupID(ctx, contextID, matchGroupID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse id")
}

func TestRepository_Create_SelectError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee",
		"Processing fee",
		"user@example.com",
	)
	require.NoError(t, err)

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
	)

	selectQuery := regexp.QuoteMeta("SELECT " + columns + " FROM adjustments WHERE id=$1")

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).
		WithArgs(adjustment.ID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err = repo.Create(ctx, adjustment)
	require.Error(t, err)
}

func TestRepository_ListByContextID_NegativeLimit(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	})

	mock.ExpectQuery("SELECT").WithArgs(contextID.String()).WillReturnRows(rows)

	results, _, err := repo.ListByContextID(ctx, contextID, matchingRepositories.CursorFilter{
		Limit: -5,
	})
	require.NoError(t, err)
	assert.Empty(t, results)
}
