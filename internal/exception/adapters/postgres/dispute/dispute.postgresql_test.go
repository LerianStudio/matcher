// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dispute

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
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var _ repositories.DisputeRepository = (*Repository)(nil)

func setupRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		require.NotNil(t, repo)
	})

	t.Run("creates repository with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		require.NotNil(t, repo)
	})
}

func TestRepository_Errors(t *testing.T) {
	t.Parallel()

	t.Run("ErrRepoNotInitialized has correct message", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "dispute repository not initialized", ErrRepoNotInitialized.Error())
	})

	t.Run("ErrDisputeNotFound has correct message", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "dispute not found", ErrDisputeNotFound.Error())
	})

	t.Run("ErrDisputeNil has correct message", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "dispute is nil", ErrDisputeNil.Error())
	})
}

func TestRepository_Create_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.Create(ctx, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_Create_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, err := repo.Create(ctx, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_Create_NilDispute(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.Create(ctx, nil)
	require.ErrorIs(t, err, ErrDisputeNil)
	require.Nil(t, result)
}

func TestRepository_Create_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	testDispute := &dispute.Dispute{
		ID:          disputeID,
		ExceptionID: exceptionID,
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateDraft,
		Description: "Test description",
		OpenedBy:    "test@example.com",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	insertQuery := regexp.QuoteMeta(`
			INSERT INTO disputes (
				id, exception_id, category, state, description, 
				opened_by, resolution, reopen_reason, evidence, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`)

	selectQuery := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"DRAFT",
		"Test description",
		"test@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).WithArgs(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"DRAFT",
		"Test description",
		"test@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).WithArgs(disputeID.String()).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.Create(ctx, testDispute)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, disputeID, result.ID)
	require.Equal(t, exceptionID, result.ExceptionID)
	require.Equal(t, dispute.DisputeCategoryBankFeeError, result.Category)
	require.Equal(t, dispute.DisputeStateDraft, result.State)
}

func TestRepository_FindByID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.FindByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_FindByID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, err := repo.FindByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	missingID := uuid.New()

	query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	mock.ExpectQuery(query).WithArgs(missingID.String()).WillReturnError(sql.ErrNoRows)

	result, err := repo.FindByID(ctx, missingID)
	require.ErrorIs(t, err, ErrDisputeNotFound)
	require.Nil(t, result)
}

func TestRepository_FindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()
	resolution := "Resolved"

	query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"UNRECOGNIZED_CHARGE",
		"WON",
		"Test description",
		"analyst@example.com",
		sql.NullString{String: resolution, Valid: true},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(disputeID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, disputeID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, disputeID, result.ID)
	require.Equal(t, exceptionID, result.ExceptionID)
	require.Equal(t, dispute.DisputeCategoryUnrecognizedCharge, result.Category)
	require.Equal(t, dispute.DisputeStateWon, result.State)
	require.NotNil(t, result.Resolution)
	require.Equal(t, resolution, *result.Resolution)
}

func TestRepository_FindByExceptionID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.FindByExceptionID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_FindByExceptionID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, err := repo.FindByExceptionID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_FindByExceptionID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, exception_id, category, state, description,
			       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
			FROM disputes
			WHERE exception_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnError(sql.ErrNoRows)

	result, err := repo.FindByExceptionID(ctx, exceptionID)
	require.ErrorIs(t, err, ErrDisputeNotFound)
	require.Nil(t, result)
}

func TestRepository_FindByExceptionID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, exception_id, category, state, description,
			       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
			FROM disputes
			WHERE exception_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"OTHER",
		"OPEN",
		"Description",
		"user@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.FindByExceptionID(ctx, exceptionID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, disputeID, result.ID)
	require.Equal(t, exceptionID, result.ExceptionID)
	require.Equal(t, dispute.DisputeCategoryOther, result.Category)
	require.Equal(t, dispute.DisputeStateOpen, result.State)
}

func TestRepository_Update_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.Update(ctx, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_Update_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, err := repo.Update(ctx, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_Update_NilDispute(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.Update(ctx, nil)
	require.ErrorIs(t, err, ErrDisputeNil)
	require.Nil(t, result)
}

func TestRepository_Update_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	testDispute := &dispute.Dispute{
		ID:          disputeID,
		ExceptionID: exceptionID,
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateOpen,
		Description: "Test",
		OpenedBy:    "user@test.com",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	updateQuery := regexp.QuoteMeta(`
			UPDATE disputes SET
				category = $2,
				state = $3,
				description = $4,
				opened_by = $5,
				resolution = $6,
				reopen_reason = $7,
				evidence = $8,
				updated_at = $9
			WHERE id = $1
		`)

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			disputeID.String(),
			"BANK_FEE_ERROR",
			"OPEN",
			"Test",
			"user@test.com",
			sql.NullString{},
			sql.NullString{},
			[]byte("[]"),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, testDispute)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDisputeNotFound)
	require.Nil(t, result)
}

func TestRepository_Update_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()
	resolution := "Resolved successfully"

	testDispute := &dispute.Dispute{
		ID:          disputeID,
		ExceptionID: exceptionID,
		Category:    dispute.DisputeCategoryDuplicateTransaction,
		State:       dispute.DisputeStateWon,
		Description: "Updated description",
		OpenedBy:    "analyst@test.com",
		Resolution:  &resolution,
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	updateQuery := regexp.QuoteMeta(`
			UPDATE disputes SET
				category = $2,
				state = $3,
				description = $4,
				opened_by = $5,
				resolution = $6,
				reopen_reason = $7,
				evidence = $8,
				updated_at = $9
			WHERE id = $1
		`)

	selectQuery := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"DUPLICATE_TRANSACTION",
		"WON",
		"Updated description",
		"analyst@test.com",
		sql.NullString{String: resolution, Valid: true},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			disputeID.String(),
			"DUPLICATE_TRANSACTION",
			"WON",
			"Updated description",
			"analyst@test.com",
			sql.NullString{String: resolution, Valid: true},
			sql.NullString{},
			[]byte("[]"),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(selectQuery).WithArgs(disputeID.String()).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.Update(ctx, testDispute)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, disputeID, result.ID)
	require.Equal(t, dispute.DisputeStateWon, result.State)
	require.NotNil(t, result.Resolution)
	require.Equal(t, resolution, *result.Resolution)
}

func TestStringPtrToNullString(t *testing.T) {
	t.Parallel()

	t.Run("nil string returns empty NullString", func(t *testing.T) {
		t.Parallel()

		result := pgcommon.StringPtrToNullString(nil)
		require.False(t, result.Valid)
		require.Empty(t, result.String)
	})

	t.Run("non-nil string returns valid NullString", func(t *testing.T) {
		t.Parallel()

		value := "test value"
		result := pgcommon.StringPtrToNullString(&value)
		require.True(t, result.Valid)
		require.Equal(t, value, result.String)
	})

	t.Run("empty string returns valid NullString", func(t *testing.T) {
		t.Parallel()

		value := ""
		result := pgcommon.StringPtrToNullString(&value)
		require.True(t, result.Valid)
		require.Empty(t, result.String)
	})
}

func TestNullStringToStringPtr(t *testing.T) {
	t.Parallel()

	t.Run("invalid NullString returns nil", func(t *testing.T) {
		t.Parallel()

		ns := sql.NullString{Valid: false}
		result := pgcommon.NullStringToStringPtr(ns)
		require.Nil(t, result)
	})

	t.Run("valid NullString returns pointer", func(t *testing.T) {
		t.Parallel()

		ns := sql.NullString{String: "test", Valid: true}
		result := pgcommon.NullStringToStringPtr(ns)
		require.NotNil(t, result)
		require.Equal(t, "test", *result)
	})

	t.Run("valid empty NullString returns pointer to empty string", func(t *testing.T) {
		t.Parallel()

		ns := sql.NullString{String: "", Valid: true}
		result := pgcommon.NullStringToStringPtr(ns)
		require.NotNil(t, result)
		require.Empty(t, *result)
	})
}

func TestRepository_FindByID_WithEvidence(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()
	evidenceID := uuid.New()

	evidencePayload := []dispute.Evidence{
		{
			ID:          evidenceID,
			DisputeID:   disputeID,
			Comment:     "Test evidence",
			SubmittedBy: "user@test.com",
			SubmittedAt: now,
		},
	}
	evidenceJSON, err := json.Marshal(evidencePayload)
	require.NoError(t, err)

	query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"OPEN",
		"Description",
		"user@example.com",
		sql.NullString{},
		sql.NullString{},
		evidenceJSON,
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(disputeID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, disputeID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Evidence, 1)
	require.Equal(t, "Test evidence", result.Evidence[0].Comment)
}

func TestRepository_FindByID_NullEvidence(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"DRAFT",
		"Description",
		"user@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("null"),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(disputeID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, disputeID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.Evidence)
}

func TestRepository_FindByID_AllDisputeStates(t *testing.T) {
	t.Parallel()

	states := []struct {
		stateString string
		stateEnum   dispute.DisputeState
	}{
		{"DRAFT", dispute.DisputeStateDraft},
		{"OPEN", dispute.DisputeStateOpen},
		{"PENDING_EVIDENCE", dispute.DisputeStatePendingEvidence},
		{"WON", dispute.DisputeStateWon},
		{"LOST", dispute.DisputeStateLost},
	}

	for _, testState := range states {
		t.Run("state_"+testState.stateString, func(t *testing.T) {
			t.Parallel()

			repo, mock, finish := setupRepository(t)
			defer finish()

			ctx := context.Background()
			now := time.Now().UTC()
			disputeID := uuid.New()
			exceptionID := uuid.New()

			query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

			rows := sqlmock.NewRows([]string{
				"id", "exception_id", "category", "state", "description",
				"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
			}).AddRow(
				disputeID.String(),
				exceptionID.String(),
				"BANK_FEE_ERROR",
				testState.stateString,
				"Description",
				"user@example.com",
				sql.NullString{},
				sql.NullString{},
				[]byte("[]"),
				now,
				now,
			)

			mock.ExpectQuery(query).WithArgs(disputeID.String()).WillReturnRows(rows)

			result, err := repo.FindByID(ctx, disputeID)
			require.NoError(t, err)
			require.Equal(t, testState.stateEnum, result.State)
		})
	}
}

func TestRepository_FindByID_AllCategories(t *testing.T) {
	t.Parallel()

	categories := []struct {
		categoryString string
		categoryEnum   dispute.DisputeCategory
	}{
		{"BANK_FEE_ERROR", dispute.DisputeCategoryBankFeeError},
		{"UNRECOGNIZED_CHARGE", dispute.DisputeCategoryUnrecognizedCharge},
		{"DUPLICATE_TRANSACTION", dispute.DisputeCategoryDuplicateTransaction},
		{"OTHER", dispute.DisputeCategoryOther},
	}

	for _, testCategory := range categories {
		t.Run("category_"+testCategory.categoryString, func(t *testing.T) {
			t.Parallel()

			repo, mock, finish := setupRepository(t)
			defer finish()

			ctx := context.Background()
			now := time.Now().UTC()
			disputeID := uuid.New()
			exceptionID := uuid.New()

			query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

			rows := sqlmock.NewRows([]string{
				"id", "exception_id", "category", "state", "description",
				"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
			}).AddRow(
				disputeID.String(),
				exceptionID.String(),
				testCategory.categoryString,
				"OPEN",
				"Description",
				"user@example.com",
				sql.NullString{},
				sql.NullString{},
				[]byte("[]"),
				now,
				now,
			)

			mock.ExpectQuery(query).WithArgs(disputeID.String()).WillReturnRows(rows)

			result, err := repo.FindByID(ctx, disputeID)
			require.NoError(t, err)
			require.Equal(t, testCategory.categoryEnum, result.Category)
		})
	}
}

func TestRepository_CreateWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()

	result, err := repo.CreateWithTx(ctx, nil, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_CreateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, err := repo.CreateWithTx(ctx, nil, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_CreateWithTx_NilDispute(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.CreateWithTx(ctx, tx, nil)
	require.ErrorIs(t, err, ErrDisputeNil)
	require.Nil(t, result)
}

func TestRepository_CreateWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	testDispute := &dispute.Dispute{
		ID:          uuid.New(),
		ExceptionID: uuid.New(),
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateDraft,
		Description: "Test",
		OpenedBy:    "user@test.com",
		Evidence:    []dispute.Evidence{},
	}

	result, err := repo.CreateWithTx(ctx, nil, testDispute)
	require.ErrorIs(t, err, ErrTransactionRequired)
	require.Nil(t, result)
}

func TestRepository_CreateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	testDispute := &dispute.Dispute{
		ID:          disputeID,
		ExceptionID: exceptionID,
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateDraft,
		Description: "Test description",
		OpenedBy:    "test@example.com",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	insertQuery := regexp.QuoteMeta(`
		INSERT INTO disputes (
			id, exception_id, category, state, description,
			opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`)

	selectQuery := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"DRAFT",
		"Test description",
		"test@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec(insertQuery).WithArgs(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"DRAFT",
		"Test description",
		"test@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).WithArgs(disputeID.String()).WillReturnRows(rows)

	result, err := repo.CreateWithTx(ctx, tx, testDispute)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, disputeID, result.ID)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_UpdateWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()

	result, err := repo.UpdateWithTx(ctx, nil, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_UpdateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, err := repo.UpdateWithTx(ctx, nil, &dispute.Dispute{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestRepository_UpdateWithTx_NilDispute(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.UpdateWithTx(ctx, tx, nil)
	require.ErrorIs(t, err, ErrDisputeNil)
	require.Nil(t, result)
}

func TestRepository_UpdateWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	testDispute := &dispute.Dispute{
		ID:          uuid.New(),
		ExceptionID: uuid.New(),
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateOpen,
		Description: "Test",
		OpenedBy:    "user@test.com",
		Evidence:    []dispute.Evidence{},
	}

	result, err := repo.UpdateWithTx(ctx, nil, testDispute)
	require.ErrorIs(t, err, ErrTransactionRequired)
	require.Nil(t, result)
}

func TestRepository_UpdateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	testDispute := &dispute.Dispute{
		ID:          disputeID,
		ExceptionID: exceptionID,
		Category:    dispute.DisputeCategoryDuplicateTransaction,
		State:       dispute.DisputeStateWon,
		Description: "Updated description",
		OpenedBy:    "analyst@test.com",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	updateQuery := regexp.QuoteMeta(`
		UPDATE disputes SET
			category = $2,
			state = $3,
			description = $4,
			opened_by = $5,
			resolution = $6,
			reopen_reason = $7,
			evidence = $8,
			updated_at = $9
		WHERE id = $1
	`)

	selectQuery := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"DUPLICATE_TRANSACTION",
		"WON",
		"Updated description",
		"analyst@test.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec(updateQuery).
		WithArgs(
			disputeID.String(),
			"DUPLICATE_TRANSACTION",
			"WON",
			"Updated description",
			"analyst@test.com",
			sql.NullString{},
			sql.NullString{},
			[]byte("[]"),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(selectQuery).WithArgs(disputeID.String()).WillReturnRows(rows)

	result, err := repo.UpdateWithTx(ctx, tx, testDispute)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, disputeID, result.ID)
	require.Equal(t, dispute.DisputeStateWon, result.State)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_ExistsForTenant_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()

	exists, err := repo.ExistsForTenant(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.False(t, exists)
}

func TestRepository_ExistsForTenant_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	exists, err := repo.ExistsForTenant(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.False(t, exists)
}

func TestRepository_ExistsForTenant_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	disputeID := uuid.New()

	query := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM disputes WHERE id = $1)`)

	mock.ExpectQuery(query).
		WithArgs(disputeID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	exists, err := repo.ExistsForTenant(ctx, disputeID)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestRepository_ExistsForTenant_NotExists(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	disputeID := uuid.New()

	query := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM disputes WHERE id = $1)`)

	mock.ExpectQuery(query).
		WithArgs(disputeID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	exists, err := repo.ExistsForTenant(ctx, disputeID)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRepository_ExistsForTenant_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	disputeID := uuid.New()

	query := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM disputes WHERE id = $1)`)

	mock.ExpectQuery(query).WithArgs(disputeID.String()).WillReturnError(errors.New("db error"))

	exists, err := repo.ExistsForTenant(ctx, disputeID)
	require.Error(t, err)
	require.False(t, exists)
	require.Contains(t, err.Error(), "failed to check dispute existence")
}

func TestRepository_Create_InsertError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	testDispute := &dispute.Dispute{
		ID:          disputeID,
		ExceptionID: exceptionID,
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateDraft,
		Description: "Test description",
		OpenedBy:    "test@example.com",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	insertQuery := regexp.QuoteMeta(`
		INSERT INTO disputes (
			id, exception_id, category, state, description,
			opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).WithArgs(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"DRAFT",
		"Test description",
		"test@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte("[]"),
		now,
		now,
	).WillReturnError(errors.New("insert error"))
	mock.ExpectRollback()

	result, err := repo.Create(ctx, testDispute)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "create dispute")
}

func TestRepository_FindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	disputeID := uuid.New()

	query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	mock.ExpectQuery(query).
		WithArgs(disputeID.String()).
		WillReturnError(errors.New("database error"))

	result, err := repo.FindByID(ctx, disputeID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "find dispute by id")
}

func TestRepository_FindByExceptionID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, exception_id, category, state, description,
			       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
			FROM disputes
			WHERE exception_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`)

	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnError(errors.New("database error"))

	result, err := repo.FindByExceptionID(ctx, exceptionID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "find dispute by exception id")
}

func TestRepository_Update_UpdateError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	testDispute := &dispute.Dispute{
		ID:          disputeID,
		ExceptionID: exceptionID,
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateOpen,
		Description: "Test",
		OpenedBy:    "user@test.com",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	updateQuery := regexp.QuoteMeta(`
		UPDATE disputes SET
			category = $2,
			state = $3,
			description = $4,
			opened_by = $5,
			resolution = $6,
			reopen_reason = $7,
			evidence = $8,
			updated_at = $9
		WHERE id = $1
	`)

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			disputeID.String(),
			"BANK_FEE_ERROR",
			"OPEN",
			"Test",
			"user@test.com",
			sql.NullString{},
			sql.NullString{},
			[]byte("[]"),
			sqlmock.AnyArg(),
		).
		WillReturnError(errors.New("update error"))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, testDispute)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "update dispute")
}

func TestRepository_FindByID_WithReopenReason(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()
	resolution := "Previously resolved"
	reopenReason := "New evidence found"

	query := regexp.QuoteMeta(`
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}).AddRow(
		disputeID.String(),
		exceptionID.String(),
		"BANK_FEE_ERROR",
		"OPEN",
		"Description",
		"user@example.com",
		sql.NullString{String: resolution, Valid: true},
		sql.NullString{String: reopenReason, Valid: true},
		[]byte("[]"),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(disputeID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, disputeID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Resolution)
	require.Equal(t, resolution, *result.Resolution)
	require.NotNil(t, result.ReopenReason)
	require.Equal(t, reopenReason, *result.ReopenReason)
}

func TestRepository_ErrTransactionRequired(t *testing.T) {
	t.Parallel()

	require.Equal(t, "transaction is required", ErrTransactionRequired.Error())
}
