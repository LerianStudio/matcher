//go:build unit

package comment

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var errTestDB = errors.New("database error")

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

func commentColumns() []string {
	return []string{"id", "exception_id", "author", "content", "created_at", "updated_at"}
}

// --- Create with sqlmock ---

func TestCommentRepository_Create_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	commentID := uuid.New()
	exceptionID := uuid.New()

	input := &entities.ExceptionComment{
		ID:          commentID,
		ExceptionID: exceptionID,
		Author:      "analyst@example.com",
		Content:     "This needs review",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	insertQuery := regexp.QuoteMeta(`
				INSERT INTO exception_comments (
					id, exception_id, author, content, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6)
			`)

	selectQuery := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE id = $1
			`)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			commentID.String(),
			exceptionID.String(),
			"analyst@example.com",
			"This needs review",
			now,
			now,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).
		WithArgs(commentID.String()).
		WillReturnRows(sqlmock.NewRows(commentColumns()).
			AddRow(commentID.String(), exceptionID.String(), "analyst@example.com", "This needs review", now, now))
	mock.ExpectCommit()

	result, err := repo.Create(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, commentID, result.ID)
	assert.Equal(t, exceptionID, result.ExceptionID)
	assert.Equal(t, "analyst@example.com", result.Author)
	assert.Equal(t, "This needs review", result.Content)
}

func TestCommentRepository_Create_InsertError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	commentID := uuid.New()
	exceptionID := uuid.New()

	input := &entities.ExceptionComment{
		ID:          commentID,
		ExceptionID: exceptionID,
		Author:      "user@example.com",
		Content:     "Test",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	insertQuery := regexp.QuoteMeta(`
				INSERT INTO exception_comments (
					id, exception_id, author, content, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6)
			`)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			commentID.String(),
			exceptionID.String(),
			"user@example.com",
			"Test",
			now,
			now,
		).
		WillReturnError(errTestDB)
	mock.ExpectRollback()

	result, err := repo.Create(ctx, input)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "create comment")
}

func TestCommentRepository_Create_NilInput_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	result, err := repo.Create(context.Background(), nil)

	require.ErrorIs(t, err, ErrCommentNil)
	require.Nil(t, result)
}

func TestCommentRepository_Create_SelectError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	commentID := uuid.New()
	exceptionID := uuid.New()

	input := &entities.ExceptionComment{
		ID:          commentID,
		ExceptionID: exceptionID,
		Author:      "user@example.com",
		Content:     "Test",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	insertQuery := regexp.QuoteMeta(`
				INSERT INTO exception_comments (
					id, exception_id, author, content, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6)
			`)

	selectQuery := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE id = $1
			`)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			commentID.String(),
			exceptionID.String(),
			"user@example.com",
			"Test",
			now,
			now,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).
		WithArgs(commentID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.Create(ctx, input)

	require.Error(t, err)
	require.Nil(t, result)
}

// --- FindByID with sqlmock ---

func TestCommentRepository_FindByID_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	commentID := uuid.New()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE id = $1
			`)

	mock.ExpectQuery(query).
		WithArgs(commentID.String()).
		WillReturnRows(sqlmock.NewRows(commentColumns()).
			AddRow(commentID.String(), exceptionID.String(), "analyst@example.com", "Test comment", now, now))

	result, err := repo.FindByID(ctx, commentID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, commentID, result.ID)
	assert.Equal(t, exceptionID, result.ExceptionID)
	assert.Equal(t, "analyst@example.com", result.Author)
	assert.Equal(t, "Test comment", result.Content)
}

func TestCommentRepository_FindByID_NotFound_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	commentID := uuid.New()

	query := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE id = $1
			`)

	mock.ExpectQuery(query).
		WithArgs(commentID.String()).
		WillReturnError(sql.ErrNoRows)

	result, err := repo.FindByID(ctx, commentID)

	require.Error(t, err)
	assert.Nil(t, result)
}

func TestCommentRepository_FindByID_QueryError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	commentID := uuid.New()

	query := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE id = $1
			`)

	mock.ExpectQuery(query).
		WithArgs(commentID.String()).
		WillReturnError(errTestDB)

	result, err := repo.FindByID(ctx, commentID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find comment by id")
}

// --- FindByExceptionID with sqlmock ---

func TestCommentRepository_FindByExceptionID_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	commentID1 := uuid.New()
	commentID2 := uuid.New()

	query := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE exception_id = $1
				ORDER BY created_at ASC
			`)

	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnRows(sqlmock.NewRows(commentColumns()).
			AddRow(commentID1.String(), exceptionID.String(), "user1@example.com", "First comment", now, now).
			AddRow(commentID2.String(), exceptionID.String(), "user2@example.com", "Second comment", now.Add(time.Minute), now.Add(time.Minute)))

	result, err := repo.FindByExceptionID(ctx, exceptionID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, commentID1, result[0].ID)
	assert.Equal(t, "First comment", result[0].Content)
	assert.Equal(t, commentID2, result[1].ID)
	assert.Equal(t, "Second comment", result[1].Content)
}

func TestCommentRepository_FindByExceptionID_Empty_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE exception_id = $1
				ORDER BY created_at ASC
			`)

	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnRows(sqlmock.NewRows(commentColumns()))

	result, err := repo.FindByExceptionID(ctx, exceptionID)

	require.NoError(t, err)
	require.Empty(t, result)
}

func TestCommentRepository_FindByExceptionID_QueryError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE exception_id = $1
				ORDER BY created_at ASC
			`)

	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnError(errTestDB)

	result, err := repo.FindByExceptionID(ctx, exceptionID)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "find comments by exception id")
}

func TestCommentRepository_FindByExceptionID_ScanError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE exception_id = $1
				ORDER BY created_at ASC
			`)

	// Return a row with an invalid UUID to trigger a scan/parse error
	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnRows(sqlmock.NewRows(commentColumns()).
			AddRow("not-a-uuid", exceptionID.String(), "user@example.com", "content", time.Now().UTC(), time.Now().UTC()))

	result, err := repo.FindByExceptionID(ctx, exceptionID)

	require.Error(t, err)
	require.Nil(t, result)
}

// --- Delete with sqlmock ---

func TestCommentRepository_Delete_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	commentID := uuid.New()

	deleteQuery := regexp.QuoteMeta(`
				DELETE FROM exception_comments WHERE id = $1
			`)

	mock.ExpectBegin()
	mock.ExpectExec(deleteQuery).
		WithArgs(commentID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(ctx, commentID)

	require.NoError(t, err)
}

func TestCommentRepository_Delete_NotFound_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	commentID := uuid.New()

	deleteQuery := regexp.QuoteMeta(`
				DELETE FROM exception_comments WHERE id = $1
			`)

	mock.ExpectBegin()
	mock.ExpectExec(deleteQuery).
		WithArgs(commentID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(ctx, commentID)

	require.ErrorIs(t, err, ErrCommentNotFound)
}

func TestCommentRepository_Delete_ExecError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	commentID := uuid.New()

	deleteQuery := regexp.QuoteMeta(`
				DELETE FROM exception_comments WHERE id = $1
			`)

	mock.ExpectBegin()
	mock.ExpectExec(deleteQuery).
		WithArgs(commentID.String()).
		WillReturnError(errTestDB)
	mock.ExpectRollback()

	err := repo.Delete(ctx, commentID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete comment")
}

// --- Scan helpers ---

func TestScanCommentInto_InvalidID_Sqlmock(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(commentColumns()).
			AddRow("not-a-uuid", uuid.New().String(), "author", "content", now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanCommentRows(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse comment id")
}

func TestScanCommentInto_InvalidExceptionID_Sqlmock(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(commentColumns()).
			AddRow(uuid.New().String(), "not-a-uuid", "author", "content", now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanCommentRows(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse exception id")
}

func TestScanCommentInto_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	commentID := uuid.New()
	exceptionID := uuid.New()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(commentColumns()).
			AddRow(commentID.String(), exceptionID.String(), "author@test.com", "Good content", now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanCommentRows(rows)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, commentID, result.ID)
	assert.Equal(t, exceptionID, result.ExceptionID)
	assert.Equal(t, "author@test.com", result.Author)
	assert.Equal(t, "Good content", result.Content)
}
