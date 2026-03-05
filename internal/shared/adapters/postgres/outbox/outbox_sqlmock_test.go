//go:build unit

package outbox

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupMockV2(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

// --- MarkInvalid ---

func TestRepository_MarkInvalid_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	err := repo.MarkInvalid(ctx, uuid.New(), "bad data")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_MarkInvalid_NilID(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()

	err := repo.MarkInvalid(ctx, uuid.Nil, "bad data")
	require.ErrorIs(t, err, ErrIDRequired)
}

func TestRepository_MarkInvalid_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	errMsg := "invalid payload format"

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox_events SET status").
		WithArgs(
			entities.OutboxStatusInvalid,
			errMsg,
			sqlmock.AnyArg(),
			id,
			entities.OutboxStatusPublished,
			entities.OutboxStatusInvalid,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.MarkInvalid(ctx, id, errMsg)

	require.NoError(t, err)
}

func TestRepository_MarkInvalid_Error(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox_events SET status").
		WithArgs(
			entities.OutboxStatusInvalid,
			"bad",
			sqlmock.AnyArg(),
			id,
			entities.OutboxStatusPublished,
			entities.OutboxStatusInvalid,
		).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	err := repo.MarkInvalid(ctx, id, "bad")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "marking invalid")
}

// --- ListPending empty path ---
// NOTE: markEventsProcessing tests require integration tests due to sqlmock
// limitations with []uuid.UUID. See end of file for details.

func TestRepository_ListPendingByType_WithResults_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusPending, "context.created", 5).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()))
	mock.ExpectCommit()

	events, err := repo.ListPendingByType(ctx, "context.created", 5)

	require.NoError(t, err)
	require.Empty(t, events)
}

// --- ListFailedForRetry ---

func TestRepository_ListFailedForRetry_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	id1 := uuid.New()
	aggregateID := uuid.New()
	now := time.Now().UTC()
	failedBefore := now.Add(-5 * time.Minute)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusFailed, 3, failedBefore, 10).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()).
			AddRow(id1, "test.event", aggregateID, []byte(`{}`), entities.OutboxStatusFailed, 1, nil, "previous error", now, now))
	mock.ExpectCommit()

	events, err := repo.ListFailedForRetry(ctx, 10, failedBefore, 3)

	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, id1, events[0].ID)
}

func TestRepository_ListFailedForRetry_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusFailed, 3, now, 10).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()))
	mock.ExpectCommit()

	events, err := repo.ListFailedForRetry(ctx, 10, now, 3)

	require.NoError(t, err)
	require.Empty(t, events)
}

func TestRepository_ListFailedForRetry_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusFailed, 3, now, 10).
		WillReturnError(errTestQueryFailed)
	mock.ExpectRollback()

	events, err := repo.ListFailedForRetry(ctx, 10, now, 3)

	require.Error(t, err)
	require.Nil(t, events)
}

// --- ResetForRetry empty path ---

func TestRepository_ResetForRetry_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusFailed, 3, now, 10).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()))
	mock.ExpectCommit()

	events, err := repo.ResetForRetry(ctx, 10, now, 3)

	require.NoError(t, err)
	require.Empty(t, events)
}

func TestRepository_ResetForRetry_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusFailed, 3, now, 10).
		WillReturnError(errTestQueryFailed)
	mock.ExpectRollback()

	events, err := repo.ResetForRetry(ctx, 10, now, 3)

	require.Error(t, err)
	require.Nil(t, events)
}

// --- ResetStuckProcessing empty/error paths ---

func TestRepository_ResetStuckProcessing_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusProcessing, now, 5, 10).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()))
	mock.ExpectCommit()

	events, err := repo.ResetStuckProcessing(ctx, 10, now, 5)

	require.NoError(t, err)
	require.Empty(t, events)
}

func TestRepository_ResetStuckProcessing_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusProcessing, now, 5, 10).
		WillReturnError(errTestQueryFailed)
	mock.ExpectRollback()

	events, err := repo.ResetStuckProcessing(ctx, 10, now, 5)

	require.Error(t, err)
	require.Nil(t, events)
}

func TestRepository_ResetStuckProcessing_MaxAttemptsInvalid(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()

	events, err := repo.ResetStuckProcessing(ctx, 10, now, 0)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)

	events, err = repo.ResetStuckProcessing(ctx, 10, now, -1)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)
}

// --- ErrEventTypeRequired ---

func TestRepository_ListPendingByType_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.ListPendingByType(context.Background(), "test", 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_MarkInvalid_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.MarkInvalid(context.Background(), uuid.New(), "bad data")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ErrEventTypeRequired(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrEventTypeRequired)
	assert.Equal(t, "event type is required", ErrEventTypeRequired.Error())
}

// markEventsProcessing requires passing []uuid.UUID slices
// which are incompatible with sqlmock's type matching (sqlmock cannot handle
// []uuid.UUID types). These code paths are covered by integration tests.
// See tests/integration/ for coverage of this function.
