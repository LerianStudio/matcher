//go:build unit

package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestDatabaseError = errors.New("database error")
	errTestQueryFailed   = errors.New("query failed")
)

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

func outboxEventColumns() []string {
	return []string{
		"id", "event_type", "aggregate_id", "payload", "status",
		"attempts", "published_at", "last_error", "created_at", "updated_at",
	}
}

func TestRepository_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.Create(ctx, &entities.OutboxEvent{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ListPending(ctx, 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ListTenants(ctx)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	err = repo.MarkPublished(ctx, uuid.New(), time.Now())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	err = repo.MarkFailed(ctx, uuid.New(), "error", 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ResetStuckProcessing(ctx, 10, time.Now(), 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_NilEvent(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before event check
	repo := &Repository{}
	_, err := repo.Create(context.Background(), nil)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_GetByID_NilID(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before ID check
	repo := &Repository{}
	_, err := repo.GetByID(context.Background(), uuid.Nil)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ListPending_InvalidLimit(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before limit check
	repo := &Repository{}
	_, err := repo.ListPending(context.Background(), 0)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ListPending(context.Background(), -1)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_MarkPublished_NilID(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before ID check
	repo := &Repository{}
	err := repo.MarkPublished(context.Background(), uuid.Nil, time.Now())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_MarkFailed_NilID(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before ID check
	repo := &Repository{}
	err := repo.MarkFailed(context.Background(), uuid.Nil, "error", 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepositorySentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrLimitMustBePositive", ErrLimitMustBePositive},
		{"ErrIDRequired", ErrIDRequired},
		{"ErrMaxAttemptsMustBePositive", ErrMaxAttemptsMustBePositive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		require.NotNil(t, repo)
		require.Nil(t, repo.provider)
	})
}

func TestRepository_ListFailedForRetry_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.ListFailedForRetry(ctx, 10, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ListFailedForRetry_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo := &Repository{}

	_, err := repo.ListFailedForRetry(context.Background(), 0, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ListFailedForRetry(context.Background(), -1, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ResetForRetry_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.ResetForRetry(ctx, 10, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ResetForRetry_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo := &Repository{}

	_, err := repo.ResetForRetry(context.Background(), 0, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ResetForRetry(context.Background(), -1, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ResetStuckProcessing_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo := &Repository{}

	_, err := repo.ResetStuckProcessing(context.Background(), 0, time.Now(), 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ResetStuckProcessing(context.Background(), -1, time.Now(), 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_CreateWithTx_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.CreateWithTx(ctx, nil, &entities.OutboxEvent{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestApplyStatusState_WithNilEvent(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	events := []*entities.OutboxEvent{
		nil,
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
	}

	applyStatusState(events, now, entities.OutboxStatusProcessing)

	require.Nil(t, events[0])
	require.Equal(t, entities.OutboxStatusProcessing, events[1].Status)
	require.Equal(t, now, events[1].UpdatedAt)
}

func TestApplyProcessingState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	events := []*entities.OutboxEvent{
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
	}

	applyProcessingState(events, now)

	for _, event := range events {
		require.Equal(t, entities.OutboxStatusProcessing, event.Status)
		require.Equal(t, now, event.UpdatedAt)
	}
}

func TestCollectEventIDs(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		ids := collectEventIDs([]*entities.OutboxEvent{})
		require.Empty(t, ids)
	})

	t.Run("with nil events", func(t *testing.T) {
		t.Parallel()

		id1 := uuid.New()
		id2 := uuid.New()
		events := []*entities.OutboxEvent{
			{ID: id1},
			nil,
			{ID: id2},
		}

		ids := collectEventIDs(events)

		require.Len(t, ids, 2)
		require.Contains(t, ids, id1)
		require.Contains(t, ids, id2)
	})

	t.Run("with uuid.Nil events", func(t *testing.T) {
		t.Parallel()

		id1 := uuid.New()
		id2 := uuid.New()
		events := []*entities.OutboxEvent{
			{ID: id1},
			{ID: uuid.Nil},
			{ID: id2},
		}

		ids := collectEventIDs(events)

		require.Len(t, ids, 2)
		require.Contains(t, ids, id1)
		require.Contains(t, ids, id2)
	})

	t.Run("with valid events", func(t *testing.T) {
		t.Parallel()

		id1 := uuid.New()
		id2 := uuid.New()
		id3 := uuid.New()
		events := []*entities.OutboxEvent{
			{ID: id1},
			{ID: id2},
			{ID: id3},
		}

		ids := collectEventIDs(events)

		require.Len(t, ids, 3)
		require.Contains(t, ids, id1)
		require.Contains(t, ids, id2)
		require.Contains(t, ids, id3)
	})

	t.Run("all nil and uuid.Nil events returns empty", func(t *testing.T) {
		t.Parallel()

		events := []*entities.OutboxEvent{
			nil,
			{ID: uuid.Nil},
			nil,
			{ID: uuid.Nil},
		}

		ids := collectEventIDs(events)

		require.Empty(t, ids)
	})
}

func TestRepository_GetByID_NilIDWithProvider(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	event, err := repo.GetByID(ctx, uuid.Nil)

	require.ErrorIs(t, err, ErrIDRequired)
	assert.Nil(t, event)
}

func TestRepository_Create_NilEventWithProvider(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	result, err := repo.Create(ctx, nil)

	require.ErrorIs(t, err, entities.ErrOutboxEventRequired)
	assert.Nil(t, result)
}

func TestRepository_MarkPublished_NilIDWithProvider(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	err := repo.MarkPublished(ctx, uuid.Nil, time.Now())

	require.ErrorIs(t, err, ErrIDRequired)
}

func TestRepository_MarkFailed_NilIDWithProvider(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	err := repo.MarkFailed(ctx, uuid.Nil, "error", 10)

	require.ErrorIs(t, err, ErrIDRequired)
}

func TestRepository_ListPending_InvalidLimitWithProvider(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	events, err := repo.ListPending(ctx, 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)

	events, err = repo.ListPending(ctx, -1)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestRepository_ListFailedForRetry_InvalidParams(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now()

	events, err := repo.ListFailedForRetry(ctx, 0, now, 3)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)

	events, err = repo.ListFailedForRetry(ctx, 10, now, 0)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)

	events, err = repo.ListFailedForRetry(ctx, 10, now, -1)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)
}

func TestRepository_ResetForRetry_InvalidParams(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now()

	events, err := repo.ResetForRetry(ctx, 0, now, 3)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)

	events, err = repo.ResetForRetry(ctx, 10, now, 0)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)
}

func TestRepository_ResetStuckProcessing_InvalidLimitWithProvider(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	events, err := repo.ResetStuckProcessing(ctx, 0, time.Now(), 10)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestNewRepository_WithValidProvider(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)

	repo := NewRepository(provider)

	require.NotNil(t, repo)
	require.Equal(t, provider, repo.provider)
}

func TestRepository_GetByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	aggregateID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events WHERE id").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()).
			AddRow(id, "test.event", aggregateID, []byte(`{"key":"value"}`), entities.OutboxStatusPending, 0, nil, "", now, now),
		)
	mock.ExpectCommit()

	event, err := repo.GetByID(ctx, id)

	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, id, event.ID)
	require.Equal(t, "test.event", event.EventType)
	require.Equal(t, aggregateID, event.AggregateID)
}

func TestRepository_GetByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events WHERE id").
		WithArgs(id).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	event, err := repo.GetByID(ctx, id)

	require.Error(t, err)
	require.Nil(t, event)
}

func TestRepository_Create_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	aggregateID := uuid.New()
	now := time.Now().UTC()

	event := &entities.OutboxEvent{
		ID:          id,
		EventType:   "test.event",
		AggregateID: aggregateID,
		Payload:     []byte(`{"key":"value"}`),
		Status:      entities.OutboxStatusPending,
		Attempts:    0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO outbox_events").
		WithArgs(id, event.EventType, aggregateID, event.Payload, event.Status, event.Attempts, nil, "", now, now).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()).
			AddRow(id, event.EventType, aggregateID, event.Payload, event.Status, event.Attempts, nil, "", now, now),
		)
	mock.ExpectCommit()

	result, err := repo.Create(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)
}

func TestRepository_Create_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	aggregateID := uuid.New()
	now := time.Now().UTC()

	event := &entities.OutboxEvent{
		ID:          id,
		EventType:   "test.event",
		AggregateID: aggregateID,
		Payload:     []byte(`{"key":"value"}`),
		Status:      entities.OutboxStatusPending,
		Attempts:    0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO outbox_events").
		WithArgs(id, event.EventType, aggregateID, event.Payload, event.Status, event.Attempts, nil, "", now, now).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	result, err := repo.Create(ctx, event)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_MarkPublished_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	publishedAt := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox_events SET status").
		WithArgs(entities.OutboxStatusPublished, publishedAt, sqlmock.AnyArg(), id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.MarkPublished(ctx, id, publishedAt)

	require.NoError(t, err)
}

func TestRepository_MarkPublished_Error(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	publishedAt := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox_events SET status").
		WithArgs(entities.OutboxStatusPublished, publishedAt, sqlmock.AnyArg(), id).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	err := repo.MarkPublished(ctx, id, publishedAt)

	require.Error(t, err)
}

func TestRepository_MarkFailed_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	errMsg := "some error message"
	maxAttempts := 10

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox_events SET").
		WithArgs(maxAttempts, entities.OutboxStatusInvalid, entities.OutboxStatusFailed, "max dispatch attempts exceeded", errMsg, sqlmock.AnyArg(), id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.MarkFailed(ctx, id, errMsg, maxAttempts)

	require.NoError(t, err)
}

func TestRepository_MarkFailed_Error(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	errMsg := "some error message"
	maxAttempts := 10

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox_events SET").
		WithArgs(maxAttempts, entities.OutboxStatusInvalid, entities.OutboxStatusFailed, "max dispatch attempts exceeded", errMsg, sqlmock.AnyArg(), id).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	err := repo.MarkFailed(ctx, id, errMsg, maxAttempts)

	require.Error(t, err)
}

func TestRepository_MarkFailed_InvalidMaxAttempts(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()

	err := repo.MarkFailed(ctx, id, "error", 0)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)

	err = repo.MarkFailed(ctx, id, "error", -1)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
}

func TestRepository_ListPending_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusPending, 10).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()))
	mock.ExpectCommit()

	events, err := repo.ListPending(ctx, 10)

	require.NoError(t, err)
	require.Empty(t, events)
}

func TestRepository_ListPending_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusPending, 10).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	events, err := repo.ListPending(ctx, 10)

	require.Error(t, err)
	require.Nil(t, events)
}

func TestRepository_ListPendingByType_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.ListPendingByType(ctx, "test.event", 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ListPendingByType_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	_, err := repo.ListPendingByType(context.Background(), "test.event", 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)

	_, err = repo.ListPendingByType(context.Background(), "test.event", -1)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
}

func TestRepository_ListPendingByType_EmptyEventType(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	_, err := repo.ListPendingByType(context.Background(), "", 10)
	require.ErrorIs(t, err, ErrEventTypeRequired)
}

func TestRepository_ListPendingByType_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusPending, "test.event", 10).
		WillReturnRows(sqlmock.NewRows(outboxEventColumns()))
	mock.ExpectCommit()

	events, err := repo.ListPendingByType(ctx, "test.event", 10)

	require.NoError(t, err)
	require.Empty(t, events)
}

func TestRepository_ListPendingByType_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM outbox_events").
		WithArgs(entities.OutboxStatusPending, "test.event", 10).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	events, err := repo.ListPendingByType(ctx, "test.event", 10)

	require.Error(t, err)
	require.Nil(t, events)
}
