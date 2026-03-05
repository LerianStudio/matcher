//go:build unit

package outbox

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
)

type outboxScannerStub struct {
	lastError sql.NullString
}

func (stub outboxScannerStub) Scan(dest ...any) error {
	now := time.Now().UTC()
	publishedAt := now

	if ptr, ok := dest[0].(*uuid.UUID); ok {
		*ptr = uuid.New()
	}

	if ptr, ok := dest[1].(*string); ok {
		*ptr = "test.event"
	}

	if ptr, ok := dest[2].(*uuid.UUID); ok {
		*ptr = uuid.New()
	}

	if ptr, ok := dest[3].(*[]byte); ok {
		*ptr = []byte(`{"ok":true}`)
	}

	if ptr, ok := dest[4].(*entities.OutboxEventStatus); ok {
		*ptr = entities.OutboxStatusPending
	}

	if ptr, ok := dest[5].(*int); ok {
		*ptr = 1
	}

	if ptr, ok := dest[6].(**time.Time); ok {
		*ptr = &publishedAt
	}

	if ptr, ok := dest[7].(*sql.NullString); ok {
		*ptr = stub.lastError
	}

	if ptr, ok := dest[8].(*time.Time); ok {
		*ptr = now
	}

	if ptr, ok := dest[9].(*time.Time); ok {
		*ptr = now
	}

	return nil
}

// --- collectEventIDs Tests ---

func TestCollectEventIDsCov_Empty(t *testing.T) {
	t.Parallel()

	ids := collectEventIDs(nil)
	assert.Empty(t, ids)
}

func TestCollectEventIDsCov_WithEvents(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()
	events := []*entities.OutboxEvent{
		{ID: id1},
		{ID: id2},
	}

	ids := collectEventIDs(events)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, id1)
	assert.Contains(t, ids, id2)
}

func TestCollectEventIDsCov_SkipsNilEvents(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	events := []*entities.OutboxEvent{
		{ID: id1},
		nil,
		{ID: uuid.Nil},
	}

	ids := collectEventIDs(events)
	assert.Len(t, ids, 1)
	assert.Contains(t, ids, id1)
}

func TestCollectEventIDsCov_AllNilOrZero(t *testing.T) {
	t.Parallel()

	events := []*entities.OutboxEvent{
		nil,
		{ID: uuid.Nil},
	}

	ids := collectEventIDs(events)
	assert.Empty(t, ids)
}

// --- applyStatusState Tests ---

func TestApplyStatusState_AppliesStatusToAll(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	events := []*entities.OutboxEvent{
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
	}

	applyStatusState(events, now, entities.OutboxStatusProcessing)

	for _, event := range events {
		assert.Equal(t, entities.OutboxStatusProcessing, event.Status)
		assert.Equal(t, now, event.UpdatedAt)
	}
}

func TestApplyStatusState_SkipsNilEvents(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	events := []*entities.OutboxEvent{
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
		nil,
	}

	// Should not panic.
	applyStatusState(events, now, entities.OutboxStatusProcessing)

	assert.Equal(t, entities.OutboxStatusProcessing, events[0].Status)
}

func TestApplyStatusState_Empty(t *testing.T) {
	t.Parallel()

	// Should not panic.
	applyStatusState(nil, time.Now().UTC(), entities.OutboxStatusProcessing)
}

// --- applyProcessingState Tests ---

func TestApplyProcessingStateCov(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	events := []*entities.OutboxEvent{
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
	}

	applyProcessingState(events, now)

	assert.Equal(t, entities.OutboxStatusProcessing, events[0].Status)
	assert.Equal(t, now, events[0].UpdatedAt)
}

// --- Sentinel Errors Tests ---

func TestSentinelErrorsCov(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "repository not initialized",
			err:      ErrRepositoryNotInitialized,
			expected: "outbox repository not initialized",
		},
		{
			name:     "limit must be positive",
			err:      ErrLimitMustBePositive,
			expected: "limit must be greater than zero",
		},
		{
			name:     "id required",
			err:      ErrIDRequired,
			expected: "id is required",
		},
		{
			name:     "max attempts must be positive",
			err:      ErrMaxAttemptsMustBePositive,
			expected: "maxAttempts must be greater than zero",
		},
		{
			name:     "event type required",
			err:      ErrEventTypeRequired,
			expected: "event type is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.EqualError(t, tt.err, tt.expected)
		})
	}
}

func TestScanOutboxEventCov_NullLastError(t *testing.T) {
	t.Parallel()

	event, err := scanOutboxEvent(outboxScannerStub{lastError: sql.NullString{Valid: false}})

	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Empty(t, event.LastError)
}

func TestScanOutboxEventCov_WithLastError(t *testing.T) {
	t.Parallel()

	event, err := scanOutboxEvent(outboxScannerStub{lastError: sql.NullString{String: "boom", Valid: true}})

	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, "boom", event.LastError)
}

// --- Repository Nil Checks ---

func TestRepositoryCov_ListPending_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	events, err := repo.ListPending(context.Background(), 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListPending_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ListPending(context.Background(), 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListPending_NegativeLimit(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ListPending(context.Background(), -5)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListPendingByType_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	events, err := repo.ListPendingByType(context.Background(), "test", 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListPendingByType_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ListPendingByType(context.Background(), "test", 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListPendingByType_EmptyType(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ListPendingByType(context.Background(), "", 10)
	require.ErrorIs(t, err, ErrEventTypeRequired)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListFailedForRetry_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	events, err := repo.ListFailedForRetry(context.Background(), 10, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListFailedForRetry_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ListFailedForRetry(context.Background(), 0, time.Now(), 3)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ListFailedForRetry_InvalidMaxAttempts(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ListFailedForRetry(context.Background(), 10, time.Now(), 0)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ResetForRetry_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	events, err := repo.ResetForRetry(context.Background(), 10, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, events)
}

func TestRepositoryCov_ResetForRetry_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ResetForRetry(context.Background(), 0, time.Now(), 3)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ResetForRetry_InvalidMaxAttempts(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ResetForRetry(context.Background(), 10, time.Now(), 0)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ResetStuckProcessing_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	events, err := repo.ResetStuckProcessing(context.Background(), 10, time.Now(), 3)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, events)
}

func TestRepositoryCov_ResetStuckProcessing_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ResetStuckProcessing(context.Background(), 0, time.Now(), 3)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_ResetStuckProcessing_InvalidMaxAttempts(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	events, err := repo.ResetStuckProcessing(context.Background(), 10, time.Now(), 0)
	require.ErrorIs(t, err, ErrMaxAttemptsMustBePositive)
	assert.Nil(t, events)
}

func TestRepositoryCov_GetByID_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	event, err := repo.GetByID(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, event)
}

func TestRepositoryCov_GetByID_NilID(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	event, err := repo.GetByID(context.Background(), uuid.Nil)
	require.ErrorIs(t, err, ErrIDRequired)
	assert.Nil(t, event)
}

func TestRepositoryCov_MarkPublished_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	err := repo.MarkPublished(context.Background(), uuid.New(), time.Now())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepositoryCov_MarkPublished_NilID(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	err := repo.MarkPublished(context.Background(), uuid.Nil, time.Now())
	require.ErrorIs(t, err, ErrIDRequired)
}

func TestRepositoryCov_MarkFailed_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	err := repo.MarkFailed(context.Background(), uuid.New(), "error", 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepositoryCov_MarkFailed_NilID(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	err := repo.MarkFailed(context.Background(), uuid.Nil, "error", 10)
	require.ErrorIs(t, err, ErrIDRequired)
}

func TestRepositoryCov_ListTenants_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	tenants, err := repo.ListTenants(context.Background())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, tenants)
}

func TestRepositoryCov_Create_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	event, err := repo.Create(context.Background(), &entities.OutboxEvent{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	assert.Nil(t, event)
}

func TestRepositoryCov_Create_NilEvent(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockV2(t)
	defer finish()

	event, err := repo.Create(context.Background(), nil)
	require.ErrorIs(t, err, entities.ErrOutboxEventRequired)
	assert.Nil(t, event)
}

func TestRepositoryCov_MarkInvalid_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	err := repo.MarkInvalid(context.Background(), uuid.New(), "error")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}
