//go:build unit

package services

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/mock/gomock"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	mocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

// errTestBoom is a sentinel error used for testing failure scenarios.
var errTestBoom = errors.New("boom")

// capturingLogger implements libLog.Logger and captures log entries for assertion.
type capturingLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

type logEntry struct {
	Level   libLog.Level
	Message string
	Fields  []libLog.Field
}

func (l *capturingLogger) Log(_ context.Context, level libLog.Level, msg string, fields ...libLog.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, logEntry{Level: level, Message: msg, Fields: fields})
}

//nolint:ireturn
func (l *capturingLogger) With(_ ...libLog.Field) libLog.Logger { return l }

//nolint:ireturn
func (l *capturingLogger) WithGroup(_ string) libLog.Logger { return l }

func (l *capturingLogger) Enabled(_ libLog.Level) bool { return true }

func (l *capturingLogger) Sync(_ context.Context) error { return nil }

func (l *capturingLogger) getEntries() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	result := make([]logEntry, len(l.entries))
	copy(result, l.entries)

	return result
}

// findEntry searches for a log entry containing the given substring in its message.
func (l *capturingLogger) findEntry(msgSubstring string) *logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	for i := range l.entries {
		if strings.Contains(l.entries[i].Message, msgSubstring) {
			entry := l.entries[i]

			return &entry
		}
	}

	return nil
}

func getStringField(entry *logEntry, key string) (string, bool) {
	if entry == nil {
		return "", false
	}

	for _, field := range entry.Fields {
		if field.Key != key {
			continue
		}

		value, ok := field.Value.(string)
		if !ok {
			return "", false
		}

		return value, true
	}

	return "", false
}

func TestDispatcherPublishEventNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	err = dispatcher.publishEvent(context.Background(), nil)
	require.Error(t, err)
}

func TestDispatcherDispatchOncePublishes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	ctx := context.Background()
	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), event.ID, gomock.Any()).Return(nil)

	dispatcher.DispatchOnce(ctx)
	require.True(t, publisher.completedCalled)
}

func TestDispatcherDispatchOnceMarksFailed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{err: errTestBoom}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	ctx := context.Background()
	payload := ingestionEntities.IngestionFailedEvent{
		EventType: ingestionEntities.EventTypeIngestionFailed,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Error:     "boom",
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionFailed,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().MarkFailed(gomock.Any(), event.ID, gomock.Any(), dispatcher.maxDispatchAttempts).Return(nil)

	dispatcher.DispatchOnce(ctx)
	require.True(t, publisher.failedCalled)
}

func TestDispatcherDispatchOncePublishesMatchConfirmed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	payload := sharedDomain.MatchConfirmedEvent{
		EventType:      sharedDomain.EventTypeMatchConfirmed,
		TenantID:       uuid.New(),
		TenantSlug:     "default",
		ContextID:      uuid.New(),
		RunID:          uuid.New(),
		MatchID:        uuid.New(),
		RuleID:         uuid.New(),
		TransactionIDs: []uuid.UUID{uuid.New()},
		Confidence:     90,
		ConfirmedAt:    time.Now().UTC(),
		Timestamp:      time.Now().UTC(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: sharedDomain.EventTypeMatchConfirmed,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), event.ID, gomock.Any()).Return(nil)

	dispatcher.DispatchOnce(context.Background())
	require.True(t, publisher.matchConfirmedCalled)
}

func TestDispatcherDispatchOncePublishesMatchUnmatched(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	payload := sharedDomain.MatchUnmatchedEvent{
		EventType:      sharedDomain.EventTypeMatchUnmatched,
		TenantID:       uuid.New(),
		TenantSlug:     "default",
		ContextID:      uuid.New(),
		RunID:          uuid.New(),
		MatchID:        uuid.New(),
		RuleID:         uuid.New(),
		TransactionIDs: []uuid.UUID{uuid.New()},
		Reason:         "revoked by user",
		Timestamp:      time.Now().UTC(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: sharedDomain.EventTypeMatchUnmatched,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), event.ID, gomock.Any()).Return(nil)

	dispatcher.DispatchOnce(context.Background())
	require.True(t, publisher.matchUnmatchedCalled)
}

func TestDispatcherPublishMatchUnmatchedBadJSON(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: sharedDomain.EventTypeMatchUnmatched,
		Payload:   []byte(`{invalid json`),
	}

	err = dispatcher.publishEvent(context.Background(), event)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidPayload)
}

func TestValidateMatchUnmatchedPayload_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		payload     sharedDomain.MatchUnmatchedEvent
		expectedErr error
	}{
		{
			name: "missing tenant ID",
			payload: sharedDomain.MatchUnmatchedEvent{
				TenantID:       uuid.Nil,
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
				Reason:         "test",
			},
			expectedErr: ErrMissingTenantID,
		},
		{
			name: "missing context ID",
			payload: sharedDomain.MatchUnmatchedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.Nil,
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
				Reason:         "test",
			},
			expectedErr: ErrMissingContextID,
		},
		{
			name: "missing run ID",
			payload: sharedDomain.MatchUnmatchedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.Nil,
				MatchID:        uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
				Reason:         "test",
			},
			expectedErr: ErrMissingMatchRunID,
		},
		{
			name: "missing match ID",
			payload: sharedDomain.MatchUnmatchedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.Nil,
				TransactionIDs: []uuid.UUID{uuid.New()},
				Reason:         "test",
			},
			expectedErr: ErrMissingMatchID,
		},
		{
			name: "missing transaction IDs",
			payload: sharedDomain.MatchUnmatchedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				TransactionIDs: nil,
				Reason:         "test",
			},
			expectedErr: ErrMissingTransactionIDs,
		},
		{
			name: "missing reason",
			payload: sharedDomain.MatchUnmatchedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
				Reason:         "",
			},
			expectedErr: ErrMissingReason,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			repo := mocks.NewMockOutboxRepository(ctrl)
			publisher := &stubPublisher{}
			dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
			require.NoError(t, err)

			body, err := json.Marshal(tc.payload)
			require.NoError(t, err)

			event := &outboxEntities.OutboxEvent{
				ID:        uuid.New(),
				EventType: sharedDomain.EventTypeMatchUnmatched,
				Payload:   body,
			}

			err = dispatcher.publishEvent(context.Background(), event)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestDispatcherPublishEventUnsupported(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	err = dispatcher.publishEvent(
		context.Background(),
		&outboxEntities.OutboxEvent{EventType: "unknown", Payload: []byte(`{}`)},
	)
	require.Error(t, err)
}

func TestDispatcherPublishEventBadPayload(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	err = dispatcher.publishEvent(
		context.Background(),
		&outboxEntities.OutboxEvent{
			EventType: ingestionEntities.EventTypeIngestionCompleted,
			Payload:   []byte(`{`),
		},
	)
	require.Error(t, err)
}

func TestDispatcherPublishEventMissingPayload(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	err = dispatcher.publishEvent(
		context.Background(),
		&outboxEntities.OutboxEvent{EventType: ingestionEntities.EventTypeIngestionCompleted},
	)
	require.Error(t, err)
}

func TestDispatcherPublishEventMissingFields(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	err = dispatcher.publishEvent(
		context.Background(),
		&outboxEntities.OutboxEvent{
			EventType: ingestionEntities.EventTypeIngestionCompleted,
			Payload:   body,
		},
	)
	require.Error(t, err)
}

func TestDispatcherPublishEventPayload(t *testing.T) {
	t.Parallel()

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	err = dispatcher.publishEvent(
		context.Background(),
		&outboxEntities.OutboxEvent{
			EventType: ingestionEntities.EventTypeIngestionCompleted,
			Payload:   body,
		},
	)
	require.NoError(t, err)
}

func TestNewDispatcherNilRepository(t *testing.T) {
	t.Parallel()

	publisher := &stubPublisher{}
	_, err := NewDispatcher(nil, publisher, publisher, nil, nil)
	require.ErrorIs(t, err, ErrOutboxRepositoryRequired)
}

func TestNewDispatcherNilIngestionPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	_, err := NewDispatcher(repo, nil, publisher, nil, nil)
	require.ErrorIs(t, err, ErrIngestionPublisherRequired)
}

func TestNewDispatcherNilMatchingPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	_, err := NewDispatcher(repo, publisher, nil, nil, nil)
	require.ErrorIs(t, err, ErrMatchingPublisherRequired)
}

func TestDispatcherDispatchOnceListPendingError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.listPendingFailureThreshold = 1

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().ListPending(gomock.Any(), dispatcher.batchSize).Return(nil, errTestBoom)

	require.NotPanics(t, func() {
		dispatcher.DispatchOnce(context.Background())
	})
	require.Equal(
		t,
		1,
		dispatcher.listPendingFailureCounts["_default"],
		"failure count should increment on ListPending error",
	)
}

func TestDispatcherDispatchOnceMarkPublishedError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), event.ID, gomock.Any()).Return(errTestBoom)

	dispatcher.DispatchOnce(context.Background())
}

func TestDispatcherDispatchOnceMarkFailedError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{err: errTestBoom}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	payload := ingestionEntities.IngestionFailedEvent{
		EventType: ingestionEntities.EventTypeIngestionFailed,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Error:     "boom",
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionFailed,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().MarkFailed(gomock.Any(), event.ID, gomock.Any(), dispatcher.maxDispatchAttempts).Return(errTestBoom)

	dispatcher.DispatchOnce(context.Background())
}

func TestDispatcherDispatchOnceNilEvent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{nil}, nil)

	dispatcher.DispatchOnce(context.Background())
}

type stubPublisher struct {
	completedCalled      bool
	failedCalled         bool
	matchConfirmedCalled bool
	matchUnmatchedCalled bool
	err                  error
}

func (s *stubPublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *ingestionEntities.IngestionCompletedEvent,
) error {
	s.completedCalled = true
	return s.err
}

func (s *stubPublisher) PublishIngestionFailed(
	_ context.Context,
	_ *ingestionEntities.IngestionFailedEvent,
) error {
	s.failedCalled = true
	return s.err
}

func (s *stubPublisher) PublishMatchConfirmed(
	_ context.Context,
	_ *sharedDomain.MatchConfirmedEvent,
) error {
	s.matchConfirmedCalled = true
	return s.err
}

func (s *stubPublisher) PublishMatchUnmatched(
	_ context.Context,
	_ *sharedDomain.MatchUnmatchedEvent,
) error {
	s.matchUnmatchedCalled = true
	return s.err
}

func TestDispatcherStopIdempotent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.Stop()
	dispatcher.Stop()
}

func TestDispatcherStopNil(t *testing.T) {
	t.Parallel()

	var dispatcher *Dispatcher

	dispatcher.Stop()
}

func TestDispatcherRunNil(t *testing.T) {
	t.Parallel()

	var dispatcher *Dispatcher

	require.Error(t, dispatcher.Run(nil))
}

func TestDispatcherRunStops(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	// Use a channel to signal when the dispatcher has started processing
	dispatchStarted := make(chan struct{})

	repo.EXPECT().ListTenants(gomock.Any()).Return([]string{auth.DefaultTenantID}, nil).AnyTimes()
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		DoAndReturn(func(context.Context, int, time.Time, int) ([]*outboxEntities.OutboxEvent, error) {
			select {
			case <-dispatchStarted:
			default:
				close(dispatchStarted)
			}

			return []*outboxEntities.OutboxEvent{}, nil
		}).
		AnyTimes()
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()

	go func() {
		<-dispatchStarted
		dispatcher.Stop()
	}()

	require.NoError(t, dispatcher.Run(nil))
}

func TestValidateMatchConfirmedPayload_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		payload     sharedDomain.MatchConfirmedEvent
		expectedErr error
	}{
		{
			name: "missing tenant ID",
			payload: sharedDomain.MatchConfirmedEvent{
				TenantID:       uuid.Nil,
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				RuleID:         uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
			},
			expectedErr: ErrMissingTenantID,
		},
		{
			name: "missing context ID",
			payload: sharedDomain.MatchConfirmedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.Nil,
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				RuleID:         uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
			},
			expectedErr: ErrMissingContextID,
		},
		{
			name: "missing run ID",
			payload: sharedDomain.MatchConfirmedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.Nil,
				MatchID:        uuid.New(),
				RuleID:         uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
			},
			expectedErr: ErrMissingMatchRunID,
		},
		{
			name: "missing match ID",
			payload: sharedDomain.MatchConfirmedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.Nil,
				RuleID:         uuid.New(),
				TransactionIDs: []uuid.UUID{uuid.New()},
			},
			expectedErr: ErrMissingMatchID,
		},
		{
			name: "missing rule ID",
			payload: sharedDomain.MatchConfirmedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				RuleID:         uuid.Nil,
				TransactionIDs: []uuid.UUID{uuid.New()},
			},
			expectedErr: ErrMissingMatchRuleID,
		},
		{
			name: "missing transaction IDs",
			payload: sharedDomain.MatchConfirmedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				RuleID:         uuid.New(),
				TransactionIDs: nil,
			},
			expectedErr: ErrMissingTransactionIDs,
		},
		{
			name: "empty transaction IDs",
			payload: sharedDomain.MatchConfirmedEvent{
				TenantID:       uuid.New(),
				ContextID:      uuid.New(),
				RunID:          uuid.New(),
				MatchID:        uuid.New(),
				RuleID:         uuid.New(),
				TransactionIDs: []uuid.UUID{},
			},
			expectedErr: ErrMissingTransactionIDs,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			repo := mocks.NewMockOutboxRepository(ctrl)
			publisher := &stubPublisher{}
			dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
			require.NoError(t, err)

			body, err := json.Marshal(tc.payload)
			require.NoError(t, err)

			event := &outboxEntities.OutboxEvent{
				ID:        uuid.New(),
				EventType: sharedDomain.EventTypeMatchConfirmed,
				Payload:   body,
			}

			err = dispatcher.publishEvent(context.Background(), event)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestDispatcherPublishMatchConfirmedBadJSON(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: sharedDomain.EventTypeMatchConfirmed,
		Payload:   []byte(`{invalid json`),
	}

	err = dispatcher.publishEvent(context.Background(), event)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidPayload)
}

func TestNewDispatcher_NilLoggerDefaultsBehavior(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, dispatcher.logger)

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), event.ID, gomock.Any()).Return(nil)

	require.NotPanics(t, func() {
		dispatcher.DispatchOnce(context.Background())
	})
}

func TestDispatcherDispatchOnce_ContextCancellation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{}, nil)

	require.NotPanics(t, func() {
		dispatcher.DispatchOnce(ctx)
	})
}

func TestDispatcherDispatchOnceProcessesFailedEvents(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	failedEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{failedEvent}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize-1).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), failedEvent.ID, gomock.Any()).Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 1, processed)
	require.True(t, publisher.completedCalled)
}

func TestDispatcherDispatchOnceProcessesBothFailedAndPending(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1
	dispatcher.batchSize = 10

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	failedEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	pendingEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, 10).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), 10, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().ResetForRetry(gomock.Any(), 10, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{failedEvent}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), 9).
		Return([]*outboxEntities.OutboxEvent{pendingEvent}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), failedEvent.ID, gomock.Any()).Return(nil)
	repo.EXPECT().MarkPublished(gomock.Any(), pendingEvent.ID, gomock.Any()).Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 2, processed)
}

func TestDispatcherDispatchOnceResetForRetryError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	pendingEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return(nil, errTestBoom)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{pendingEvent}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), pendingEvent.ID, gomock.Any()).Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 1, processed)
}

func TestDispatcherSetRetryWindow(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	require.Equal(t, defaultRetryWindow, dispatcher.retryWindow)

	dispatcher.SetRetryWindow(10 * time.Minute)
	require.Equal(t, 10*time.Minute, dispatcher.retryWindow)

	dispatcher.SetRetryWindow(0)
	require.Equal(t, 10*time.Minute, dispatcher.retryWindow)

	dispatcher.SetRetryWindow(-1 * time.Minute)
	require.Equal(t, 10*time.Minute, dispatcher.retryWindow)
}

func TestDispatcherSetMaxDispatchAttempts(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	require.Equal(t, defaultMaxDispatchAttempts, dispatcher.maxDispatchAttempts)

	dispatcher.SetMaxDispatchAttempts(5)
	require.Equal(t, 5, dispatcher.maxDispatchAttempts)

	dispatcher.SetMaxDispatchAttempts(0)
	require.Equal(t, 5, dispatcher.maxDispatchAttempts)

	dispatcher.SetMaxDispatchAttempts(-1)
	require.Equal(t, 5, dispatcher.maxDispatchAttempts)
}

func TestDispatcherRunRecoversFromPanic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.interval = 5 * time.Millisecond

	panicCalled := make(chan struct{})
	secondTick := make(chan struct{})

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		DoAndReturn(func(context.Context, int, time.Time, int) ([]*outboxEntities.OutboxEvent, error) {
			close(panicCalled)
			panic("boom")
		}).Times(1)
	repo.EXPECT().ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil).AnyTimes()
	repo.EXPECT().ListTenants(gomock.Any()).Return([]string{auth.DefaultTenantID}, nil).AnyTimes()
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().ListPending(gomock.Any(), dispatcher.batchSize).
		DoAndReturn(func(context.Context, int) ([]*outboxEntities.OutboxEvent, error) {
			select {
			case <-secondTick:
			default:
				close(secondTick)
			}

			return []*outboxEntities.OutboxEvent{}, nil
		}).MinTimes(1)

	done := make(chan error, 1)

	go func() {
		done <- dispatcher.Run(nil)
	}()

	select {
	case <-panicCalled:
	case <-time.After(200 * time.Millisecond):
		dispatcher.Stop()
		require.FailNow(t, "dispatcher did not hit panic path")
	}

	select {
	case <-secondTick:
	case <-time.After(200 * time.Millisecond):
		dispatcher.Stop()
		require.FailNow(t, "dispatcher did not tick after panic")
	}

	dispatcher.Stop()
	require.NoError(t, <-done)
}

func TestDispatcherCollectPriorityEventsFirst(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	auditPub := &stubAuditPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil, WithAuditPublisher(auditPub))
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1
	dispatcher.batchSize = 5

	auditPayload := sharedDomain.AuditLogCreatedEvent{
		TenantID:   uuid.New(),
		EntityType: "match",
		EntityID:   uuid.New(),
		Action:     "created",
	}
	auditBody, err := json.Marshal(auditPayload)
	require.NoError(t, err)

	auditEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: sharedDomain.EventTypeAuditLogCreated,
		Payload:   auditBody,
	}

	ingestionPayload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	ingestionBody, err := json.Marshal(ingestionPayload)
	require.NoError(t, err)

	pendingEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   ingestionBody,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, 5).
		Return([]*outboxEntities.OutboxEvent{auditEvent}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), 4, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), 4, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), 4).
		Return([]*outboxEntities.OutboxEvent{pendingEvent}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), auditEvent.ID, gomock.Any()).Return(nil)
	repo.EXPECT().MarkPublished(gomock.Any(), pendingEvent.ID, gomock.Any()).Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 2, processed)
	require.True(t, auditPub.called)
	require.True(t, publisher.completedCalled)
}

func TestDispatcherFailedBatchCap(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	failedEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{failedEvent}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize-1).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().MarkPublished(gomock.Any(), failedEvent.ID, gomock.Any()).Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 1, processed)
}

type stubAuditPublisher struct {
	called bool
	err    error
}

func (s *stubAuditPublisher) PublishAuditLogCreated(
	_ context.Context,
	_ *sharedDomain.AuditLogCreatedEvent,
) error {
	s.called = true
	return s.err
}

// selectivePublisher succeeds for ingestion-completed and match events but fails
// for ingestion-failed events when failIngestionFailed is true. This allows testing
// mixed success/failure outcomes within a single dispatch batch.
type selectivePublisher struct {
	completedCount      int
	failedAttemptCount  int
	failIngestionFailed bool
}

func (s *selectivePublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *ingestionEntities.IngestionCompletedEvent,
) error {
	s.completedCount++
	return nil
}

func (s *selectivePublisher) PublishIngestionFailed(
	_ context.Context,
	_ *ingestionEntities.IngestionFailedEvent,
) error {
	s.failedAttemptCount++
	if s.failIngestionFailed {
		return errTestBoom
	}

	return nil
}

func (s *selectivePublisher) PublishMatchConfirmed(
	_ context.Context,
	_ *sharedDomain.MatchConfirmedEvent,
) error {
	return nil
}

func (s *selectivePublisher) PublishMatchUnmatched(
	_ context.Context,
	_ *sharedDomain.MatchUnmatchedEvent,
) error {
	return nil
}

func TestDispatcherDispatchOnce_BatchMixedOutcomes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)

	// Use a selectivePublisher that fails only for ingestion-failed events
	// and succeeds for ingestion-completed events.
	publisher := &selectivePublisher{
		failIngestionFailed: true,
	}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	// Build a successful event (ingestion completed).
	successPayload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	successBody, err := json.Marshal(successPayload)
	require.NoError(t, err)

	successEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   successBody,
	}

	// Build a failing event (ingestion failed -- publisher will error on this type).
	failPayload := ingestionEntities.IngestionFailedEvent{
		EventType: ingestionEntities.EventTypeIngestionFailed,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Error:     "upstream failure",
	}
	failBody, err := json.Marshal(failPayload)
	require.NoError(t, err)

	failEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionFailed,
		Payload:   failBody,
	}

	// Build a second successful event (ingestion completed).
	successPayload2 := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	successBody2, err := json.Marshal(successPayload2)
	require.NoError(t, err)

	successEvent2 := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   successBody2,
	}

	// Expect: collect returns all 3 events.
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{successEvent, failEvent, successEvent2}, nil)

	// Successful events get MarkPublished; failed event gets MarkFailed.
	repo.EXPECT().MarkPublished(gomock.Any(), successEvent.ID, gomock.Any()).Return(nil)
	repo.EXPECT().MarkFailed(gomock.Any(), failEvent.ID, gomock.Any(), dispatcher.maxDispatchAttempts).Return(nil)
	repo.EXPECT().MarkPublished(gomock.Any(), successEvent2.ID, gomock.Any()).Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 3, processed, "all 3 events should be counted as processed")
	require.Equal(t, 2, publisher.completedCount, "two ingestion-completed events should succeed")
	require.Equal(t, 1, publisher.failedAttemptCount, "one ingestion-failed event should be attempted")
}

func TestDispatcherSetProcessingTimeout(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	// Verify default.
	require.Equal(t, defaultProcessingTimeout, dispatcher.processingTimeout)

	// Set a custom value.
	dispatcher.SetProcessingTimeout(20 * time.Minute)
	require.Equal(t, 20*time.Minute, dispatcher.processingTimeout)

	// Zero should be ignored.
	dispatcher.SetProcessingTimeout(0)
	require.Equal(t, 20*time.Minute, dispatcher.processingTimeout)

	// Negative should be ignored.
	dispatcher.SetProcessingTimeout(-1 * time.Minute)
	require.Equal(t, 20*time.Minute, dispatcher.processingTimeout)
}

func TestDispatcherShutdownDrainsInFlight(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.interval = 50 * time.Millisecond

	dispatchStarted := make(chan struct{})

	repo.EXPECT().ListTenants(gomock.Any()).Return([]string{auth.DefaultTenantID}, nil).AnyTimes()
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		DoAndReturn(func(context.Context, int, time.Time, int) ([]*outboxEntities.OutboxEvent, error) {
			select {
			case <-dispatchStarted:
			default:
				close(dispatchStarted)
			}

			return []*outboxEntities.OutboxEvent{}, nil
		}).
		AnyTimes()
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()

	done := make(chan error, 1)

	go func() {
		done <- dispatcher.Run(nil)
	}()

	<-dispatchStarted

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = dispatcher.Shutdown(ctx)
	require.NoError(t, err)
	require.NoError(t, <-done)
}

func TestDispatcherShutdownTimesOut(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	// Manually add to the WaitGroup to simulate an in-flight dispatch that never finishes
	dispatcher.dispatchWg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = dispatcher.Shutdown(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Clean up: unblock the WaitGroup so goroutine exits
	dispatcher.dispatchWg.Done()
}

func TestDispatcherListPendingFailureCountPerTenant(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.listPendingFailureThreshold = 5

	tenantA := "tenant-aaa"
	tenantB := "tenant-bbb"

	// Two calls for tenant A, one for tenant B — all fail ListPending
	for range 2 {
		ctxA := context.WithValue(context.Background(), auth.TenantIDKey, tenantA)

		repo.EXPECT().
			ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
			Return([]*outboxEntities.OutboxEvent{}, nil)
		repo.EXPECT().
			ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
			Return([]*outboxEntities.OutboxEvent{}, nil)
		repo.EXPECT().
			ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
			Return([]*outboxEntities.OutboxEvent{}, nil)
		repo.EXPECT().
			ListPending(gomock.Any(), dispatcher.batchSize).
			Return(nil, errTestBoom)

		dispatcher.DispatchOnce(ctxA)
	}

	ctxB := context.WithValue(context.Background(), auth.TenantIDKey, tenantB)

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return(nil, errTestBoom)

	dispatcher.DispatchOnce(ctxB)

	require.Equal(t, 2, dispatcher.listPendingFailureCounts[tenantA])
	require.Equal(t, 1, dispatcher.listPendingFailureCounts[tenantB])
}

func TestDispatcherListPendingFailureCountResetsOnSuccess(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	tenantA := "tenant-reset"
	ctxA := context.WithValue(context.Background(), auth.TenantIDKey, tenantA)

	// First call fails
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return(nil, errTestBoom)

	dispatcher.DispatchOnce(ctxA)
	require.Equal(t, 1, dispatcher.listPendingFailureCounts[tenantA])

	// Second call succeeds — counter should be cleared
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{}, nil)

	dispatcher.DispatchOnce(ctxA)
	require.Equal(t, 0, dispatcher.listPendingFailureCounts[tenantA])
}

func TestDispatcherAuditPublisherNotConfigured(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	// No WithAuditPublisher — auditPub is nil
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	auditPayload := sharedDomain.AuditLogCreatedEvent{
		TenantID:   uuid.New(),
		EntityType: "match",
		EntityID:   uuid.New(),
		Action:     "created",
	}
	body, err := json.Marshal(auditPayload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: sharedDomain.EventTypeAuditLogCreated,
		Payload:   body,
	}

	// The event should be marked as FAILED (retryable), not INVALID
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize-1, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize-1).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().MarkFailed(gomock.Any(), event.ID, gomock.Any(), dispatcher.maxDispatchAttempts).Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 1, processed)
}

func TestDispatcherAuditPublisherNotConfiguredErrorType(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	err = dispatcher.publishEvent(context.Background(), &outboxEntities.OutboxEvent{
		EventType: sharedDomain.EventTypeAuditLogCreated,
		Payload:   []byte(`{}`),
	})
	require.ErrorIs(t, err, ErrAuditPublisherNotConfigured)
	require.False(t, isNonRetryableError(err), "ErrAuditPublisherNotConfigured must be retryable")
}

func TestDeduplicateEvents(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()

	tests := []struct {
		name     string
		input    []*outboxEntities.OutboxEvent
		expected int
	}{
		{
			name:     "empty slice",
			input:    []*outboxEntities.OutboxEvent{},
			expected: 0,
		},
		{
			name:     "nil slice",
			input:    nil,
			expected: 0,
		},
		{
			name: "no duplicates",
			input: []*outboxEntities.OutboxEvent{
				{ID: id1, EventType: "a"},
				{ID: id2, EventType: "b"},
			},
			expected: 2,
		},
		{
			name: "with duplicates",
			input: []*outboxEntities.OutboxEvent{
				{ID: id1, EventType: "priority"},
				{ID: id2, EventType: "b"},
				{ID: id1, EventType: "pending-duplicate"},
			},
			expected: 2,
		},
		{
			name: "filters nil entries",
			input: []*outboxEntities.OutboxEvent{
				{ID: id1, EventType: "a"},
				nil,
				{ID: id1, EventType: "dup"},
			},
			expected: 1, // nil filtered out, dup removed
		},
		{
			name: "preserves first occurrence order",
			input: []*outboxEntities.OutboxEvent{
				{ID: id1, EventType: "priority"},
				{ID: id2, EventType: "pending"},
				{ID: id1, EventType: "should-be-dropped"},
			},
			expected: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := deduplicateEvents(tc.input)
			require.Len(t, result, tc.expected)
		})
	}
}

func TestDeduplicateEventsPreservesOrder(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()

	input := []*outboxEntities.OutboxEvent{
		{ID: id1, EventType: "priority"},
		{ID: id2, EventType: "pending"},
		{ID: id1, EventType: "duplicate-from-pending"},
	}

	result := deduplicateEvents(input)
	require.Len(t, result, 2)
	require.Equal(t, "priority", result[0].EventType,
		"first occurrence should be preserved, not the duplicate")
	require.Equal(t, "pending", result[1].EventType)
}

func TestWithBatchSize(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil, WithBatchSize(100))
	require.NoError(t, err)
	require.Equal(t, 100, dispatcher.batchSize)
}

func TestWithBatchSizeIgnoresInvalid(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil, WithBatchSize(0))
	require.NoError(t, err)
	require.Equal(t, defaultBatchSize, dispatcher.batchSize)

	dispatcher, err = NewDispatcher(repo, publisher, publisher, nil, nil, WithBatchSize(-5))
	require.NoError(t, err)
	require.Equal(t, defaultBatchSize, dispatcher.batchSize)
}

func TestWithDispatchInterval(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil, WithDispatchInterval(5*time.Second))
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, dispatcher.interval)
}

func TestWithDispatchIntervalIgnoresInvalid(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil, WithDispatchInterval(0))
	require.NoError(t, err)
	require.Equal(t, defaultDispatchInterval, dispatcher.interval)

	dispatcher, err = NewDispatcher(repo, publisher, publisher, nil, nil, WithDispatchInterval(-1*time.Second))
	require.NoError(t, err)
	require.Equal(t, defaultDispatchInterval, dispatcher.interval)
}

// --- B10-H2: handleListPendingError threshold escalation logging ---

func TestDispatcherHandleListPendingErrorThresholdLogging(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := &capturingLogger{}
	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	// threshold=1 so the very first ListPending failure triggers escalation.
	dispatcher, err := NewDispatcher(repo, publisher, publisher, logger, nil)
	require.NoError(t, err)

	dispatcher.listPendingFailureThreshold = 1

	// Set up expectations: all collect stages succeed except ListPending.
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return(nil, errTestBoom)

	dispatcher.DispatchOnce(context.Background())

	// Verify the threshold escalation log entry was emitted.
	entry := logger.findEntry("outbox list pending failures exceeded threshold")
	require.NotNil(t, entry, "threshold escalation log entry should be emitted")
	require.Equal(t, libLog.LevelError, entry.Level, "threshold log should be at error level")

	// Verify structured fields contain "tenant" and "count".
	fieldKeys := make(map[string]bool, len(entry.Fields))
	for _, f := range entry.Fields {
		fieldKeys[f.Key] = true
	}

	require.True(t, fieldKeys["tenant"], "log entry should contain 'tenant' field")
	require.True(t, fieldKeys["count"], "log entry should contain 'count' field")
}

// --- B10-H1: handlePublishError logging when MarkFailed fails ---

func TestDispatcherHandlePublishErrorLogging(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := &capturingLogger{}
	repo := mocks.NewMockOutboxRepository(ctrl)
	publisherErr := errors.New(
		"password=supersecret user@example.com " +
			"Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c " +
			strings.Repeat("x", 320),
	)
	// Publisher returns a retryable error so handlePublishError takes the MarkFailed path.
	publisher := &stubPublisher{err: publisherErr}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, logger, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	payload := ingestionEntities.IngestionFailedEvent{
		EventType: ingestionEntities.EventTypeIngestionFailed,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Error:     "upstream failure",
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionFailed,
		Payload:   body,
	}

	var storedErr string

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	// MarkFailed itself returns an error, which should be logged.
	repo.EXPECT().
		MarkFailed(gomock.Any(), event.ID, gomock.Any(), dispatcher.maxDispatchAttempts).
		DoAndReturn(func(_ context.Context, _ uuid.UUID, errMsg string, _ int) error {
			storedErr = errMsg

			return errors.New("db connection lost")
		})

	dispatcher.DispatchOnce(context.Background())

	// Verify that the "failed to mark outbox failed" log was captured.
	entry := logger.findEntry("failed to mark outbox failed")
	require.NotNil(t, entry, "should log when MarkFailed itself fails")
	require.Equal(t, libLog.LevelError, entry.Level, "MarkFailed failure should be logged at error level")
	require.NotEmpty(t, storedErr)
	require.LessOrEqual(t, len([]rune(storedErr)), maxErrorLength)
	require.Contains(t, storedErr, redactedValue)
	require.NotContains(t, storedErr, "supersecret")
	require.NotContains(t, storedErr, "user@example.com")

	// Verify the error field is present.
	fieldKeys := make(map[string]bool, len(entry.Fields))
	for _, f := range entry.Fields {
		fieldKeys[f.Key] = true
	}

	require.True(t, fieldKeys["error"], "log entry should contain 'error' field with the MarkFailed error")
}

func TestDispatcherHandlePublishErrorLoggingMarkInvalidFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := &capturingLogger{}
	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, logger, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: "unsupported.event.type",
		Payload:   []byte(`{"some":"data"}`),
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)
	repo.EXPECT().
		MarkInvalid(gomock.Any(), event.ID, gomock.Any()).
		Return(errors.New("password=supersecret user@example.com " + strings.Repeat("x", 320)))

	dispatcher.DispatchOnce(context.Background())

	entry := logger.findEntry("failed to mark outbox invalid")
	require.NotNil(t, entry, "should log when MarkInvalid itself fails")
	require.Equal(t, libLog.LevelError, entry.Level)

	errorField, ok := getStringField(entry, "error")
	require.True(t, ok, "error field should be available as string")
	require.LessOrEqual(t, len([]rune(errorField)), maxErrorLength)
	require.Contains(t, errorField, redactedValue)
	require.NotContains(t, errorField, "supersecret")
	require.NotContains(t, errorField, "user@example.com")
}

// --- B10-M4: MarkInvalid path for non-retryable errors ---

func TestDispatcherDispatchOnceMarkInvalidForNonRetryableError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	// An unsupported event type triggers ErrUnsupportedEventType which is in nonRetryableErrors.
	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: "totally.unsupported.event.type",
		Payload:   []byte(`{"some":"data"}`),
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)

	// Key assertion: MarkInvalid should be called (not MarkFailed).
	expectedErr := "publish attempt 1/1 failed: unsupported outbox event type"
	repo.EXPECT().
		MarkInvalid(gomock.Any(), event.ID, expectedErr).
		Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 1, processed)
}

func TestDispatcherDispatchOnceMarkInvalidForBadPayload(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := &capturingLogger{}
	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, logger, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	// Invalid JSON payload triggers ErrInvalidPayload which is non-retryable.
	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   []byte(`{corrupt json`),
	}

	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{event}, nil)

	// ErrInvalidPayload is non-retryable => MarkInvalid, not MarkFailed.
	var storedErr string

	repo.EXPECT().
		MarkInvalid(gomock.Any(), event.ID, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uuid.UUID, errMsg string) error {
			storedErr = errMsg

			return nil
		})

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 1, processed)
	require.NotEmpty(t, storedErr)
	require.LessOrEqual(t, len([]rune(storedErr)), maxErrorLength)
	require.Contains(t, storedErr, "invalid payload format")
}

func TestSanitizeErrorForStorage(t *testing.T) {
	t.Parallel()

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "", sanitizeErrorForStorage(nil))
	})

	t.Run("short error unchanged", func(t *testing.T) {
		t.Parallel()

		msg := "temporary downstream failure"
		require.Equal(t, msg, sanitizeErrorForStorage(errors.New(msg)))
	})

	t.Run("long error is truncated to configured cap", func(t *testing.T) {
		t.Parallel()

		raw := strings.Repeat("x", maxErrorLength+40)
		sanitized := sanitizeErrorForStorage(errors.New(raw))

		require.LessOrEqual(t, len([]rune(sanitized)), maxErrorLength)
		require.Contains(t, sanitized, errorTruncatedSuffix)
	})

	t.Run("sensitive values are redacted", func(t *testing.T) {
		t.Parallel()

		raw := "password=supersecret api_key:abc123 user@example.com 4111111111111111 " +
			"Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
		sanitized := sanitizeErrorForStorage(errors.New(raw))

		require.Contains(t, sanitized, redactedValue)
		require.NotContains(t, sanitized, "supersecret")
		require.NotContains(t, sanitized, "abc123")
		require.NotContains(t, sanitized, "user@example.com")
		require.NotContains(t, sanitized, "4111111111111111")
	})
}

func TestDispatcherPublishEventWithRetryShortCircuitsNonRetryable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 3

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: "unsupported.type",
		Payload:   []byte(`{"k":"v"}`),
	}

	err = dispatcher.publishEventWithRetry(context.Background(), event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "publish attempt 1/3 failed")
	require.ErrorIs(t, err, ErrUnsupportedEventType)
}

func TestDispatcherPublishEventWithRetryContextCanceled(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &selectivePublisher{failIngestionFailed: true}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 3
	dispatcher.publishBackoff = time.Second

	payload := ingestionEntities.IngestionFailedEvent{
		EventType: ingestionEntities.EventTypeIngestionFailed,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Error:     "temporary upstream outage",
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	event := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionFailed,
		Payload:   body,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = dispatcher.publishEventWithRetry(ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "publish attempt 1/3 failed")
	require.Equal(t, 1, publisher.failedAttemptCount)
}

func TestDispatcherFailureCountConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}
	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.listPendingFailureThreshold = 10_000

	const workerCount = 200

	tenantKey := "tenant-concurrent"
	span := trace.SpanFromContext(context.Background())

	// Phase 1: concurrent increments should deterministically sum to workerCount.
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for range workerCount {
		go func() {
			defer wg.Done()

			dispatcher.handleListPendingError(context.Background(), span, tenantKey, errTestBoom)
		}()
	}

	wg.Wait()

	dispatcher.failureCountsMu.Lock()
	countAfterIncrements := dispatcher.listPendingFailureCounts[tenantKey]
	dispatcher.failureCountsMu.Unlock()

	require.Equal(t, workerCount, countAfterIncrements)

	// Phase 2: concurrent clears should leave the map key removed.
	wg.Add(workerCount)

	for range workerCount {
		go func() {
			defer wg.Done()

			dispatcher.clearListPendingFailureCount(tenantKey)
		}()
	}

	wg.Wait()

	dispatcher.failureCountsMu.Lock()
	count, exists := dispatcher.listPendingFailureCounts[tenantKey]
	dispatcher.failureCountsMu.Unlock()

	require.False(t, exists)
	require.Equal(t, 0, count)
}

// --- B10-M5: ListTenants error path ---

func TestDispatcherDispatchAcrossTenantsListTenantsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	dispatcher.interval = 5 * time.Millisecond

	// ListTenants always returns an error. No per-tenant calls should happen.
	// If ListPending, ListPendingByType, ResetForRetry, etc. are called, gomock
	// will fail the test because no expectations are set for them.
	dispatchHappened := make(chan struct{})
	callCount := 0

	repo.EXPECT().
		ListTenants(gomock.Any()).
		DoAndReturn(func(_ context.Context) ([]string, error) {
			callCount++
			if callCount == 1 {
				close(dispatchHappened)
			}

			return nil, errors.New("database unavailable")
		}).
		AnyTimes()

	done := make(chan error, 1)

	go func() {
		done <- dispatcher.Run(nil)
	}()

	// Wait for at least one dispatch cycle to exercise dispatchAcrossTenants.
	select {
	case <-dispatchHappened:
	case <-time.After(500 * time.Millisecond):
		dispatcher.Stop()
		require.FailNow(t, "dispatcher did not attempt ListTenants within timeout")
	}

	dispatcher.Stop()
	require.NoError(t, <-done)

	// The key assertion is that no ListPending/ListPendingByType/etc. calls were made
	// (gomock will fail if unexpected calls occur). The dispatch loop itself continues
	// running without crashing, demonstrating graceful error handling.
}

// --- B10-M6: ListPendingByType error path ---

func TestDispatcherCollectPriorityEventsListPendingByTypeError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := &capturingLogger{}
	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, logger, nil)
	require.NoError(t, err)

	dispatcher.publishMaxAttempts = 1

	// Build a valid pending event that will go through normal ListPending.
	payload := ingestionEntities.IngestionCompletedEvent{
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	pendingEvent := &outboxEntities.OutboxEvent{
		ID:        uuid.New(),
		EventType: ingestionEntities.EventTypeIngestionCompleted,
		Payload:   body,
	}

	// ListPendingByType (for audit priority events) fails.
	repo.EXPECT().
		ListPendingByType(gomock.Any(), sharedDomain.EventTypeAuditLogCreated, defaultPriorityBudget).
		Return(nil, errors.New("priority query failed"))
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), dispatcher.batchSize, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	repo.EXPECT().
		ResetForRetry(gomock.Any(), defaultMaxFailedPerBatch, gomock.Any(), dispatcher.maxDispatchAttempts).
		Return([]*outboxEntities.OutboxEvent{}, nil)
	// Regular ListPending still returns an event -- dispatch should NOT be blocked.
	repo.EXPECT().
		ListPending(gomock.Any(), dispatcher.batchSize).
		Return([]*outboxEntities.OutboxEvent{pendingEvent}, nil)
	repo.EXPECT().
		MarkPublished(gomock.Any(), pendingEvent.ID, gomock.Any()).
		Return(nil)

	processed := dispatcher.DispatchOnce(context.Background())
	require.Equal(t, 1, processed, "regular events should still be dispatched despite priority query failure")
	require.True(t, publisher.completedCalled, "ingestion completed event should be published")

	// Verify that a log entry was captured about the priority events failure.
	entry := logger.findEntry("failed to list priority events")
	require.NotNil(t, entry, "should log when ListPendingByType fails")
}

func TestSanitizeErrorForStorage_Boundaries(t *testing.T) {
	t.Parallel()

	t.Run("short error unchanged", func(t *testing.T) {
		t.Parallel()

		actual := sanitizeErrorForStorage(errors.New("short error"))
		require.Equal(t, "short error", actual)
	})

	t.Run("exactly max length unchanged", func(t *testing.T) {
		t.Parallel()

		exact := strings.Repeat("x", maxErrorLength)
		actual := sanitizeErrorForStorage(errors.New(exact))
		require.Equal(t, exact, actual)
	})

	t.Run("over max length truncated with suffix", func(t *testing.T) {
		t.Parallel()

		actual := sanitizeErrorForStorage(errors.New(strings.Repeat("y", maxErrorLength+1)))
		require.True(t, strings.HasSuffix(actual, "... (truncated)"))

		prefix := strings.TrimSuffix(actual, "... (truncated)")
		expectedPrefixLen := maxErrorLength - len(errorTruncatedSuffix)
		require.Len(t, prefix, expectedPrefixLen)
		require.Equal(t, strings.Repeat("y", expectedPrefixLen), prefix)
	})
}

func TestDispatcherDispatchAcrossTenants_FallbackTracerWhenTrackingMissing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockOutboxRepository(ctrl)
	publisher := &stubPublisher{}

	dispatcher, err := NewDispatcher(repo, publisher, publisher, nil, nil)
	require.NoError(t, err)

	// Simulate a defensive fallback path where dispatcher tracer must be used.
	dispatcher.tracer = nil

	repo.EXPECT().ListTenants(gomock.Any()).Return([]string{}, nil)

	require.NotPanics(t, func() {
		dispatcher.dispatchAcrossTenants(context.Background())
	})
}
