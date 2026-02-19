//go:build unit

package repositories_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	"github.com/LerianStudio/matcher/internal/outbox/domain/repositories"
	repositoriesmocks "github.com/LerianStudio/matcher/internal/outbox/domain/repositories/mocks"
)

func TestOutboxRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ repositories.OutboxRepository = repositoriesmocks.NewMockOutboxRepository(ctrl)
}

func TestMockOutboxRepositoryCreate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := repositoriesmocks.NewMockOutboxRepository(ctrl)
	eventID := uuid.New()
	event := &entities.OutboxEvent{
		ID:        eventID,
		EventType: "test.event",
		Status:    entities.OutboxStatusPending,
	}

	repo.EXPECT().Create(gomock.Any(), event).Return(event, nil)

	created, err := repo.Create(context.Background(), event)

	require.NoError(t, err)
	require.Equal(t, eventID, created.ID)
}

func TestMockOutboxRepositoryGetByID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := repositoriesmocks.NewMockOutboxRepository(ctrl)
	eventID := uuid.New()
	event := &entities.OutboxEvent{
		ID:        eventID,
		EventType: "test.event",
		Status:    entities.OutboxStatusPending,
	}

	t.Run("returns event when found", func(t *testing.T) {
		t.Parallel()

		repo.EXPECT().GetByID(gomock.Any(), eventID).Return(event, nil)

		result, err := repo.GetByID(context.Background(), eventID)
		require.NoError(t, err)
		require.Equal(t, eventID, result.ID)
	})

	t.Run("returns error when not found", func(t *testing.T) {
		t.Parallel()

		missingID := uuid.New()
		missingErr := errors.New("not found")
		repo.EXPECT().GetByID(gomock.Any(), missingID).Return(nil, missingErr)

		_, err := repo.GetByID(context.Background(), missingID)
		require.ErrorIs(t, err, missingErr)
	})
}

func TestMockOutboxRepositoryListPending(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := repositoriesmocks.NewMockOutboxRepository(ctrl)
	limit := 10
	results := []*entities.OutboxEvent{
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
		{ID: uuid.New(), Status: entities.OutboxStatusPending},
	}

	repo.EXPECT().ListPending(gomock.Any(), limit).Return(results, nil)

	pending, err := repo.ListPending(context.Background(), limit)
	require.NoError(t, err)
	require.Len(t, pending, 2)
}

func TestMockOutboxRepositoryMarkPublished(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := repositoriesmocks.NewMockOutboxRepository(ctrl)
	eventID := uuid.New()
	publishedAt := time.Now().UTC()

	repo.EXPECT().MarkPublished(gomock.Any(), eventID, publishedAt).Return(nil)

	err := repo.MarkPublished(context.Background(), eventID, publishedAt)
	require.NoError(t, err)
}

func TestMockOutboxRepositoryMarkFailed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := repositoriesmocks.NewMockOutboxRepository(ctrl)
	eventID := uuid.New()
	errMsg := "publish error"

	maxAttempts := 10

	repo.EXPECT().MarkFailed(gomock.Any(), eventID, errMsg, maxAttempts).Return(nil)

	err := repo.MarkFailed(context.Background(), eventID, errMsg, maxAttempts)
	require.NoError(t, err)
}
