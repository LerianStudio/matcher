//go:build unit

package ports

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

func TestEventPublisherInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ EventPublisher = (*mockEventPublisher)(nil)
}

type mockEventPublisher struct{}

func (m *mockEventPublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *entities.IngestionCompletedEvent,
) error {
	return nil
}

func (m *mockEventPublisher) PublishIngestionFailed(
	_ context.Context,
	_ *entities.IngestionFailedEvent,
) error {
	return nil
}

func TestEventPublisherMockImplementation(t *testing.T) {
	t.Parallel()

	t.Run("PublishIngestionCompleted returns nil", func(t *testing.T) {
		t.Parallel()

		publisher := &mockEventPublisher{}
		ctx := t.Context()

		err := publisher.PublishIngestionCompleted(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("PublishIngestionFailed returns nil", func(t *testing.T) {
		t.Parallel()

		publisher := &mockEventPublisher{}
		ctx := t.Context()

		err := publisher.PublishIngestionFailed(ctx, nil)
		assert.NoError(t, err)
	})
}
