//go:build unit

package ports

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActorExtractor_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ ActorExtractor = (*mockActorExtractor)(nil)
}

type mockActorExtractor struct {
	actor string
}

func (m *mockActorExtractor) GetActor(_ context.Context) string {
	return m.actor
}

func TestActorExtractor_MockImplementation(t *testing.T) {
	t.Parallel()

	t.Run("returns actor when present", func(t *testing.T) {
		t.Parallel()

		extractor := &mockActorExtractor{actor: "user-123"}
		ctx := t.Context()

		result := extractor.GetActor(ctx)

		assert.Equal(t, "user-123", result)
	})

	t.Run("returns empty string when no actor", func(t *testing.T) {
		t.Parallel()

		extractor := &mockActorExtractor{actor: ""}
		ctx := t.Context()

		result := extractor.GetActor(ctx)

		assert.Empty(t, result)
	})
}
