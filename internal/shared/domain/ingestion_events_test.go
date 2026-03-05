//go:build unit

package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIngestionEventTypeConstants(t *testing.T) {
	t.Parallel()

	t.Run("EventTypeIngestionCompleted has correct value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "ingestion.completed", EventTypeIngestionCompleted)
	})

	t.Run("EventTypeIngestionFailed has correct value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "ingestion.failed", EventTypeIngestionFailed)
	})

	t.Run("event type constants are non-empty", func(t *testing.T) {
		t.Parallel()

		assert.NotEmpty(t, EventTypeIngestionCompleted)
		assert.NotEmpty(t, EventTypeIngestionFailed)
	})

	t.Run("event type constants are distinct", func(t *testing.T) {
		t.Parallel()

		assert.NotEqual(t, EventTypeIngestionCompleted, EventTypeIngestionFailed)
	})
}
