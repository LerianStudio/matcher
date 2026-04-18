//go:build unit

package ports

import (
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

func TestOutboxRepositoryInterfaceAlias(t *testing.T) {
	t.Parallel()

	// Verify the type alias resolves to the canonical interface.
	var _ OutboxRepository = (outbox.OutboxRepository)(nil)
}
