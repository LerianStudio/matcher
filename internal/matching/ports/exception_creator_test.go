//go:build unit

package ports_test

import (
	"testing"

	"github.com/LerianStudio/matcher/internal/matching/adapters/postgres/exception_creator"
	"github.com/LerianStudio/matcher/internal/matching/ports"
)

func TestExceptionCreator_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	// Verify the postgres implementation satisfies the interface
	var _ ports.ExceptionCreator = (*exception_creator.Repository)(nil)
}
