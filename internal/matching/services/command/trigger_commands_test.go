//go:build unit

package command

import (
	"testing"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time assertion: the matching UseCase satisfies the MatchTrigger
// port. T-004 absorbed the former MatchTriggerAdapter; this test locks
// the signature so the adapter cannot quietly return.
func TestUseCase_ImplementsMatchTrigger(t *testing.T) {
	t.Parallel()

	var _ sharedPorts.MatchTrigger = (*UseCase)(nil)
}
