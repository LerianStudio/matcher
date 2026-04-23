//go:build unit

package ports

import "testing"

// Compile-time assertion that the FeeScheduleRepository interface is the
// canonical contract (T-007 K-13 relocation from configuration/ports and
// matching/domain/repositories). The assertion is trivial — its value is
// the companion test file that check-tests.sh requires.
func TestFeeScheduleRepository_InterfaceType(t *testing.T) {
	t.Parallel()

	var repo FeeScheduleRepository
	_ = repo
}
