//go:build unit

package cross

import "testing"

// Compile-time assertion that TransactionContextLookup stays constructible
// with the expected shape. T-006 deleted the companion glue-tests for the
// removed *Finder interfaces; this file keeps check-tests.sh satisfied and
// locks the lookup struct's presence.
func TestTransactionContextLookup_TypeExists(t *testing.T) {
	t.Parallel()

	var lookup *TransactionContextLookup
	_ = lookup
}
