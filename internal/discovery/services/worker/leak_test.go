//go:build unit && leak

// REFACTOR-009: Goroutine-leak coverage for discovery workers.
//
// The discovery worker package spawns long-lived goroutines for:
//   - discovery_worker (tenant enumeration + periodic poll)
//   - bridge_worker (fetcher→ingestion handoff)
//   - custody_retention_worker (archival sweep)
//   - extraction_poller (per-extraction detached goroutine; highest-risk
//     leak surface)
//
// Each worker exposes Start(ctx)/Stop() or ctx-cancel semantics. TestMain
// installs goleak.VerifyTestMain so any goroutine that outlives the test
// binary is surfaced.
package worker

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions()...)
}
