//go:build unit && leak

// REFACTOR-012: Goroutine-leak coverage for configuration workers.
//
// Covers SchedulerWorker (scheduler_worker.go:179). TestMain installs
// goleak.VerifyTestMain so any goroutine spawned by Start() that
// survives Stop() is surfaced. This also validates REFACTOR-015's
// defer-order fix for the scheduler worker.
package worker

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions()...)
}
