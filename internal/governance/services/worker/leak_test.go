//go:build unit && leak

// REFACTOR-010: Goroutine-leak coverage for governance archival worker.
//
// archival_worker spawns:
//   - The main archival loop (line ~228) via SafeGoWithContextAndComponent.
//   - A pipe producer per partition upload (line ~782) that feeds
//     compressed rows to an S3-compatible uploader. If ctx is cancelled
//     mid-upload the consumer must close the pipe so the producer
//     unblocks; otherwise the producer goroutine leaks.
//
// This is the highest-leak-risk site in the codebase — pipe-based
// producer/consumer handoffs are classic leak surfaces.
package worker

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions()...)
}
