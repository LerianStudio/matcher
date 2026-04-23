//go:build unit && leak

// REFACTOR-009: extraction_poller cancel-during-extraction leak test.
//
// PollUntilComplete spawns a detached goroutine per extraction ID.
// This test drives the highest-risk path: a long-poll where the parent
// context is cancelled while the poller is mid-interval, and asserts
// the spawned goroutine terminates.
package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestExtractionPoller_CancelDuringExtraction_NoLeak(t *testing.T) {
	defer goleak.VerifyNone(t, testutil.LeakOptions()...)

	var pollCount atomic.Int32

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			pollCount.Add(1)
			// Always return RUNNING so the poller loops until ctx is cancelled.
			return &sharedPorts.ExtractionJobStatus{Status: "RUNNING"}, nil
		},
	}

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-running",
		Status:       vo.ExtractionStatusSubmitted,
	}
	repo := &stubExtractionRepo{entity: extraction}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{
			PollInterval: 5 * time.Millisecond,
			Timeout:      5 * time.Second, // well beyond the test window
		},
		&stubLogger{},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	p.PollUntilComplete(
		ctx,
		extraction.ID,
		func(_ context.Context, _ string) error { return nil },
		func(_ context.Context, _ string) {},
	)

	// Let the poller iterate at least once so the detached goroutine is
	// definitely running.
	require.Eventually(t, func() bool {
		return pollCount.Load() >= 1
	}, time.Second, 2*time.Millisecond, "poller should have made at least one call")

	// Cancel the context mid-poll and give the detached goroutine a
	// moment to observe the cancellation before VerifyNone snapshots.
	cancel()

	// goleak.VerifyNone runs via defer after this function returns;
	// give the SafeGo wrapper a beat to unwind its recover/log defers.
	time.Sleep(50 * time.Millisecond)
}
