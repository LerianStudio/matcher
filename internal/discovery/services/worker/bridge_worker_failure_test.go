// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// newTestWorker constructs a worker with stub dependencies suitable for the
// failure-classification tests. The lifecycle is irrelevant for these tests
// — we drive bridgeOne directly.
func newTestWorker(orch sharedPorts.BridgeOrchestrator, repo *stubBridgeExtractionRepo, retry BridgeRetryBackoff) *BridgeWorker {
	return &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{},
		infraProvider:  &stubInfraProvider{},
		cfg: BridgeWorkerConfig{
			Interval:  time.Second,
			BatchSize: 10,
			Retry:     retry.Normalize(),
		},
		logger: &stubLogger{},
		tracer: otel.Tracer("test.bridge"),
	}
}

// TestBridgeWorker_TerminalFailure_PersistsBridgeErrorAndExitsQueue is the
// cornerstone test for T-005. A 404 from Fetcher → terminal class persisted
// → row no longer eligible.
func TestBridgeWorker_TerminalFailure_PersistsBridgeErrorAndExitsQueue(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()
	connID := uuid.New()
	extraction := completeExtraction(extractionID, connID)

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, fmt.Errorf("retrieve: %w", sharedPorts.ErrFetcherResourceNotFound)
		},
	}
	repo := &stubBridgeExtractionRepo{}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	err := w.bridgeOne(context.Background(), extraction, tenantID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, sharedPorts.ErrFetcherResourceNotFound))

	// Domain entity has been mutated.
	assert.Equal(t, vo.BridgeErrorClassArtifactNotFound, extraction.BridgeLastError)
	assert.Equal(t, 1, extraction.BridgeAttempts)
	assert.False(t, extraction.BridgeFailedAt.IsZero())
	assert.True(t, extraction.HasTerminalBridgeFailure())

	// Repository call shape: MarkBridgeFailed was invoked with the row.
	require.Len(t, repo.markedFailures, 1)
	assert.Equal(t, extractionID, repo.markedFailures[0].ID)
	assert.Equal(t, vo.BridgeErrorClassArtifactNotFound, repo.markedFailures[0].BridgeLastError)
}

// TestBridgeWorker_TerminalFailure_IntegrityFailed_PersistsCorrectClass.
func TestBridgeWorker_TerminalFailure_IntegrityFailed_PersistsCorrectClass(t *testing.T) {
	t.Parallel()

	extraction := completeExtraction(uuid.New(), uuid.New())
	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrIntegrityVerificationFailed
		},
	}
	repo := &stubBridgeExtractionRepo{}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	err := w.bridgeOne(context.Background(), extraction, "tenant-x")
	require.Error(t, err)
	assert.Equal(t, vo.BridgeErrorClassIntegrityFailed, extraction.BridgeLastError)
	require.Len(t, repo.markedFailures, 1)
}

// TestBridgeWorker_TransientFailure_BumpAttemptsBelowCeiling_NotTerminal.
func TestBridgeWorker_TransientFailure_BumpAttemptsBelowCeiling_NotTerminal(t *testing.T) {
	t.Parallel()

	extraction := completeExtraction(uuid.New(), uuid.New())
	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrCustodyStoreFailed
		},
	}
	repo := &stubBridgeExtractionRepo{}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	err := w.bridgeOne(context.Background(), extraction, "tenant-y")
	require.Error(t, err)

	// Below ceiling: NOT terminal — empty class, no bridge_failed_at.
	assert.Empty(t, extraction.BridgeLastError)
	assert.True(t, extraction.BridgeFailedAt.IsZero())
	assert.Equal(t, 1, extraction.BridgeAttempts)

	// MarkBridgeFailed not called; IncrementBridgeAttempts was called to
	// persist the bumped counter via the narrow UPDATE (Polish Fix 3).
	assert.Empty(t, repo.markedFailures)
	assert.Empty(t, repo.updatedExtractions, "wide Update path no longer used for transient retries")
	require.Len(t, repo.incrementCalls, 1)
	assert.Equal(t, 1, repo.incrementCalls[0].Attempts)
	assert.Equal(t, extraction.ID, repo.incrementCalls[0].ID)
}

// TestBridgeWorker_TransientFailure_HitsMaxAttempts_EscalatesToTerminal.
func TestBridgeWorker_TransientFailure_HitsMaxAttempts_EscalatesToTerminal(t *testing.T) {
	t.Parallel()

	extraction := completeExtraction(uuid.New(), uuid.New())
	// Pre-set attempts so the bump pushes us to ceiling.
	extraction.BridgeAttempts = 4

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrCustodyStoreFailed
		},
	}
	repo := &stubBridgeExtractionRepo{}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	err := w.bridgeOne(context.Background(), extraction, "tenant-z")
	require.Error(t, err)

	// Hit the ceiling: escalated to terminal max_attempts_exceeded.
	assert.Equal(t, vo.BridgeErrorClassMaxAttemptsExceeded, extraction.BridgeLastError)
	assert.Equal(t, 5, extraction.BridgeAttempts)
	assert.False(t, extraction.BridgeFailedAt.IsZero())
	require.Len(t, repo.markedFailures, 1)
}

// TestBridgeWorker_TransientFailure_SourceUnresolvable_EscalatesToSourceUnresolved.
func TestBridgeWorker_TransientFailure_SourceUnresolvable_EscalatesToSourceUnresolved(t *testing.T) {
	t.Parallel()

	extraction := completeExtraction(uuid.New(), uuid.New())
	extraction.BridgeAttempts = 4

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrBridgeSourceUnresolvable
		},
	}
	repo := &stubBridgeExtractionRepo{}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	err := w.bridgeOne(context.Background(), extraction, "tenant-q")
	require.Error(t, err)

	// EscalateAfterMaxAttempts maps source-unresolvable → source_unresolved.
	assert.Equal(t, vo.BridgeErrorClassSourceUnresolved, extraction.BridgeLastError)
	require.Len(t, repo.markedFailures, 1)
}

// TestBridgeWorker_IdempotentSignal_NoFailureRecorded.
func TestBridgeWorker_IdempotentSignal_NoFailureRecorded(t *testing.T) {
	t.Parallel()

	extraction := completeExtraction(uuid.New(), uuid.New())
	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrBridgeExtractionIneligible
		},
	}
	repo := &stubBridgeExtractionRepo{}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	err := w.bridgeOne(context.Background(), extraction, "tenant-i")
	require.NoError(t, err) // swallowed
	assert.Empty(t, extraction.BridgeLastError)
	assert.Equal(t, 0, extraction.BridgeAttempts)
	assert.Empty(t, repo.markedFailures)
	assert.Empty(t, repo.updatedExtractions)
	assert.Empty(t, repo.incrementCalls)
}

// TestBridgeWorker_NilExtraction_SilentNoOp guards the defensive nil path.
func TestBridgeWorker_NilExtraction_SilentNoOp(t *testing.T) {
	t.Parallel()

	w := newTestWorker(&stubBridgeOrchestrator{}, &stubBridgeExtractionRepo{}, BridgeRetryBackoff{})
	require.NoError(t, w.bridgeOne(context.Background(), nil, "tenant"))
}

// TestBridgeWorker_TransientFailure_ConcurrentLink_LogsAndStops is the
// Polish Fix 3 regression: when a concurrent LinkIfUnlinked wins the race
// between FindEligibleForBridge and the worker's transient retry persist,
// IncrementBridgeAttempts surfaces ErrExtractionAlreadyLinked. The worker
// must NOT bubble this as a wide-update failure — the link is the desired
// outcome, so we log info and stop.
func TestBridgeWorker_TransientFailure_ConcurrentLink_LogsAndStops(t *testing.T) {
	t.Parallel()

	extraction := completeExtraction(uuid.New(), uuid.New())
	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrCustodyStoreFailed
		},
	}
	repo := &stubBridgeExtractionRepo{
		incrementFn: func(_ uuid.UUID, _ int) error {
			return sharedPorts.ErrExtractionAlreadyLinked
		},
	}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	// bridgeOne returns the original transient error (the persist path's
	// ErrExtractionAlreadyLinked is swallowed inside handleTransientFailure
	// because the link itself is the outcome we wanted).
	err := w.bridgeOne(context.Background(), extraction, "tenant-race")
	require.Error(t, err)
	assert.True(t, errors.Is(err, sharedPorts.ErrCustodyStoreFailed))
}

// TestBridgeWorker_TerminalFailure_ConcurrentLink_BenignSkip is the C21
// regression counterpart for the terminal-failure path: when a concurrent
// LinkIfUnlinked wins the race between FindEligibleForBridge and
// persistTerminalFailure, MarkBridgeFailed's narrow UPDATE misses the row
// (the NULL-guard rejects the write) and the repository surfaces
// ErrExtractionAlreadyLinked. persistTerminalFailure must treat this as
// benign — the link is the authoritative outcome, so the terminal-failure
// write is correctly skipped. The original terminal-classified error from
// the orchestrator still bubbles up through bridgeOne unchanged.
func TestBridgeWorker_TerminalFailure_ConcurrentLink_BenignSkip(t *testing.T) {
	t.Parallel()

	extraction := completeExtraction(uuid.New(), uuid.New())
	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, fmt.Errorf("retrieve: %w", sharedPorts.ErrFetcherResourceNotFound)
		},
	}
	repo := &stubBridgeExtractionRepo{
		markBridgeFailedFn: func(_ *entities.ExtractionRequest) error {
			return sharedPorts.ErrExtractionAlreadyLinked
		},
	}
	w := newTestWorker(orch, repo, BridgeRetryBackoff{MaxAttempts: 5})

	err := w.bridgeOne(context.Background(), extraction, "tenant-terminal-race")
	require.Error(t, err)
	// The original terminal error still bubbles — only the persist of the
	// terminal-failure row is skipped benignly.
	assert.True(t, errors.Is(err, sharedPorts.ErrFetcherResourceNotFound))
	// Entity was mutated (domain-level MarkBridgeFailed ran) and the repo
	// MarkBridgeFailed was attempted — but the benign sentinel means the
	// write was rejected and treated as a no-op. The entity still reflects
	// the terminal classification locally.
	assert.Equal(t, vo.BridgeErrorClassArtifactNotFound, extraction.BridgeLastError)
	assert.Equal(t, 1, extraction.BridgeAttempts)
	assert.True(t, extraction.HasTerminalBridgeFailure())
}
