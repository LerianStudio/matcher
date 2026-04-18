// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestBridgeWorker_ProcessTenant_EligibleExtraction_BridgesEndToEnd is the
// cornerstone test: one tenant, one COMPLETE+unlinked extraction, worker
// processTenant drives it through the orchestrator.
func TestBridgeWorker_ProcessTenant_EligibleExtraction_BridgesEndToEnd(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()
	connID := uuid.New()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, connID)},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 1, count, "one extraction should be processed")

	orch.mu.Lock()
	calls := orch.calls
	orch.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Equal(t, extractionID, calls[0].ExtractionID)
	assert.Equal(t, tenantID, calls[0].TenantID)
}

// TestBridgeWorker_ProcessTenant_IneligibleExtraction_IsSwallowedAsIdempotent
// exercises the orchestrator returning ErrBridgeExtractionIneligible (a
// concurrent worker won the race). The worker treats this as idempotent
// success and counts the extraction as "processed" for reporting purposes.
func TestBridgeWorker_ProcessTenant_IneligibleExtraction_IsSwallowedAsIdempotent(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrBridgeExtractionIneligible
		},
	}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, uuid.New())},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	// Ineligible is idempotent success: processed=1 (no error surfaced).
	assert.Equal(t, 1, count)
}

// TestBridgeWorker_ProcessTenant_AlreadyLinked_IsSwallowedAsIdempotent
// mirrors the ineligible case but via ErrExtractionAlreadyLinked, which
// occurs when a concurrent worker wrote the link between our eligible-find
// and our orchestrator call.
func TestBridgeWorker_ProcessTenant_AlreadyLinked_IsSwallowedAsIdempotent(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrExtractionAlreadyLinked
		},
	}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, uuid.New())},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 1, count)
}

// TestBridgeWorker_ProcessTenant_TransientError_IsCountedNegatively
// verifies that terminal orchestrator failures (neither idempotent signal)
// are NOT counted as processed. The extraction stays unlinked and the next
// cycle will re-attempt it.
func TestBridgeWorker_ProcessTenant_TransientError_IsCountedNegatively(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, errors.New("transient retrieval boom")
		},
	}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, uuid.New())},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 0, count, "transient failure should not count as processed")
}

// TestBridgeWorker_ProcessTenant_EmptyTenantBatch_ProcessesZero ensures a
// tenant with no eligible extractions is cheap — the worker returns 0 and
// does not invoke the orchestrator at all.
func TestBridgeWorker_ProcessTenant_EmptyTenantBatch_ProcessesZero(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 0, count)
	assert.Equal(t, int64(0), orch.callsCount.Load())
}

// TestBridgeWorker_ProcessTenant_RepoError_LogsAndReturnsZero ensures a
// FindEligibleForBridge failure does not crash the worker — the cycle
// moves on to the next tenant.
func TestBridgeWorker_ProcessTenant_RepoError_LogsAndReturnsZero(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleErr: errors.New("db down"),
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 0, count)
	assert.Equal(t, int64(0), orch.callsCount.Load())
}

// TestBridgeWorker_ProcessTenant_MultipleExtractions_AllProcessed verifies
// the batch loop iterates through every eligible extraction.
func TestBridgeWorker_ProcessTenant_MultipleExtractions_AllProcessed(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	connID := uuid.New()

	extractions := []*entities.ExtractionRequest{
		completeExtraction(uuid.New(), connID),
		completeExtraction(uuid.New(), connID),
		completeExtraction(uuid.New(), connID),
	}

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: extractions,
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 3, count)
	assert.Equal(t, int64(3), orch.callsCount.Load())
}
