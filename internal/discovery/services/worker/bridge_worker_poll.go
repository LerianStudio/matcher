// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// pollCycle acquires the distributed lock, lists tenants (INCLUDING the
// default tenant), and drives each tenant's eligible extractions through
// the orchestrator.
//
// Heartbeat write happens on every exit path (lock denied, Redis error,
// empty tenant list, full cycle) because every path still constitutes a
// tick — the worker is alive. The dashboard's "worker healthy" signal
// then reflects replica liveness rather than "this specific replica won
// the lock this cycle." C15.
func (worker *BridgeWorker) pollCycle(ctx context.Context) {
	logger, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.bridge.poll_cycle")
	defer span.End()

	// Deferred in LIFO order: heartbeat runs BEFORE span.End, giving the
	// span a chance to record heartbeat-related attributes if needed.
	defer worker.writeHeartbeat(ctx)

	acquired, token, err := worker.acquireLock(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bridge lock acquire failed", err)
		logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: lock acquire failed")

		return
	}

	if !acquired {
		return
	}

	defer worker.releaseLock(ctx, token)

	tenants, err := worker.tenantLister.ListTenants(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bridge: list tenants failed", err)
		logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelError, "bridge: failed to list tenants")

		return
	}

	span.SetAttributes(attribute.Int("bridge.tenant_count", len(tenants)))

	processed := 0

	for _, tenantID := range tenants {
		if tenantID == "" {
			continue
		}

		count := worker.processTenant(ctx, tenantID)
		processed += count
	}

	span.SetAttributes(attribute.Int("bridge.extractions_processed", processed))
}

// processTenant drives bridge work for a single tenant. Returns the number
// of extractions that completed the pipeline (successfully or with a
// terminal idempotent signal) so the cycle-level span can report totals.
func (worker *BridgeWorker) processTenant(parentCtx context.Context, tenantID string) int {
	ctx := context.WithValue(parentCtx, auth.TenantIDKey, tenantID)
	logger, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.bridge.process_tenant")
	defer span.End()

	span.SetAttributes(attribute.String("tenant.id", tenantID))

	batchSize := worker.cfg.BatchSize

	extractions, err := worker.extractionRepo.FindEligibleForBridge(ctx, batchSize)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bridge: find eligible extractions failed", err)
		logger.With(
			libLog.String("tenant.id", tenantID),
			libLog.String("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "bridge: failed to find eligible extractions")

		return 0
	}

	span.SetAttributes(attribute.Int("bridge.eligible_count", len(extractions)))

	processed := 0

	for _, extraction := range extractions {
		if extraction == nil {
			continue
		}

		if err := worker.bridgeOne(ctx, extraction, tenantID); err != nil {
			worker.logBridgeError(ctx, logger, extraction.ID, tenantID, err)

			continue
		}

		processed++
	}

	return processed
}

// bridgeOne runs a single extraction through the orchestrator. Wraps each
// call in its own span so operators can see per-extraction timing even when
// the tenant batch is large.
//
// T-005 retry semantics:
//  1. Idempotent signals (already-linked, ineligible) → silent success.
//  2. Terminal classifications (integrity / 404) → persist
//     MarkBridgeFailed; the row exits the eligibility queue.
//  3. Transient classifications (custody / network / source-unresolvable) →
//     increment bridge_attempts; if attempts ≥ max, escalate to terminal.
//
// Backoff strategy: PASSIVE — the worker does NOT sleep between retries and
// has no exponential-backoff math. Backoff is enforced by
// FindEligibleForBridge ordering by `updated_at ASC`: every attempt bumps
// the row's updated_at, pushing it to the tail of the eligibility queue so
// newer rows drain first. The tick cadence (BridgeWorkerConfig.Interval)
// IS the retry cadence; MaxAttempts caps total retries before terminal
// escalation. This is simpler, race-free, and avoids the dual-clock confusion
// of an in-process backoff timer racing the DB queue ordering.
func (worker *BridgeWorker) bridgeOne(ctx context.Context, extraction *entities.ExtractionRequest, tenantID string) error {
	if extraction == nil {
		return nil
	}

	_, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.bridge.bridge_one")
	defer span.End()

	span.SetAttributes(
		attribute.String("extraction.id", extraction.ID.String()),
		attribute.String("tenant.id", tenantID),
		attribute.Int("bridge.attempts_before", extraction.BridgeAttempts),
	)

	outcome, bridgeErr := worker.orchestrator.BridgeExtraction(ctx, sharedPorts.BridgeExtractionInput{
		ExtractionID: extraction.ID,
		TenantID:     tenantID,
	})

	classification := ClassifyBridgeError(bridgeErr)
	span.SetAttributes(attribute.String("bridge.retry_policy", classification.Policy.String()))

	// Wrap once so callers get a traceable error chain while retaining the
	// original sentinels for errors.Is checks. Nil stays nil.
	var wrappedErr error
	if bridgeErr != nil {
		wrappedErr = fmt.Errorf("bridge extraction: %w", bridgeErr)
	}

	switch classification.Policy {
	case RetryIdempotent:
		// Either no error (happy path) or a benign concurrent-write signal.
		if outcome != nil {
			span.SetAttributes(
				attribute.String("ingestion.job_id", outcome.IngestionJobID.String()),
				attribute.Int("ingestion.transaction_count", outcome.TransactionCount),
				attribute.Bool("bridge.custody_deleted", outcome.CustodyDeleted),
			)
		}

		return nil

	case RetryTerminal:
		worker.persistTerminalFailure(ctx, extraction, classification.Class, bridgeErr)

		return wrappedErr

	case RetryTransient:
		worker.handleTransientFailure(ctx, extraction, bridgeErr)

		return wrappedErr
	}

	return wrappedErr
}
