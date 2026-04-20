// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// Sentinel errors for bridge readiness queries.
var (
	// ErrInvalidReadinessState is returned when the caller asks for a state
	// the value-object package does not recognise.
	ErrInvalidReadinessState = errors.New("invalid bridge readiness state")
	// ErrReadinessLimitInvalid is returned when the caller passes a negative
	// page size. Zero is allowed (the adapter will fall back to its default).
	ErrReadinessLimitInvalid = errors.New("bridge readiness limit must be non-negative")
	// ErrReadinessThresholdInvalid is returned when the caller passes a
	// negative stale threshold. Zero is allowed; the adapter clamps it to a
	// 1-second floor so the partition stays meaningful.
	ErrReadinessThresholdInvalid = errors.New("bridge readiness threshold must be non-negative")
	// ErrNilBridgeReadinessUseCase is returned when a bridge readiness method
	// is invoked on a nil receiver. Distinct from ErrNilExtractionRepository
	// which signals a constructor wiring problem.
	ErrNilBridgeReadinessUseCase = errors.New("bridge readiness use case is required")
)

// BridgeReadinessSummary captures the five-way partition of extractions plus
// the threshold that produced it. Counts.InFlightCount surfaces upstream
// extractions still running so callers can see a complete picture. The
// threshold is echoed back so dashboards can render "stale after Nm" labels
// without needing to re-read config.
//
// Worker liveness fields (C15):
//   - WorkerLastTickAt is the most recent cycle timestamp written by any
//     bridge worker replica; nil means no heartbeat has been observed yet
//     (fresh deploy, Fetcher disabled, or all replicas dead long enough for
//     the Redis key to expire).
//   - WorkerStalenessSeconds is (now - WorkerLastTickAt) in seconds; nil
//     when WorkerLastTickAt is nil so the dashboard renders "unknown"
//     instead of a misleading zero.
//   - WorkerHealthy is true when staleness is below the configured
//     threshold. False when the worker is absent or stalled. Populating the
//     derived field server-side lets every dashboard render the same verdict
//     without re-implementing the threshold math.
type BridgeReadinessSummary struct {
	Counts                 repositories.BridgeReadinessCounts
	StaleThreshold         time.Duration
	GeneratedAt            time.Time
	WorkerLastTickAt       *time.Time
	WorkerStalenessSeconds *int64
	WorkerHealthy          bool
}

// BridgeCandidate decorates an extraction entity with its derived readiness
// state so the caller doesn't have to recompute it. The state is computed
// from the same staleThreshold passed to ListBridgeCandidates.
type BridgeCandidate struct {
	Extraction     *entities.ExtractionRequest
	ReadinessState vo.BridgeReadinessState
	AgeSeconds     int64
}

// CountBridgeReadinessByTenant returns aggregate readiness counts for the
// tenant resolved from ctx. staleThreshold partitions COMPLETE+unlinked
// extractions into pending vs stale.
//
// Negative thresholds are rejected; zero is permitted and clamped to a 1s
// floor by the adapter so the partition stays meaningful.
func (uc *UseCase) CountBridgeReadinessByTenant(
	ctx context.Context,
	staleThreshold time.Duration,
) (*BridgeReadinessSummary, error) {
	if uc == nil {
		return nil, ErrNilBridgeReadinessUseCase
	}

	if staleThreshold < 0 {
		return nil, fmt.Errorf("%w: %s", ErrReadinessThresholdInvalid, staleThreshold)
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "query.discovery.count_bridge_readiness")
	defer span.End()

	counts, err := uc.extractionRepo.CountBridgeReadiness(ctx, staleThreshold)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "count bridge readiness", err)

		return nil, fmt.Errorf("count bridge readiness: %w", err)
	}

	generatedAt := time.Now().UTC()
	summary := &BridgeReadinessSummary{
		Counts:         counts,
		StaleThreshold: staleThreshold,
		GeneratedAt:    generatedAt,
	}

	uc.decorateWorkerHeartbeat(ctx, span, summary, generatedAt)

	return summary, nil
}

// decorateWorkerHeartbeat populates the WorkerLastTickAt / WorkerStalenessSeconds
// / WorkerHealthy fields on the summary using the optional heartbeat
// reader. A missing reader (Fetcher disabled, wiring not completed) leaves
// every heartbeat field at its zero value so the dashboard renders
// "unknown" — which is the truthful state when there is nothing to report.
//
// Errors from the reader are logged via the span but never propagated: the
// readiness summary is the primary payload and degrading the adjacent
// liveness metadata to nil is strictly better than 500-ing the whole
// dashboard. C15.
func (uc *UseCase) decorateWorkerHeartbeat(
	ctx context.Context,
	span trace.Span,
	summary *BridgeReadinessSummary,
	now time.Time,
) {
	if uc == nil || summary == nil || uc.heartbeatReader == nil {
		return
	}

	lastTickAt, err := uc.heartbeatReader.ReadLastTickAt(ctx)
	if err != nil {
		// Non-fatal: dashboard gets "unknown" liveness and a warn log.
		libOpentelemetry.HandleSpanError(span, "read bridge heartbeat", err)

		return
	}

	if lastTickAt.IsZero() {
		// No heartbeat observed. Leave WorkerHealthy false; callers use the
		// nil timestamp to distinguish "never ticked" from "stale".
		return
	}

	// Always report in UTC so clients don't have to normalize.
	ticked := lastTickAt.UTC()
	summary.WorkerLastTickAt = &ticked

	staleness := now.Sub(ticked)
	if staleness < 0 {
		staleness = 0
	}

	stalenessSec := int64(staleness.Seconds())
	summary.WorkerStalenessSeconds = &stalenessSec

	// Healthy when staleness is under the configured threshold. A zero /
	// negative staleAt disables the derived flag: callers will see the raw
	// timestamp + seconds and decide for themselves.
	if uc.heartbeatStaleAt > 0 {
		summary.WorkerHealthy = staleness <= uc.heartbeatStaleAt
	}
}

// ListBridgeCandidates returns extractions in the requested readiness state
// with cursor pagination.
//
// state is parsed and validated up front so callers receive
// ErrInvalidReadinessState before any I/O is issued. limit is forwarded to
// the adapter, which applies its own clamp.
func (uc *UseCase) ListBridgeCandidates(
	ctx context.Context,
	state string,
	staleThreshold time.Duration,
	cursorCreatedAt time.Time,
	cursorID uuid.UUID,
	limit int,
) ([]BridgeCandidate, error) {
	if uc == nil {
		return nil, ErrNilBridgeReadinessUseCase
	}

	if limit < 0 {
		return nil, fmt.Errorf("%w: %d", ErrReadinessLimitInvalid, limit)
	}

	if staleThreshold < 0 {
		return nil, fmt.Errorf("%w: %s", ErrReadinessThresholdInvalid, staleThreshold)
	}

	parsedState, err := vo.ParseBridgeReadinessState(state)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidReadinessState, state)
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "query.discovery.list_bridge_candidates")
	defer span.End()

	rows, err := uc.extractionRepo.ListBridgeCandidates(
		ctx,
		string(parsedState),
		staleThreshold,
		cursorCreatedAt,
		cursorID,
		limit,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list bridge candidates", err)

		return nil, fmt.Errorf("list bridge candidates: %w", err)
	}

	now := time.Now().UTC()
	candidates := make([]BridgeCandidate, 0, len(rows))

	for _, row := range rows {
		if row == nil {
			continue
		}

		ageSec := int64(now.Sub(row.CreatedAt).Seconds())
		if ageSec < 0 {
			ageSec = 0
		}

		candidates = append(candidates, BridgeCandidate{
			Extraction:     row,
			ReadinessState: parsedState,
			AgeSeconds:     ageSec,
		})
	}

	return candidates, nil
}
