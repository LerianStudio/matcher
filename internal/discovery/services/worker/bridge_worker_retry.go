// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// persistTerminalFailure records the BridgeErrorClass on the extraction and
// removes it from the eligibility queue. The persist failure path itself is
// best-effort: if the DB write fails, the next tick will pick up the same
// extraction, classify again, and try the persist again. Logging the persist
// failure separately so operators can spot a stuck-in-loop pattern.
func (worker *BridgeWorker) persistTerminalFailure(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	class vo.BridgeErrorClass,
	originalErr error,
) {
	logger, _ := worker.tracking(ctx)

	extraction.RecordBridgeAttempt()

	message := "terminal: " + terminalFailureMessage(originalErr)
	if markErr := extraction.MarkBridgeFailed(class, message); markErr != nil {
		logger.With(
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.String("class", string(class)),
			libLog.String("error", markErr.Error()),
		).Log(ctx, libLog.LevelError, "bridge: domain mark-failed rejected (wiring bug)")

		return
	}

	if persistErr := worker.extractionRepo.MarkBridgeFailed(ctx, extraction); persistErr != nil {
		if errors.Is(persistErr, sharedPorts.ErrExtractionAlreadyLinked) {
			// Concurrent LinkIfUnlinked won the race between the worker's
			// read and this persist — likely the lock TTL expired while the
			// orchestrator was downstream. The link is the authoritative
			// outcome (invariant: EITHER linked OR terminally-failed, never
			// both), so treat the skipped failure write as benign. C21.
			logger.With(
				libLog.String("extraction.id", extraction.ID.String()),
				libLog.String("class", string(class)),
			).Log(ctx, libLog.LevelInfo, "bridge: terminal-failure write skipped — concurrent link won")

			return
		}

		logger.With(
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.String("bridge.class", string(class)),
			libLog.String("error", persistErr.Error()),
		).Log(ctx, libLog.LevelError, "bridge: persist terminal failure failed")
	}
}

// handleTransientFailure increments bridge_attempts, escalates to terminal
// if the configured ceiling is reached, otherwise persists just the bumped
// attempts via the existing Update path.
func (worker *BridgeWorker) handleTransientFailure(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	originalErr error,
) {
	logger, _ := worker.tracking(ctx)

	attempts := extraction.RecordBridgeAttempt()

	if worker.cfg.Retry.ShouldEscalate(attempts) {
		escalated := EscalateAfterMaxAttempts(originalErr)
		message := fmt.Sprintf(
			"terminal: escalated after %d attempts: %s",
			attempts,
			terminalFailureMessage(originalErr),
		)

		if markErr := extraction.MarkBridgeFailed(escalated, message); markErr != nil {
			logger.With(
				libLog.String("extraction.id", extraction.ID.String()),
				libLog.String("class", string(escalated)),
				libLog.String("error", markErr.Error()),
			).Log(ctx, libLog.LevelError, "bridge: domain mark-failed rejected during escalation")

			return
		}

		if persistErr := worker.extractionRepo.MarkBridgeFailed(ctx, extraction); persistErr != nil {
			logger.With(
				libLog.String("extraction.id", extraction.ID.String()),
				libLog.String("bridge.class", string(escalated)),
				libLog.String("error", persistErr.Error()),
			).Log(ctx, libLog.LevelError, "bridge: persist escalated failure failed")
		}

		return
	}

	// Below the ceiling: persist ONLY the bumped attempts + updated_at via
	// the narrow IncrementBridgeAttempts UPDATE (Polish Fix 3). The wide
	// Update path could otherwise clobber a concurrent link write under a
	// lock-TTL-expiry edge case — the narrow UPDATE is gated by
	// `ingestion_job_id IS NULL` so that race produces ErrExtractionAlreadyLinked
	// (logged at info level, not warn) instead of silent data corruption.
	persistErr := worker.extractionRepo.IncrementBridgeAttempts(ctx, extraction.ID, attempts)
	if persistErr == nil {
		return
	}

	if errors.Is(persistErr, sharedPorts.ErrExtractionAlreadyLinked) {
		// Concurrent link won the race. The link itself is the desired
		// outcome — stop retrying. Log at info because this is benign
		// concurrency, not an error.
		logger.With(
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.Any("attempts", attempts),
		).Log(ctx, libLog.LevelInfo, "bridge: transient retry skipped — concurrent link won")

		return
	}

	logger.With(
		libLog.String("extraction.id", extraction.ID.String()),
		libLog.Any("attempts", attempts),
		libLog.String("error", persistErr.Error()),
	).Log(ctx, libLog.LevelWarn, "bridge: failed to persist transient attempt counter")
}

// terminalFailureMessage builds the operator-facing message persisted in
// bridge_last_error_message. Bounded by the entity's MaxBridgeFailureMessageLength.
//
// A nil err is a wiring bug: every caller reaches this helper through
// persistTerminalFailure or handleTransientFailure's escalation branch, both
// of which are reached only after the classifier maps a non-nil bridgeOne
// error to a terminal policy. A nil here means the classifier or its caller
// leaked a nil-error code path into persistence, which would otherwise
// silently write a meaningless "unknown failure" row. Return a loud,
// self-identifying marker so operators grepping audit rows spot the bug
// instead of the symptom.
func terminalFailureMessage(err error) string {
	if err == nil {
		return "internal: nil failure (wiring bug — terminalFailureMessage called with nil err)"
	}

	return err.Error()
}

func (worker *BridgeWorker) logBridgeError(
	ctx context.Context,
	logger libLog.Logger,
	extractionID uuid.UUID,
	tenantID string,
	err error,
) {
	level := libLog.LevelError

	// Source-unresolvable is a config gap, not a transient failure. Log
	// at WARN so operators see it without page-worthy urgency.
	if errors.Is(err, sharedPorts.ErrBridgeSourceUnresolvable) {
		level = libLog.LevelWarn
	}

	logger.With(
		libLog.String("tenant.id", tenantID),
		libLog.String("extraction.id", extractionID.String()),
		libLog.String("error", err.Error()),
	).Log(ctx, level, "bridge: extraction failed")
}
