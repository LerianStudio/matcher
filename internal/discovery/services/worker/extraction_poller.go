// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	discoveryMetrics "github.com/LerianStudio/matcher/internal/discovery/services/metrics"
	workermetrics "github.com/LerianStudio/matcher/internal/shared/observability/workermetrics"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// extractionPollerName is the stable label value emitted on matcher.worker.*
// metrics from this poller. The extraction poller is not a ticker-based
// background worker — it spawns a per-extraction goroutine that polls to
// terminal state. A "cycle" here means "one PollUntilComplete call ran to
// its natural end" (terminal state, timeout, or context cancellation).
const extractionPollerName = "extraction_poller"

// loggerFromContext extracts a logger from context, falling back to the provided default.
func loggerFromContext(ctx context.Context, fallback libLog.Logger) libLog.Logger {
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // utility wrapper
	if logger == nil {
		return fallback
	}

	return logger
}

// Sentinel errors for the extraction poller.
var (
	ErrNilExtractionRepository = errors.New("extraction repository is required")
)

// Default poller configuration values.
const (
	defaultPollInterval = 5 * time.Second
	defaultPollTimeout  = 10 * time.Minute
)

// ExtractionPollerConfig configures extraction polling behavior.
type ExtractionPollerConfig struct {
	// PollInterval is the time between status checks.
	PollInterval time.Duration
	// Timeout is the maximum duration to wait for extraction completion.
	Timeout time.Duration
}

// ExtractionPoller polls Fetcher for extraction job completion.
// Unlike the DiscoveryWorker, this is NOT a background worker.
// It spawns a per-extraction goroutine that polls until the job completes or times out.
type ExtractionPoller struct {
	fetcherClient  sharedPorts.FetcherClient
	extractionRepo repositories.ExtractionRepository
	logger         libLog.Logger
	cfg            ExtractionPollerConfig
	metrics        *workermetrics.Recorder
}

// NewExtractionPoller creates a new extraction poller.
func NewExtractionPoller(
	fetcherClient sharedPorts.FetcherClient,
	extractionRepo repositories.ExtractionRepository,
	cfg ExtractionPollerConfig,
	logger libLog.Logger,
) (*ExtractionPoller, error) {
	if fetcherClient == nil {
		return nil, ErrNilFetcherClient
	}

	if extractionRepo == nil {
		return nil, ErrNilExtractionRepository
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultPollTimeout
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &ExtractionPoller{
		fetcherClient:  fetcherClient,
		extractionRepo: extractionRepo,
		logger:         logger,
		cfg:            cfg,
		metrics:        workermetrics.NewRecorder(extractionPollerName),
	}, nil
}

// PollUntilComplete starts polling for an extraction job completion.
// It runs asynchronously in a goroutine managed by runtime.SafeGoWithContextAndComponent.
func (ep *ExtractionPoller) PollUntilComplete(
	ctx context.Context,
	extractionID uuid.UUID,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	if ep == nil {
		if onFailed != nil {
			onFailed(ctx, "extraction poller unavailable")
		}

		return
	}

	if extractionID == uuid.Nil {
		if onFailed != nil {
			onFailed(ctx, "extraction id is required")
		}

		return
	}

	runtime.SafeGoWithContextAndComponent(
		ctx,
		ep.logger,
		"discovery",
		"extraction_poller",
		runtime.KeepRunning,
		func(innerCtx context.Context) {
			ep.doPoll(innerCtx, extractionID, onComplete, onFailed)
		},
	)
}

func (ep *ExtractionPoller) doPoll(
	ctx context.Context,
	extractionID uuid.UUID,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	// The extraction poller's "cycle" is one complete PollUntilComplete call
	// from start to terminal state (or timeout / ctx cancel). Item count is
	// therefore 1 per cycle; success vs failure drops on which of the two
	// callbacks fires first. We wrap the original callbacks in a small
	// state observer so the final outcome is recorded exactly once regardless
	// of which exit path the state machine takes (pollOnce terminal path,
	// handleTimeout, or ctx.Done — the last being "skipped" since the caller
	// voluntarily cancelled before a terminal classification was reached).
	startedAt := time.Now()
	outcome := workermetrics.OutcomeSkipped

	var processed, failed int

	wrapComplete := func(cbCtx context.Context, resultPath string) error {
		outcome = workermetrics.OutcomeSuccess
		processed = 1

		if onComplete == nil {
			return nil
		}

		return onComplete(cbCtx, resultPath)
	}

	wrapFailed := func(cbCtx context.Context, errMsg string) {
		outcome = workermetrics.OutcomeFailure
		failed = 1

		if onFailed != nil {
			onFailed(cbCtx, errMsg)
		}
	}

	defer func() {
		ep.metrics.RecordCycle(ctx, startedAt, outcome)
		ep.metrics.RecordItems(ctx, processed, failed)
	}()

	ticker := time.NewTicker(ep.cfg.PollInterval)
	defer ticker.Stop()

	deadline := time.After(ep.cfg.Timeout)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			ep.handleTimeout(ctx, extractionID, wrapFailed)

			return
		case <-ticker.C:
			done := ep.pollOnce(ctx, extractionID, wrapComplete, wrapFailed)
			if done {
				return
			}
		}
	}
}

// handleTimeout marks the extraction as failed due to timeout and invokes the failure callback.
func (ep *ExtractionPoller) handleTimeout(
	ctx context.Context,
	extractionID uuid.UUID,
	onFailed func(ctx context.Context, errMsg string),
) {
	extraction, err := ep.extractionRepo.FindByID(ctx, extractionID)
	if err != nil || extraction == nil {
		if onFailed != nil {
			onFailed(ctx, "extraction timed out")
		}

		return
	}

	if extraction.Status.IsTerminal() {
		return
	}

	expectedUpdatedAt := extraction.UpdatedAt

	if err := extraction.MarkFailed("extraction timed out"); err != nil {
		ep.logger.With(libLog.Err(err)).
			Log(ctx, libLog.LevelWarn, "extraction poller: failed to mark extraction as failed on timeout")
	}

	if err := ep.extractionRepo.UpdateIfUnchanged(ctx, extraction, expectedUpdatedAt); err != nil {
		if errors.Is(err, repositories.ErrExtractionConflict) {
			return
		}

		ep.logger.With(libLog.Err(err)).
			Log(ctx, libLog.LevelWarn, "extraction poller: failed to update extraction on timeout")
	} else {
		discoveryMetrics.RecordExtractionState(ctx, extraction.Status.String())
	}

	if onFailed != nil {
		onFailed(ctx, "extraction timed out")
	}
}

// pollOnce checks the extraction status once and returns true if terminal.
//
//nolint:cyclop,nestif // polling must handle fetcher drift, remote cancellation, and conflict-aware persistence in one place.
func (ep *ExtractionPoller) pollOnce(
	ctx context.Context,
	extractionID uuid.UUID,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) bool {
	logger := loggerFromContext(ctx, ep.logger)
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // need tracer for span

	ctx, span := tracer.Start(ctx, "discovery.poll_extraction_once")
	defer span.End()

	extraction, err := ep.extractionRepo.FindByID(ctx, extractionID)
	if err != nil {
		logger.With(libLog.Err(err)).
			Log(ctx, libLog.LevelWarn, "extraction poller: failed to reload extraction request")

		return false
	}

	if extraction == nil {
		logger.Log(ctx, libLog.LevelWarn, "extraction poller: extraction request not found")

		if onFailed != nil {
			onFailed(ctx, "extraction request not found")
		}

		return true
	}

	if extraction.Status.IsTerminal() {
		return true
	}

	if extraction.FetcherJobID == "" {
		logger.Log(ctx, libLog.LevelWarn, "extraction poller: extraction missing fetcher job id")

		if onFailed != nil {
			onFailed(ctx, "missing fetcher job id")
		}

		return true
	}

	status, err := ep.fetcherClient.GetExtractionJobStatus(ctx, extraction.FetcherJobID)
	if err != nil {
		if errors.Is(err, sharedPorts.ErrFetcherResourceNotFound) {
			expectedUpdatedAt := extraction.UpdatedAt

			if cancelErr := extraction.MarkCancelled(); cancelErr != nil {
				logger.With(libLog.Err(cancelErr)).
					Log(ctx, libLog.LevelWarn, "extraction poller: failed to mark extraction as cancelled")

				return true
			}

			if updateErr := ep.extractionRepo.UpdateIfUnchanged(ctx, extraction, expectedUpdatedAt); updateErr != nil {
				if errors.Is(updateErr, repositories.ErrExtractionConflict) {
					return ep.stopOnConflict(ctx, extraction.ID)
				}

				logger.With(libLog.Err(updateErr)).
					Log(ctx, libLog.LevelWarn, "extraction poller: failed to persist cancelled extraction")

				return false
			}

			discoveryMetrics.RecordExtractionState(ctx, extraction.Status.String())

			if onFailed != nil {
				onFailed(ctx, "extraction cancelled")
			}

			return true
		}

		logger.With(libLog.Err(err)).
			Log(ctx, libLog.LevelWarn,
				fmt.Sprintf("poll extraction %s: %v", extraction.FetcherJobID, err))

		return false
	}

	if status == nil {
		logger.Log(ctx, libLog.LevelWarn, "extraction poller: fetcher returned nil status")

		return false
	}

	return ep.handlePollStatus(ctx, span, logger, extraction, status, onComplete, onFailed)
}

// handlePollStatus processes the extraction status and invokes callbacks.
//
//nolint:cyclop,funlen,gocognit,gocyclo // status switch inherently branches per extraction state; further splitting hurts readability.
func (ep *ExtractionPoller) handlePollStatus(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	extraction *entities.ExtractionRequest,
	status *sharedPorts.ExtractionJobStatus,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) bool {
	switch status.Status {
	case "PENDING", "SUBMITTED":
		return false

	case "COMPLETE":
		// NOTE: Intentionally duplicated with extraction_commands.go handleExtractionStatus.
		// Both call sites handle COMPLETE independently (polled vs inline) in different
		// packages; extracting a shared helper would add cross-package coupling for a
		// single log statement.
		if status.ResultHmac != "" {
			logger.With(
				libLog.String("fetcher.job_id", extraction.FetcherJobID),
				libLog.String("result_hmac", status.ResultHmac),
			).Log(ctx, libLog.LevelWarn,
				"extraction result HMAC received but not verified: "+
					"Matcher does not download extraction data and lacks the external HMAC key")
		}

		expectedUpdatedAt := extraction.UpdatedAt
		previousStatus := extraction.Status
		previousResultPath := extraction.ResultPath
		previousUpdatedAt := extraction.UpdatedAt

		if err := extraction.MarkComplete(status.ResultPath); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark extraction complete", err)
			return true // terminal — stop polling even on transition error
		}

		if err := ep.extractionRepo.UpdateIfUnchanged(ctx, extraction, expectedUpdatedAt); err != nil {
			if errors.Is(err, repositories.ErrExtractionConflict) {
				return ep.stopOnConflict(ctx, extraction.ID)
			}

			libOpentelemetry.HandleSpanError(span, "update extraction complete", err)

			extraction.Status = previousStatus
			extraction.ResultPath = previousResultPath
			extraction.UpdatedAt = previousUpdatedAt

			return false // retry on next poll — DB may recover
		}

		discoveryMetrics.RecordExtractionState(ctx, extraction.Status.String())

		if onComplete != nil {
			if err := onComplete(ctx, status.ResultPath); err != nil {
				logger.With(libLog.Err(err)).
					Log(ctx, libLog.LevelWarn, "extraction complete callback failed")
			}
		}

		return true

	case "FAILED":
		expectedUpdatedAt := extraction.UpdatedAt
		previousStatus := extraction.Status
		previousErrorMessage := extraction.ErrorMessage
		previousUpdatedAt := extraction.UpdatedAt

		if err := extraction.MarkFailed(entities.SanitizedExtractionFailureMessage); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark extraction failed", err)
			return true // terminal — stop polling even on transition error
		}

		if err := ep.extractionRepo.UpdateIfUnchanged(ctx, extraction, expectedUpdatedAt); err != nil {
			if errors.Is(err, repositories.ErrExtractionConflict) {
				return ep.stopOnConflict(ctx, extraction.ID)
			}

			libOpentelemetry.HandleSpanError(span, "update extraction failed", err)

			extraction.Status = previousStatus
			extraction.ErrorMessage = previousErrorMessage
			extraction.UpdatedAt = previousUpdatedAt

			return false // retry on next poll — DB may recover
		}

		discoveryMetrics.RecordExtractionState(ctx, extraction.Status.String())

		if onFailed != nil {
			onFailed(ctx, entities.SanitizedExtractionFailureMessage)
		}

		return true

	case "CANCELLED":
		expectedUpdatedAt := extraction.UpdatedAt
		previousStatus := extraction.Status
		previousErrorMessage := extraction.ErrorMessage
		previousResultPath := extraction.ResultPath
		previousUpdatedAt := extraction.UpdatedAt

		if err := extraction.MarkCancelled(); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark extraction cancelled", err)
			return true
		}

		if err := ep.extractionRepo.UpdateIfUnchanged(ctx, extraction, expectedUpdatedAt); err != nil {
			if errors.Is(err, repositories.ErrExtractionConflict) {
				return ep.stopOnConflict(ctx, extraction.ID)
			}

			libOpentelemetry.HandleSpanError(span, "update extraction cancelled", err)

			extraction.Status = previousStatus
			extraction.ErrorMessage = previousErrorMessage
			extraction.ResultPath = previousResultPath
			extraction.UpdatedAt = previousUpdatedAt

			return false
		}

		discoveryMetrics.RecordExtractionState(ctx, extraction.Status.String())

		if onFailed != nil {
			onFailed(ctx, "extraction cancelled")
		}

		return true

	case "RUNNING", "EXTRACTING":
		expectedUpdatedAt := extraction.UpdatedAt
		previousStatus := extraction.Status
		previousUpdatedAt := extraction.UpdatedAt

		if err := extraction.MarkExtracting(); err != nil {
			// Non-fatal: log but don't update DB with potentially inconsistent state.
			logger.With(libLog.Err(err)).
				Log(ctx, libLog.LevelWarn, "extraction poller: failed to mark extraction as extracting")

			return false
		}

		if extraction.Status == previousStatus && extraction.UpdatedAt.Equal(previousUpdatedAt) {
			break
		}

		err := ep.extractionRepo.UpdateIfUnchanged(ctx, extraction, expectedUpdatedAt)
		if err == nil {
			if extraction.Status != previousStatus {
				discoveryMetrics.RecordExtractionState(ctx, extraction.Status.String())
			}

			break
		}

		if errors.Is(err, repositories.ErrExtractionConflict) {
			return ep.stopOnConflict(ctx, extraction.ID)
		}

		logger.With(libLog.Err(err)).
			Log(ctx, libLog.LevelWarn, "extraction poller: failed to update extraction status")
	default:
		logger.With(libLog.String("fetcher.status", status.Status)).
			Log(ctx, libLog.LevelWarn, "extraction poller: unknown extraction status")
	}

	return false
}

func (ep *ExtractionPoller) stopOnConflict(ctx context.Context, extractionID uuid.UUID) bool {
	latest, err := ep.extractionRepo.FindByID(ctx, extractionID)
	if err != nil || latest == nil {
		return false
	}

	return latest.Status.IsTerminal()
}
