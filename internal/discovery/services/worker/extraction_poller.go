package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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
	}, nil
}

// PollUntilComplete starts polling for an extraction job completion.
// It runs asynchronously in a goroutine managed by runtime.SafeGoWithContextAndComponent.
func (ep *ExtractionPoller) PollUntilComplete(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	if extraction == nil {
		if onFailed != nil {
			onFailed(ctx, "nil extraction request")
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
			ep.doPoll(innerCtx, extraction, onComplete, onFailed)
		},
	)
}

func (ep *ExtractionPoller) doPoll(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	ticker := time.NewTicker(ep.cfg.PollInterval)
	defer ticker.Stop()

	deadline := time.After(ep.cfg.Timeout)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			ep.handleTimeout(ctx, extraction, onFailed)

			return
		case <-ticker.C:
			done := ep.pollOnce(ctx, extraction, onComplete, onFailed)
			if done {
				return
			}
		}
	}
}

// handleTimeout marks the extraction as failed due to timeout and invokes the failure callback.
func (ep *ExtractionPoller) handleTimeout(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	onFailed func(ctx context.Context, errMsg string),
) {
	if err := extraction.MarkFailed("extraction timed out"); err != nil {
		ep.logger.With(libLog.Any("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "extraction poller: failed to mark extraction as failed on timeout")
	}

	if err := ep.extractionRepo.Update(ctx, extraction); err != nil {
		ep.logger.With(libLog.Any("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "extraction poller: failed to update extraction on timeout")
	}

	if onFailed != nil {
		onFailed(ctx, "extraction timed out")
	}
}

// pollOnce checks the extraction status once and returns true if terminal.
func (ep *ExtractionPoller) pollOnce(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) bool {
	logger := loggerFromContext(ctx, ep.logger)
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // need tracer for span

	ctx, span := tracer.Start(ctx, "discovery.poll_extraction_once")
	defer span.End()

	if extraction == nil {
		logger.Log(ctx, libLog.LevelWarn, "extraction poller: nil extraction request")

		if onFailed != nil {
			onFailed(ctx, "nil extraction request")
		}

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
		logger.With(libLog.Any("error", err.Error())).
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
//nolint:cyclop // status switch inherently branches per extraction state; further splitting hurts readability.
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
	case "COMPLETE":
		previousStatus := extraction.Status
		previousResultPath := extraction.ResultPath
		previousUpdatedAt := extraction.UpdatedAt

		if err := extraction.MarkComplete(status.ResultPath); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark extraction complete", err)
			return true // terminal — stop polling even on transition error
		}

		if err := ep.extractionRepo.Update(ctx, extraction); err != nil {
			libOpentelemetry.HandleSpanError(span, "update extraction complete", err)

			extraction.Status = previousStatus
			extraction.ResultPath = previousResultPath
			extraction.UpdatedAt = previousUpdatedAt

			return false // retry on next poll — DB may recover
		}

		if onComplete != nil {
			if err := onComplete(ctx, status.ResultPath); err != nil {
				logger.With(libLog.Any("error", err.Error())).
					Log(ctx, libLog.LevelWarn, "extraction complete callback failed")
			}
		}

		return true

	case "FAILED":
		previousStatus := extraction.Status
		previousErrorMessage := extraction.ErrorMessage
		previousUpdatedAt := extraction.UpdatedAt

		if err := extraction.MarkFailed(status.ErrorMessage); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark extraction failed", err)
			return true // terminal — stop polling even on transition error
		}

		if err := ep.extractionRepo.Update(ctx, extraction); err != nil {
			libOpentelemetry.HandleSpanError(span, "update extraction failed", err)

			extraction.Status = previousStatus
			extraction.ErrorMessage = previousErrorMessage
			extraction.UpdatedAt = previousUpdatedAt

			return false // retry on next poll — DB may recover
		}

		if onFailed != nil {
			onFailed(ctx, status.ErrorMessage)
		}

		return true

	case "RUNNING", "EXTRACTING":
		if err := extraction.MarkExtracting(); err != nil {
			// Non-fatal: log but don't update DB with potentially inconsistent state.
			logger.With(libLog.Any("error", err.Error())).
				Log(ctx, libLog.LevelWarn, "extraction poller: failed to mark extraction as extracting")

			return false
		}

		if err := ep.extractionRepo.Update(ctx, extraction); err != nil {
			logger.With(libLog.Any("error", err.Error())).
				Log(ctx, libLog.LevelWarn, "extraction poller: failed to update extraction status")
		}
	default:
		logger.With(libLog.String("fetcher.status", status.Status)).
			Log(ctx, libLog.LevelWarn, "extraction poller: unknown extraction status")
	}

	return false
}
