package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// PollExtractionStatus checks the status of an in-flight extraction job and updates
// the local ExtractionRequest accordingly. It transitions the request through
// EXTRACTING, COMPLETE, or FAILED based on the Fetcher's response.
//
//nolint:cyclop,gocognit,gocyclo,nestif // polling combines fetcher errors, remote lifecycle transitions, and optimistic concurrency handling.
func (uc *UseCase) PollExtractionStatus(ctx context.Context, extractionID uuid.UUID) (*entities.ExtractionRequest, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.poll_extraction_status")
	defer span.End()

	req, err := uc.extractionRepo.FindByID(ctx, extractionID)
	if err != nil {
		if errors.Is(err, repositories.ErrExtractionNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "extraction request not found", err)

			return nil, ErrExtractionNotFound
		}

		libOpentelemetry.HandleSpanError(span, "find extraction request", err)

		return nil, fmt.Errorf("find extraction request: %w", err)
	}

	if req == nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "extraction request not found", ErrExtractionNotFound)

		return nil, ErrExtractionNotFound
	}

	if req.Status.IsTerminal() {
		return req, nil // already complete or failed
	}

	if strings.TrimSpace(req.FetcherJobID) == "" {
		libOpentelemetry.HandleSpanError(span, "missing fetcher job id", ErrExtractionTrackingIncomplete)

		return nil, ErrExtractionTrackingIncomplete
	}

	expectedUpdatedAt := req.UpdatedAt

	status, err := uc.fetcherClient.GetExtractionJobStatus(ctx, req.FetcherJobID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "get extraction job status", err)

		if errors.Is(err, sharedPorts.ErrFetcherUnavailable) {
			return nil, ErrFetcherUnavailable
		}

		if errors.Is(err, sharedPorts.ErrFetcherResourceNotFound) {
			if cancelErr := req.MarkCancelled(); cancelErr != nil {
				return nil, fmt.Errorf("mark cancelled: %w", cancelErr)
			}

			if err := uc.extractionRepo.UpdateIfUnchanged(ctx, req, expectedUpdatedAt); err != nil {
				if errors.Is(err, repositories.ErrExtractionConflict) {
					latest, reloadErr := uc.extractionRepo.FindByID(ctx, extractionID)
					if reloadErr == nil && latest != nil {
						return latest, nil
					}

					if reloadErr != nil {
						return nil, fmt.Errorf("reload extraction request after conflict: %w", reloadErr)
					}

					return nil, ErrExtractionNotFound
				}

				return nil, fmt.Errorf("cancel extraction request after remote not found: %w", err)
			}

			return req, nil
		}

		return nil, fmt.Errorf("get extraction job status: %w", err)
	}

	if status == nil {
		libOpentelemetry.HandleSpanError(span, "nil extraction status", ErrNilExtractionStatus)

		return nil, ErrNilExtractionStatus
	}

	// Diagnostic: compare submitted MappedFields against Fetcher's echo.
	// Divergence is expected when Fetcher auto-qualifies table names.
	// The entity stores the requested tables but not the full 3-level
	// MappedFields (configName wrapper is built at submission time and not
	// persisted). We reconstruct the inner table→columns map from the entity
	// for comparison against the echo's inner layer.
	if len(status.MappedFields) > 0 {
		submittedTables := extractSubmittedColumns(req.Tables)
		logMappedFieldsDivergence(ctx, submittedTables, status.MappedFields, req.FetcherJobID)
	}

	changed, err := uc.applyExtractionStatusTransition(ctx, span, req, status)
	if err != nil {
		return nil, err
	}

	if changed {
		if err := uc.extractionRepo.UpdateIfUnchanged(ctx, req, expectedUpdatedAt); err != nil {
			if errors.Is(err, repositories.ErrExtractionConflict) {
				latest, reloadErr := uc.extractionRepo.FindByID(ctx, extractionID)
				if reloadErr == nil && latest != nil {
					return latest, nil
				}

				if reloadErr != nil {
					return nil, fmt.Errorf("reload extraction request after conflict: %w", reloadErr)
				}

				return nil, ErrExtractionNotFound
			}

			return nil, fmt.Errorf("update extraction request: %w", err)
		}
	}

	return req, nil
}

// applyExtractionStatusTransition transitions the extraction request based on Fetcher's reported status.
//
//nolint:cyclop,gocyclo // extraction lifecycle is an explicit finite-state switch and reads clearest as one transition table.
func (uc *UseCase) applyExtractionStatusTransition(
	ctx context.Context,
	span trace.Span,
	req *entities.ExtractionRequest,
	status *sharedPorts.ExtractionJobStatus,
) (bool, error) {
	changed := false

	switch status.Status {
	case "PENDING", "SUBMITTED":
		return false, nil
	case "RUNNING", "EXTRACTING":
		previousStatus := req.Status
		previousUpdatedAt := req.UpdatedAt

		if err := req.MarkExtracting(); err != nil {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "mark extracting", err)

			return false, fmt.Errorf("mark extracting: %w", err)
		}

		changed = previousStatus != req.Status || !req.UpdatedAt.Equal(previousUpdatedAt)
	case "COMPLETE":
		// NOTE: Intentionally duplicated with extraction_poller.go handlePollStatus.
		// Both call sites handle COMPLETE independently (inline vs polled) in different
		// packages; extracting a shared helper would add cross-package coupling for a
		// single log statement.
		if status.ResultHmac != "" {
			logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
			logger.With(
				libLog.String("fetcher.job_id", req.FetcherJobID),
				libLog.String("result_hmac", status.ResultHmac),
			).Log(ctx, libLog.LevelWarn,
				"extraction result HMAC received but not verified: "+
					"Matcher does not download extraction data and lacks the external HMAC key")
		}

		previousStatus := req.Status
		previousResultPath := req.ResultPath
		previousErrorMessage := req.ErrorMessage
		previousUpdatedAt := req.UpdatedAt

		if err := req.MarkComplete(status.ResultPath); err != nil {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "mark complete", err)

			return false, fmt.Errorf("mark complete: %w", err)
		}

		changed = previousStatus != req.Status ||
			previousResultPath != req.ResultPath ||
			previousErrorMessage != req.ErrorMessage ||
			!req.UpdatedAt.Equal(previousUpdatedAt)
	case "FAILED":
		previousStatus := req.Status
		previousErrorMessage := req.ErrorMessage
		previousResultPath := req.ResultPath
		previousUpdatedAt := req.UpdatedAt

		if err := req.MarkFailed(entities.SanitizedExtractionFailureMessage); err != nil {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "mark failed", err)

			return false, fmt.Errorf("mark failed: %w", err)
		}

		changed = previousStatus != req.Status ||
			previousErrorMessage != req.ErrorMessage ||
			previousResultPath != req.ResultPath ||
			!req.UpdatedAt.Equal(previousUpdatedAt)
	case "CANCELLED":
		previousStatus := req.Status
		previousErrorMessage := req.ErrorMessage
		previousResultPath := req.ResultPath
		previousUpdatedAt := req.UpdatedAt

		if err := req.MarkCancelled(); err != nil {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "mark cancelled", err)

			return false, fmt.Errorf("mark cancelled: %w", err)
		}

		changed = previousStatus != req.Status ||
			previousErrorMessage != req.ErrorMessage ||
			previousResultPath != req.ResultPath ||
			!req.UpdatedAt.Equal(previousUpdatedAt)
	default:
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("unknown extraction status %q for job %s", status.Status, req.FetcherJobID))
	}

	return changed, nil
}
