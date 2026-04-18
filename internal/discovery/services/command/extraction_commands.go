package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// StartExtraction submits a data extraction job to Fetcher.
// It creates an ExtractionRequest record to track the job lifecycle and delegates
// the actual extraction to the Fetcher service via the FetcherClient port.
// The ingestionJobID is not required; extraction requests can exist independently
// and be linked to ingestion jobs later when the data is imported.
//
//nolint:cyclop,gocyclo // workflow branches explicitly on connection ownership, persistence, and remote submission outcomes.
func (uc *UseCase) StartExtraction(
	ctx context.Context,
	connectionID uuid.UUID,
	tables map[string]any,
	params sharedPorts.ExtractionParams,
) (*entities.ExtractionRequest, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.start_extraction")
	defer span.End()

	if err := validateExtractionRequest(tables, params); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "validate extraction request", err)

		return nil, err
	}

	if !uc.fetcherClient.IsHealthy(ctx) {
		libOpentelemetry.HandleSpanError(span, "fetcher unavailable", ErrFetcherUnavailable)

		return nil, ErrFetcherUnavailable
	}

	conn, err := uc.connRepo.FindByID(ctx, connectionID)
	if err != nil {
		if errors.Is(err, repositories.ErrConnectionNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "connection not found", err)

			return nil, ErrConnectionNotFound
		}

		libOpentelemetry.HandleSpanError(span, "find connection", err)

		return nil, fmt.Errorf("find connection: %w", err)
	}

	if conn == nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "connection not found", ErrConnectionNotFound)

		return nil, ErrConnectionNotFound
	}

	if conn.Status != vo.ConnectionStatusAvailable || !conn.SchemaDiscovered {
		err = fmt.Errorf("%w: connection schema is not available for extraction", ErrInvalidExtractionRequest)
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "connection not ready for extraction", err)

		return nil, err
	}

	schemas, err := uc.schemaRepo.FindByConnectionID(ctx, connectionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "find connection schema", err)

		return nil, fmt.Errorf("find connection schema: %w", err)
	}

	if err := validateExtractionScope(tables, schemas); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "validate extraction scope", err)

		return nil, err
	}

	extractionReq, err := entities.NewExtractionRequest(ctx, connectionID, tables, params.StartDate, params.EndDate, params.Filters.ToMap())
	if err != nil {
		return nil, fmt.Errorf("create extraction request: %w", err)
	}

	if err := uc.extractionRepo.Create(ctx, extractionReq); err != nil {
		return nil, fmt.Errorf("persist pending extraction request: %w", err)
	}

	input, inputErr := buildExtractionJobInput(conn, tables, params)
	if inputErr != nil {
		return nil, inputErr
	}

	jobID, err := uc.fetcherClient.SubmitExtractionJob(ctx, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "submit extraction job", err)

		if markErr := extractionReq.MarkFailed(entities.SanitizedExtractionFailureMessage); markErr == nil {
			if updateErr := uc.extractionRepo.Update(ctx, extractionReq); updateErr != nil {
				libOpentelemetry.HandleSpanError(span, "persist failed extraction request", updateErr)
			}
		}

		return nil, fmt.Errorf("submit extraction job: %w", err)
	}

	if err := extractionReq.MarkSubmitted(jobID); err != nil {
		return nil, fmt.Errorf("mark extraction submitted: %w", err)
	}

	if err := uc.persistSubmittedExtraction(ctx, span, extractionReq); err != nil {
		return nil, fmt.Errorf("persist submitted extraction request: %w", err)
	}

	// Start async polling for extraction completion (if poller configured).
	if uc.extractionPoller != nil {
		// Polling must outlive the originating HTTP request context, otherwise
		// client cancellation aborts extraction tracking mid-flight.
		uc.extractionPoller.PollUntilComplete(context.WithoutCancel(ctx), extractionReq.ID, nil, nil)
	}

	return extractionReq, nil
}

// buildExtractionJobInput assembles the Fetcher extraction request from the
// connection metadata, requested tables, and extraction parameters.
// MappedFields: configName -> table -> columns. Filters: configName -> table -> filter.
func buildExtractionJobInput(
	conn *entities.FetcherConnection,
	tables map[string]any,
	params sharedPorts.ExtractionParams,
) (sharedPorts.ExtractionJobInput, error) {
	configName := conn.ConfigName
	if configName == "" {
		configName = conn.FetcherConnID // fallback
	}

	tableMap := make(map[string][]string, len(tables))

	for name, cfg := range tables {
		columns, colErr := extractRequestedColumns(cfg)
		if colErr != nil {
			return sharedPorts.ExtractionJobInput{}, colErr
		}

		tableMap[name] = columns
	}

	mappedFields := map[string]map[string][]string{
		configName: tableMap,
	}

	// Build Filters (if any): configName -> table -> filter
	var filters map[string]map[string]map[string]any

	if params.Filters != nil {
		filterMap := params.Filters.ToMap()
		if len(filterMap) > 0 {
			tableFilters := make(map[string]map[string]any, len(tables))
			for name := range tables {
				tableFilters[name] = filterMap
			}

			filters = map[string]map[string]map[string]any{
				configName: tableFilters,
			}
		}
	}

	// Build Metadata with required "source" key.
	// Use ConfigName (user-assigned unique identifier, e.g. "prod-db") rather than
	// ProductName (human label, e.g. "PostgreSQL 16.2") to ensure unique provenance
	// when multiple connections share the same engine type.
	metadata := map[string]any{
		"source": configName,
	}

	return sharedPorts.ExtractionJobInput{
		MappedFields: mappedFields,
		Filters:      filters,
		Metadata:     metadata,
	}, nil
}

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
