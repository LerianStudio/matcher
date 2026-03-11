package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// StartExtraction submits a data extraction job to Fetcher.
// It creates an ExtractionRequest record to track the job lifecycle and delegates
// the actual extraction to the Fetcher service via the FetcherClient port.
// The ingestionJobID is not required; extraction requests can exist independently
// and be linked to ingestion jobs later when the data is imported.
//
//nolint:cyclop // workflow branches explicitly on connection ownership, persistence, and remote submission outcomes.
func (uc *UseCase) StartExtraction(
	ctx context.Context,
	connectionID uuid.UUID,
	tables map[string]any,
	params sharedPorts.ExtractionParams,
) (*entities.ExtractionRequest, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.start_extraction")
	defer span.End()

	if !uc.fetcherClient.IsHealthy(ctx) {
		libOpentelemetry.HandleSpanError(span, "fetcher unavailable", ErrFetcherUnavailable)

		return nil, ErrFetcherUnavailable
	}

	conn, err := uc.connRepo.FindByID(ctx, connectionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "find connection", err)

		if errors.Is(err, repositories.ErrConnectionNotFound) {
			return nil, ErrConnectionNotFound
		}

		return nil, fmt.Errorf("find connection: %w", err)
	}

	if conn == nil {
		libOpentelemetry.HandleSpanError(span, "find connection", ErrConnectionNotFound)

		return nil, ErrConnectionNotFound
	}

	extractionReq, err := entities.NewExtractionRequest(ctx, connectionID, tables, params.Filters)
	if err != nil {
		return nil, fmt.Errorf("create extraction request: %w", err)
	}

	if err := uc.extractionRepo.Create(ctx, extractionReq); err != nil {
		return nil, fmt.Errorf("persist pending extraction request: %w", err)
	}

	// Build the extraction job input from params.
	input := sharedPorts.ExtractionJobInput{
		ConnectionID: conn.FetcherConnID,
		Filters:      params.Filters,
		Tables:       make(map[string]sharedPorts.ExtractionTableConfig, len(tables)),
	}

	for name, cfg := range tables {
		input.Tables[name] = buildTableConfig(cfg, params)
	}

	jobID, err := uc.fetcherClient.SubmitExtractionJob(ctx, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "submit extraction job", err)

		if markErr := extractionReq.MarkFailed(err.Error()); markErr == nil {
			if updateErr := uc.extractionRepo.Update(ctx, extractionReq); updateErr != nil {
				libOpentelemetry.HandleSpanError(span, "persist failed extraction request", updateErr)
			}
		}

		return nil, fmt.Errorf("submit extraction job: %w", err)
	}

	if err := extractionReq.MarkSubmitted(jobID); err != nil {
		return nil, fmt.Errorf("mark extraction submitted: %w", err)
	}

	if err := uc.extractionRepo.Update(ctx, extractionReq); err != nil {
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

// buildTableConfig normalizes a single table's configuration from raw JSON-deserialized data.
func buildTableConfig(cfg any, params sharedPorts.ExtractionParams) sharedPorts.ExtractionTableConfig {
	tc := sharedPorts.ExtractionTableConfig{
		StartDate: params.StartDate,
		EndDate:   params.EndDate,
	}

	cfgMap, ok := cfg.(map[string]any)
	if !ok {
		return tc
	}

	switch cols := cfgMap["columns"].(type) {
	case []string:
		tc.Columns = cols
	case []any:
		strCols := make([]string, 0, len(cols))

		for _, col := range cols {
			if colStr, colOK := col.(string); colOK {
				strCols = append(strCols, colStr)
			}
		}

		if len(strCols) > 0 {
			tc.Columns = strCols
		}
	}

	return tc
}

// PollExtractionStatus checks the status of an in-flight extraction job and updates
// the local ExtractionRequest accordingly. It transitions the request through
// EXTRACTING, COMPLETE, or FAILED based on the Fetcher's response.
func (uc *UseCase) PollExtractionStatus(ctx context.Context, extractionID uuid.UUID) (*entities.ExtractionRequest, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.poll_extraction_status")
	defer span.End()

	req, err := uc.extractionRepo.FindByID(ctx, extractionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "find extraction request", err)

		if errors.Is(err, repositories.ErrExtractionNotFound) {
			return nil, ErrExtractionNotFound
		}

		return nil, fmt.Errorf("find extraction request: %w", err)
	}

	if req == nil {
		libOpentelemetry.HandleSpanError(span, "nil extraction request", ErrExtractionNotFound)

		return nil, ErrExtractionNotFound
	}

	if req.Status.IsTerminal() {
		return req, nil // already complete or failed
	}

	status, err := uc.fetcherClient.GetExtractionJobStatus(ctx, req.FetcherJobID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "get extraction job status", err)

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
		if err := uc.extractionRepo.Update(ctx, req); err != nil {
			return nil, fmt.Errorf("update extraction request: %w", err)
		}
	}

	return req, nil
}

// applyExtractionStatusTransition transitions the extraction request based on Fetcher's reported status.
//
//nolint:cyclop // extraction lifecycle is an explicit finite-state switch and reads clearest as one transition table.
func (uc *UseCase) applyExtractionStatusTransition(
	ctx context.Context,
	span trace.Span,
	req *entities.ExtractionRequest,
	status *sharedPorts.ExtractionJobStatus,
) (bool, error) {
	changed := false

	switch status.Status {
	case "RUNNING", "EXTRACTING":
		previousStatus := req.Status
		previousUpdatedAt := req.UpdatedAt

		if err := req.MarkExtracting(); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark extracting", err)

			return false, fmt.Errorf("mark extracting: %w", err)
		}

		changed = previousStatus != req.Status || !req.UpdatedAt.Equal(previousUpdatedAt)
	case "COMPLETE":
		previousStatus := req.Status
		previousResultPath := req.ResultPath
		previousErrorMessage := req.ErrorMessage
		previousUpdatedAt := req.UpdatedAt

		if err := req.MarkComplete(status.ResultPath); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark complete", err)

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

		if err := req.MarkFailed(status.ErrorMessage); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark failed", err)

			return false, fmt.Errorf("mark failed: %w", err)
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
