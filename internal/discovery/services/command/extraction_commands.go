package command

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// StartExtraction submits a data extraction job to Fetcher.
// It creates an ExtractionRequest record to track the job lifecycle and delegates
// the actual extraction to the Fetcher service via the FetcherClient port.
// The ingestionJobID is not required; extraction requests can exist independently
// and be linked to ingestion jobs later when the data is imported.
func (uc *UseCase) StartExtraction(
	ctx context.Context,
	fetcherConnID string,
	tables map[string]any,
	params sharedPorts.ExtractionParams,
) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.start_extraction")
	defer span.End()

	if !uc.fetcherClient.IsHealthy(ctx) {
		libOpentelemetry.HandleSpanError(span, "fetcher unavailable", ErrFetcherUnavailable)

		return ErrFetcherUnavailable
	}

	// Build the extraction job input from params.
	input := sharedPorts.ExtractionJobInput{
		ConnectionID: fetcherConnID,
		Filters:      params.Filters,
		Tables:       make(map[string]sharedPorts.ExtractionTableConfig, len(tables)),
	}

	for name, cfg := range tables {
		input.Tables[name] = buildTableConfig(cfg, params)
	}

	jobID, err := uc.fetcherClient.SubmitExtractionJob(ctx, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "submit extraction job", err)

		return fmt.Errorf("submit extraction job: %w", err)
	}

	// Create extraction request to track the job.
	// IngestionJobID is not set at creation; it will be linked later when data is imported.
	extractionReq, err := entities.NewExtractionRequest(ctx, fetcherConnID, tables)
	if err != nil {
		return fmt.Errorf("create extraction request: %w", err)
	}

	if params.Filters != nil {
		extractionReq.Filters = params.Filters
	}

	if err := extractionReq.MarkSubmitted(jobID); err != nil {
		return fmt.Errorf("mark extraction submitted: %w", err)
	}

	if err := uc.extractionRepo.Create(ctx, extractionReq); err != nil {
		return fmt.Errorf("persist extraction request: %w", err)
	}

	// Start async polling for extraction completion (if poller configured).
	if uc.extractionPoller != nil {
		// Polling must outlive the originating HTTP request context, otherwise
		// client cancellation aborts extraction tracking mid-flight.
		uc.extractionPoller.PollUntilComplete(context.WithoutCancel(ctx), extractionReq, nil, nil)
	}

	return nil
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
func (uc *UseCase) PollExtractionStatus(ctx context.Context, extractionID uuid.UUID) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.poll_extraction_status")
	defer span.End()

	req, err := uc.extractionRepo.FindByID(ctx, extractionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "find extraction request", err)

		return fmt.Errorf("find extraction request: %w", err)
	}

	if req == nil {
		libOpentelemetry.HandleSpanError(span, "nil extraction request", ErrExtractionNotFound)

		return ErrExtractionNotFound
	}

	if req.Status.IsTerminal() {
		return nil // already complete or failed
	}

	status, err := uc.fetcherClient.GetExtractionJobStatus(ctx, req.FetcherJobID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "get extraction job status", err)

		return fmt.Errorf("get extraction job status: %w", err)
	}

	if status == nil {
		libOpentelemetry.HandleSpanError(span, "nil extraction status", ErrNilExtractionStatus)

		return ErrNilExtractionStatus
	}

	if err := uc.applyExtractionStatusTransition(ctx, span, req, status); err != nil {
		return err
	}

	if err := uc.extractionRepo.Update(ctx, req); err != nil {
		return fmt.Errorf("update extraction request: %w", err)
	}

	return nil
}

// applyExtractionStatusTransition transitions the extraction request based on Fetcher's reported status.
func (uc *UseCase) applyExtractionStatusTransition(
	ctx context.Context,
	span trace.Span,
	req *entities.ExtractionRequest,
	status *sharedPorts.ExtractionJobStatus,
) error {
	switch status.Status {
	case "RUNNING", "EXTRACTING":
		if err := req.MarkExtracting(); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark extracting", err)

			return fmt.Errorf("mark extracting: %w", err)
		}
	case "COMPLETE":
		if err := req.MarkComplete(status.ResultPath); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark complete", err)

			return fmt.Errorf("mark complete: %w", err)
		}
	case "FAILED":
		if err := req.MarkFailed(status.ErrorMessage); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark failed", err)

			return fmt.Errorf("mark failed: %w", err)
		}
	default:
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("unknown extraction status %q for job %s", status.Status, req.FetcherJobID))
	}

	return nil
}
