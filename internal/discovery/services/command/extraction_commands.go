package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

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
		libOpentelemetry.HandleSpanError(span, "validate extraction request", err)

		return nil, err
	}

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

	if conn.Status != vo.ConnectionStatusAvailable || !conn.SchemaDiscovered {
		err = fmt.Errorf("%w: connection schema is not available for extraction", ErrInvalidExtractionRequest)
		libOpentelemetry.HandleSpanError(span, "connection not ready for extraction", err)

		return nil, err
	}

	schemas, err := uc.schemaRepo.FindByConnectionID(ctx, connectionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "find connection schema", err)

		return nil, fmt.Errorf("find connection schema: %w", err)
	}

	if err := validateExtractionScope(tables, schemas); err != nil {
		libOpentelemetry.HandleSpanError(span, "validate extraction scope", err)

		return nil, err
	}

	extractionReq, err := entities.NewExtractionRequest(ctx, connectionID, tables, params.StartDate, params.EndDate, params.Filters.ToMap())
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
		tableConfig, configErr := buildTableConfig(cfg, params)
		if configErr != nil {
			return nil, configErr
		}

		input.Tables[name] = tableConfig
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

// buildTableConfig normalizes a single table's configuration from raw JSON-deserialized data.
func buildTableConfig(cfg any, params sharedPorts.ExtractionParams) (sharedPorts.ExtractionTableConfig, error) {
	tc := sharedPorts.ExtractionTableConfig{
		StartDate: params.StartDate,
		EndDate:   params.EndDate,
	}

	columns, err := extractRequestedColumns(cfg)
	if err != nil {
		return sharedPorts.ExtractionTableConfig{}, err
	}

	if len(columns) > 0 {
		tc.Columns = columns
	}

	return tc, nil
}

const extractionDateLayout = "2006-01-02"

func validateExtractionRequest(tables map[string]any, params sharedPorts.ExtractionParams) error {
	if len(tables) == 0 {
		return fmt.Errorf("%w: at least one table is required", ErrInvalidExtractionRequest)
	}

	startDate, err := parseExtractionDate("start date", params.StartDate)
	if err != nil {
		return err
	}

	endDate, err := parseExtractionDate("end date", params.EndDate)
	if err != nil {
		return err
	}

	if !startDate.IsZero() && !endDate.IsZero() && endDate.Before(startDate) {
		return fmt.Errorf("%w: end date must be on or after start date", ErrInvalidExtractionRequest)
	}

	for tableName, cfg := range tables {
		if strings.TrimSpace(tableName) == "" {
			return fmt.Errorf("%w: table name is required", ErrInvalidExtractionRequest)
		}

		if _, err := extractRequestedColumns(cfg); err != nil {
			return err
		}
	}

	return nil
}

func parseExtractionDate(label, raw string) (time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, nil
	}

	parsed, err := time.Parse(extractionDateLayout, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must use YYYY-MM-DD format", ErrInvalidExtractionRequest, label)
	}

	return parsed, nil
}

func validateExtractionScope(tables map[string]any, schemas []*entities.DiscoveredSchema) error {
	if len(schemas) == 0 {
		return fmt.Errorf("%w: schema has not been discovered for this connection", ErrInvalidExtractionRequest)
	}

	allowedTables := make(map[string]map[string]struct{}, len(schemas))
	for _, schema := range schemas {
		if schema == nil || strings.TrimSpace(schema.TableName) == "" {
			continue
		}

		columns := make(map[string]struct{}, len(schema.Columns))
		for _, column := range schema.Columns {
			if strings.TrimSpace(column.Name) == "" {
				continue
			}

			columns[column.Name] = struct{}{}
		}

		allowedTables[schema.TableName] = columns
	}

	for tableName, cfg := range tables {
		allowedColumns, ok := allowedTables[tableName]
		if !ok {
			return fmt.Errorf("%w: unknown table %q", ErrInvalidExtractionRequest, tableName)
		}

		columns, err := extractRequestedColumns(cfg)
		if err != nil {
			return err
		}

		for _, column := range columns {
			if _, exists := allowedColumns[column]; !exists {
				return fmt.Errorf("%w: unknown column %q for table %q", ErrInvalidExtractionRequest, column, tableName)
			}
		}
	}

	return nil
}

func extractRequestedColumns(cfg any) ([]string, error) {
	if cfg == nil {
		return nil, nil
	}

	cfgMap, ok := cfg.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: table configuration must be an object", ErrInvalidExtractionRequest)
	}

	for key := range cfgMap {
		if key != "columns" {
			return nil, fmt.Errorf("%w: unsupported table configuration key %q", ErrInvalidExtractionRequest, key)
		}
	}

	cols, ok := cfgMap["columns"]
	if !ok {
		return nil, nil
	}

	switch typed := cols.(type) {
	case []string:
		return validateRequestedColumns(typed)
	case []any:
		stringCols := make([]string, 0, len(typed))
		for _, raw := range typed {
			colName, isString := raw.(string)
			if !isString {
				return nil, fmt.Errorf("%w: columns must be strings", ErrInvalidExtractionRequest)
			}

			stringCols = append(stringCols, colName)
		}

		return validateRequestedColumns(stringCols)
	default:
		return nil, fmt.Errorf("%w: columns must be an array of strings", ErrInvalidExtractionRequest)
	}
}

func validateRequestedColumns(columns []string) ([]string, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: columns must not be empty", ErrInvalidExtractionRequest)
	}

	normalized := make([]string, 0, len(columns))

	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		trimmed := strings.TrimSpace(column)
		if trimmed == "" {
			return nil, fmt.Errorf("%w: columns must not contain blanks", ErrInvalidExtractionRequest)
		}

		if _, exists := seen[trimmed]; exists {
			continue
		}

		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	return normalized, nil
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

		if err := req.MarkFailed(entities.SanitizedExtractionFailureMessage); err != nil {
			libOpentelemetry.HandleSpanError(span, "mark failed", err)

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
			libOpentelemetry.HandleSpanError(span, "mark cancelled", err)

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

func (uc *UseCase) persistSubmittedExtraction(
	ctx context.Context,
	span trace.Span,
	extractionReq *entities.ExtractionRequest,
) error {
	err := uc.extractionRepo.Update(ctx, extractionReq)
	if err == nil {
		return nil
	}

	libOpentelemetry.HandleSpanError(span, "persist submitted extraction request", err)

	recovered, recoverErr := uc.recoverSubmittedExtraction(ctx, extractionReq)
	if recoverErr == nil {
		if recovered != nil && recovered != extractionReq {
			*extractionReq = *recovered
		}

		return nil
	}

	libOpentelemetry.HandleSpanError(span, "recover submitted extraction request", recoverErr)

	return fmt.Errorf("%w: extraction %s: %w", ErrExtractionTrackingIncomplete, extractionReq.ID, recoverErr)
}

func (uc *UseCase) recoverSubmittedExtraction(
	ctx context.Context,
	submitted *entities.ExtractionRequest,
) (*entities.ExtractionRequest, error) {
	latest, err := uc.extractionRepo.FindByID(ctx, submitted.ID)
	if err != nil {
		if errors.Is(err, repositories.ErrExtractionNotFound) {
			return nil, ErrExtractionNotFound
		}

		return nil, fmt.Errorf("reload extraction request: %w", err)
	}

	if latest == nil {
		return nil, ErrExtractionNotFound
	}

	if latest.Status == submitted.Status && latest.FetcherJobID == submitted.FetcherJobID {
		return latest, nil
	}

	if strings.TrimSpace(latest.FetcherJobID) != "" {
		return latest, nil
	}

	expectedUpdatedAt := latest.UpdatedAt
	if expectedUpdatedAt.IsZero() {
		expectedUpdatedAt = submitted.CreatedAt
	}

	err = uc.extractionRepo.UpdateIfUnchanged(ctx, submitted, expectedUpdatedAt)
	if err == nil {
		return submitted, nil
	}

	if !errors.Is(err, repositories.ErrExtractionConflict) {
		return nil, fmt.Errorf("repair submitted extraction request: %w", err)
	}

	return uc.reloadRecoveredExtraction(ctx, submitted.ID)
}

func (uc *UseCase) reloadRecoveredExtraction(ctx context.Context, extractionID uuid.UUID) (*entities.ExtractionRequest, error) {
	reloaded, err := uc.extractionRepo.FindByID(ctx, extractionID)
	if err != nil {
		if errors.Is(err, repositories.ErrExtractionNotFound) {
			return nil, ErrExtractionNotFound
		}

		return nil, fmt.Errorf("reload extraction request after repair conflict: %w", err)
	}

	if reloaded == nil {
		return nil, ErrExtractionNotFound
	}

	if strings.TrimSpace(reloaded.FetcherJobID) == "" {
		return nil, fmt.Errorf("repair submitted extraction request: %w", repositories.ErrExtractionConflict)
	}

	return reloaded, nil
}
