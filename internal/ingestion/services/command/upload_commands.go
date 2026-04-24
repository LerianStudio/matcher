// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedObservability "github.com/LerianStudio/matcher/internal/shared/observability"
)

// ingestionState tracks state during ingestion processing.
type ingestionState struct {
	job           *entities.IngestionJob
	fieldMap      *shared.FieldMap
	parser        ports.Parser
	reader        io.Reader
	markedHashes  []string
	totalInserted int
	totalRows     int
	totalErrors   int
	dateRange     *ports.DateRange
	succeeded     bool
	firstErrors   []ports.ParseError
}

// StartIngestionInput contains the data required to start an ingestion.
//
// ExtractionID is the optional originating Fetcher extraction id (T-005 P1 +
// Polish Fix 4). When non-empty, the ingestion job's metadata is stamped with
// this id atomically as part of the initial INSERT — closing the orphan-job
// window where a follow-up Update could fail and leave a stamp-less job.
// Empty string is the upload path (no bridge involved).
type StartIngestionInput struct {
	ContextID    uuid.UUID
	SourceID     uuid.UUID
	FileName     string
	FileSize     int64
	Format       string
	Reader       io.Reader
	ExtractionID string
}

// StartIngestion begins the ingestion process for a file.
func (uc *UseCase) StartIngestion(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
	fileName string,
	fileSize int64,
	format string,
	reader io.Reader,
) (*entities.IngestionJob, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed at this boundary — logger is
	// pulled again deeper when the first error condition fires.
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.ingestion.start_ingestion")
	defer span.End()

	uc.annotateIngestionSpan(span, contextID, sourceID)

	input := StartIngestionInput{
		ContextID: contextID,
		SourceID:  sourceID,
		FileName:  fileName,
		FileSize:  fileSize,
		Format:    format,
		Reader:    reader,
	}

	state, err := uc.prepareIngestion(ctx, input, span)
	if err != nil {
		return nil, err
	}

	defer uc.cleanupOnFailure(ctx, state)

	if err := uc.processIngestionFile(ctx, state); err != nil {
		return nil, uc.failJob(ctx, state.job, err, state.markedHashes)
	}

	state.succeeded = true

	completedJob, err := uc.completeIngestionJob(ctx, state, span)
	if err != nil {
		return nil, err
	}

	// Clear dedup keys after successful ingestion to allow legitimate re-uploads.
	// The TTL-based expiry is only a safety net; explicit cleanup is preferred.
	uc.clearDedupKeys(ctx, state)

	uc.triggerAutoMatchIfEnabled(ctx, input.ContextID)

	return completedJob, nil
}

// annotateIngestionSpan records context/source attributes on an existing
// ingestion span. Extracted so StartIngestion's span lifecycle
// (NewTrackingFromContext → tracer.Start → defer span.End) stays inline at
// the method boundary, which both improves readability and lets the
// observability linter verify the pattern structurally.
func (uc *UseCase) annotateIngestionSpan(
	span trace.Span,
	contextID, sourceID uuid.UUID,
) {
	if span == nil {
		return
	}

	_ = libOpentelemetry.SetSpanAttributesFromValue(
		span,
		"ingestion",
		struct {
			ContextID string `json:"contextId"`
			SourceID  string `json:"sourceId"`
		}{
			ContextID: contextID.String(),
			SourceID:  sourceID.String(),
		},
		sharedObservability.NewMatcherRedactor(),
	)
}

// prepareIngestion validates input and loads dependencies for ingestion.
func (uc *UseCase) prepareIngestion(
	ctx context.Context,
	input StartIngestionInput,
	span trace.Span,
) (*ingestionState, error) {
	format := strings.ToLower(strings.TrimSpace(input.Format))
	if format == "" {
		return nil, ErrFormatRequiredUC
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"ingestion",
			struct {
				FileFormat string `json:"fileFormat"`
			}{
				FileFormat: format,
			},
			sharedObservability.NewMatcherRedactor(),
		)
	}

	fieldMap, err := uc.loadFieldMap(ctx, input.ContextID, input.SourceID)
	if err != nil {
		return nil, err
	}

	job, err := uc.createAndStartJob(
		ctx,
		input.ContextID,
		input.SourceID,
		input.FileName,
		input.FileSize,
		input.ExtractionID,
	)
	if err != nil {
		return nil, err
	}

	parser, err := uc.parsers.GetParser(format)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	return &ingestionState{
		job:      job,
		fieldMap: fieldMap,
		parser:   parser,
		reader:   input.Reader,
	}, nil
}

// loadFieldMap loads and validates the source and field map.
func (uc *UseCase) loadFieldMap(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	source, err := uc.sourceRepo.FindByID(ctx, contextID, sourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSourceNotFound
		}

		return nil, fmt.Errorf("failed to load source: %w", err)
	}

	if source == nil {
		return nil, ErrSourceNotFound
	}

	fieldMap, err := uc.fieldMapRepo.FindBySourceID(ctx, sourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrFieldMapNotFound
		}

		return nil, fmt.Errorf("failed to load field map: %w", err)
	}

	if fieldMap == nil {
		return nil, ErrFieldMapNotFound
	}

	return fieldMap, nil
}

// createAndStartJob creates a new ingestion job and starts it.
//
// extractionID, when non-empty, is stamped onto the job's metadata BEFORE the
// Create round-trip so the INSERT carries the extraction-id link atomically
// (Polish Fix 4). Stamping atomically (not via a follow-up Update) closes the
// orphan-job window where a transient Update failure on Tick 1 would cause
// Tick 2's FindLatestByExtractionID to miss and create a duplicate job.
//
// Empty extractionID is the upload path — the metadata is left clean and the
// JSONB index's partial WHERE predicate excludes the row.
func (uc *UseCase) createAndStartJob(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
	fileName string,
	fileSize int64,
	extractionID string,
) (*entities.IngestionJob, error) {
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, fileName, fileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	if err := job.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start job: %w", err)
	}

	// Polish Fix 4: stamp extraction id atomically with the INSERT.
	if extractionID != "" {
		stampExtractionIDOnJob(job, extractionID)
	}

	createdJob, err := uc.jobRepo.Create(ctx, job)
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	return createdJob, nil
}

// processIngestionFile parses the file and inserts transactions.
func (uc *UseCase) processIngestionFile(ctx context.Context, state *ingestionState) error {
	streamingParser, isStreaming := state.parser.(ports.StreamingParser)
	if isStreaming {
		return uc.processStreaming(ctx, state, streamingParser)
	}

	return uc.processNonStreaming(ctx, state)
}

// processStreaming handles streaming parser execution.
func (uc *UseCase) processStreaming(
	ctx context.Context,
	state *ingestionState,
	parser ports.StreamingParser,
) error {
	result, err := parser.ParseStreaming(
		ctx,
		state.reader,
		state.job,
		state.fieldMap,
		ports.DefaultChunkSize,
		func(chunk []*shared.Transaction, chunkErrors []ports.ParseError) error {
			inserted, markedHashes, err := uc.filterAndInsertChunk(ctx, state.job, chunk)
			if err != nil {
				return err
			}

			state.markedHashes = append(state.markedHashes, markedHashes...)
			state.totalInserted += inserted
			state.totalErrors += len(chunkErrors)

			return nil
		},
	)
	if err != nil {
		return err
	}

	state.totalRows = result.TotalRecords + result.TotalErrors
	state.dateRange = result.DateRange
	state.firstErrors = result.FirstBatchErrs

	if state.totalRows == 0 {
		return ErrEmptyFile
	}

	if result.TotalErrors > 0 {
		state.job.Metadata.Error = fmt.Sprintf("%d rows failed validation", result.TotalErrors)
		state.job.Metadata.ErrorDetails = convertParseErrors(result.FirstBatchErrs)
	}

	return nil
}

// processNonStreaming handles non-streaming parser execution.
// For large files, streaming parsers are preferred to avoid loading all
// transactions into memory at once. This method logs a warning if the file
// exceeds maxNonStreamingFileSize but does not reject it.
func (uc *UseCase) processNonStreaming(ctx context.Context, state *ingestionState) error {
	if state.job != nil && state.job.Metadata.FileSize > maxNonStreamingFileSize {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(
			libLog.Any("file_size_bytes", state.job.Metadata.FileSize),
			libLog.Any("threshold_bytes", maxNonStreamingFileSize),
		).Log(ctx, libLog.LevelWarn, "non-streaming parser processing large file; consider using a streaming parser to reduce memory usage")
	}

	result, err := state.parser.Parse(ctx, state.reader, state.job, state.fieldMap)
	if err != nil {
		return err
	}

	inserted, markedHashes, err := uc.filterAndInsertChunk(ctx, state.job, result.Transactions)
	if err != nil {
		state.markedHashes = markedHashes

		return err
	}

	state.markedHashes = markedHashes
	state.totalInserted = inserted
	state.totalRows = len(result.Transactions) + len(result.Errors)
	state.totalErrors = len(result.Errors)
	state.dateRange = result.DateRange
	state.firstErrors = result.Errors

	if state.totalRows == 0 {
		return ErrEmptyFile
	}

	if len(result.Errors) > 0 {
		state.job.Metadata.Error = fmt.Sprintf("%d rows failed validation", len(result.Errors))
		state.job.Metadata.ErrorDetails = convertParseErrors(result.Errors)
	}

	return nil
}

const maxErrorDetails = 50

// convertParseErrors converts ports.ParseError to entities.RowError for metadata storage.
// Limited to maxErrorDetails to prevent bloating the JSONB column.
func convertParseErrors(errs []ports.ParseError) []entities.RowError {
	if len(errs) == 0 {
		return nil
	}

	limit := min(len(errs), maxErrorDetails)

	result := make([]entities.RowError, limit)
	for i := range limit {
		result[i] = entities.RowError{
			Row:     errs[i].Row,
			Field:   errs[i].Field,
			Message: errs[i].Message,
		}
	}

	return result
}
