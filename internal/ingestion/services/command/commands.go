// Package command provides command use cases for the ingestion service.
package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	defaultDedupeRetries = 3
	defaultDedupeTTL     = time.Hour // Safety net TTL for dedup keys
	minDedupeTTL         = time.Minute

	// maxNonStreamingFileSize is the threshold above which a warning is logged
	// when using non-streaming parsing. Streaming parsers should be preferred
	// for files larger than this to avoid high memory consumption.
	maxNonStreamingFileSize int64 = 50 * 1024 * 1024 // 50 MB
)

var (
	// ErrNilJobRepository indicates job repository is nil.
	ErrNilJobRepository = errors.New("job repository is required")
	// ErrJobRepoNotTxRunner indicates job repository doesn't support transactions.
	ErrJobRepoNotTxRunner = errors.New("job repository does not support transactions")
	// ErrJobRepoNotTxUpdater indicates job repository doesn't support tx updates.
	ErrJobRepoNotTxUpdater = errors.New("job repository does not support transactional updates")
	// ErrNilTransactionRepository indicates transaction repository is nil.
	ErrNilTransactionRepository = errors.New("transaction repository is required")
	// ErrTransactionRepoNotCleanupTxUpdater indicates transaction repo doesn't support tx cleanup.
	ErrTransactionRepoNotCleanupTxUpdater = errors.New(
		"transaction repository does not support transactional cleanup updates",
	)
	// ErrNilDedupeService indicates dedupe service is nil.
	ErrNilDedupeService = errors.New("dedupe service is required")
	// ErrDedupeTTLTooShort indicates dedupe TTL is below the minimum.
	ErrDedupeTTLTooShort = errors.New("dedupe ttl must be at least one minute")
	// ErrNilEventPublisher indicates event publisher is nil.
	ErrNilEventPublisher = errors.New("event publisher is required")
	// ErrNilOutboxRepository indicates outbox repository is nil.
	ErrNilOutboxRepository = errors.New("outbox repository is required")
	// ErrOutboxRepoNotTxCreator indicates outbox repo doesn't support tx creates.
	ErrOutboxRepoNotTxCreator = errors.New(
		"outbox repository does not support transactional creates",
	)
	// ErrNilParserRegistry indicates parser registry is nil.
	ErrNilParserRegistry = errors.New("parser registry is required")
	// ErrNilFieldMapRepository indicates field map repository is nil.
	ErrNilFieldMapRepository = errors.New("field map repository is required")
	// ErrNilSourceRepository indicates source repository is nil.
	ErrNilSourceRepository = errors.New("source repository is required")
	// ErrNilUseCase indicates the use case is nil.
	ErrNilUseCase = errors.New("ingestion use case is required")
	// ErrFormatRequiredUC indicates format is required.
	ErrFormatRequiredUC = errors.New("format is required")
	// ErrSourceNotFound indicates the source was not found.
	ErrSourceNotFound = errors.New("source not found")
	// ErrFieldMapNotFound indicates the field map was not found.
	ErrFieldMapNotFound = errors.New("field map not found")
	// ErrTransactionNotFound indicates the transaction was not found.
	ErrTransactionNotFound = errors.New("transaction not found")
	// ErrTransactionNotIgnorable indicates the transaction cannot be ignored.
	ErrTransactionNotIgnorable = errors.New(
		"transaction cannot be ignored: only UNMATCHED transactions can be ignored",
	)
	// ErrReasonRequired indicates the reason is required.
	ErrReasonRequired = errors.New("reason is required")
	// ErrEmptyFile indicates the file contains no data rows.
	ErrEmptyFile = errors.New("file contains no data rows")
)

type jobTxRunner interface {
	WithTx(ctx context.Context, fn func(*sql.Tx) error) error
}

type jobTxUpdater interface {
	UpdateWithTx(
		ctx context.Context,
		tx *sql.Tx,
		job *entities.IngestionJob,
	) (*entities.IngestionJob, error)
}

type outboxTxCreator interface {
	CreateWithTx(
		ctx context.Context,
		tx *sql.Tx,
		event *shared.OutboxEvent,
	) (*shared.OutboxEvent, error)
}

type transactionCleanupTxUpdater interface {
	CleanupFailedJobTransactionsWithTx(ctx context.Context, tx *sql.Tx, jobID uuid.UUID) error
}

// UseCase implements ingestion command operations.
type UseCase struct {
	jobRepo         ingestionRepositories.JobRepository
	transactionRepo ingestionRepositories.TransactionRepository
	dedupe          ports.DedupeService
	dedupeTTL       time.Duration
	dedupeTTLGetter func() time.Duration
	publisher       sharedPorts.IngestionEventPublisher
	outboxRepo      sharedPorts.OutboxRepository
	jobTxRunner     jobTxRunner
	jobRepoTx       jobTxUpdater
	txCleanupRepoTx transactionCleanupTxUpdater
	outboxRepoTx    outboxTxCreator
	parsers         ports.ParserRegistry
	fieldMapRepo    ports.FieldMapRepository
	sourceRepo      ports.SourceRepository
	matchTrigger    sharedPorts.MatchTrigger
	contextProvider sharedPorts.ContextProvider
}

// UseCaseDeps groups all dependencies required by the ingestion UseCase.
type UseCaseDeps struct {
	JobRepo         ingestionRepositories.JobRepository
	TransactionRepo ingestionRepositories.TransactionRepository
	Dedupe          ports.DedupeService
	Publisher       sharedPorts.IngestionEventPublisher
	OutboxRepo      sharedPorts.OutboxRepository
	Parsers         ports.ParserRegistry
	FieldMapRepo    ports.FieldMapRepository
	SourceRepo      ports.SourceRepository
	DedupeTTL       time.Duration
	DedupeTTLGetter func() time.Duration
	MatchTrigger    sharedPorts.MatchTrigger
	ContextProvider sharedPorts.ContextProvider
}

func (deps *UseCaseDeps) validate() error {
	checks := []struct {
		cond bool
		err  error
	}{
		{deps.JobRepo == nil, ErrNilJobRepository},
		{deps.TransactionRepo == nil, ErrNilTransactionRepository},
		{deps.Dedupe == nil, ErrNilDedupeService},
		{deps.Publisher == nil, ErrNilEventPublisher},
		{deps.OutboxRepo == nil, ErrNilOutboxRepository},
		{deps.Parsers == nil, ErrNilParserRegistry},
		{deps.FieldMapRepo == nil, ErrNilFieldMapRepository},
		{deps.SourceRepo == nil, ErrNilSourceRepository},
	}

	for _, check := range checks {
		if check.cond {
			return check.err
		}
	}

	return nil
}

// NewUseCase creates a new command use case with required dependencies.
// defaultDedupeTTL is a safety net; callers should clear dedupe keys on completion.
func NewUseCase(deps UseCaseDeps) (*UseCase, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}

	if isNilInterface(deps.MatchTrigger) {
		deps.MatchTrigger = nil
	}

	if isNilInterface(deps.ContextProvider) {
		deps.ContextProvider = nil
	}

	if deps.DedupeTTL == 0 {
		deps.DedupeTTL = defaultDedupeTTL
	}

	if deps.DedupeTTL < minDedupeTTL {
		return nil, ErrDedupeTTLTooShort
	}

	jobTx, ok := deps.JobRepo.(jobTxRunner)
	if !ok {
		return nil, ErrJobRepoNotTxRunner
	}

	jobRepoTx, ok := deps.JobRepo.(jobTxUpdater)
	if !ok {
		return nil, ErrJobRepoNotTxUpdater
	}

	txCleanupRepoTx, ok := deps.TransactionRepo.(transactionCleanupTxUpdater)
	if !ok {
		return nil, ErrTransactionRepoNotCleanupTxUpdater
	}

	outboxRepoTx, ok := deps.OutboxRepo.(outboxTxCreator)
	if !ok {
		return nil, ErrOutboxRepoNotTxCreator
	}

	return &UseCase{
		jobRepo:         deps.JobRepo,
		transactionRepo: deps.TransactionRepo,
		dedupe:          deps.Dedupe,
		dedupeTTL:       deps.DedupeTTL,
		dedupeTTLGetter: deps.DedupeTTLGetter,
		publisher:       deps.Publisher,
		outboxRepo:      deps.OutboxRepo,
		jobTxRunner:     jobTx,
		jobRepoTx:       jobRepoTx,
		txCleanupRepoTx: txCleanupRepoTx,
		outboxRepoTx:    outboxRepoTx,
		parsers:         deps.Parsers,
		fieldMapRepo:    deps.FieldMapRepo,
		sourceRepo:      deps.SourceRepo,
		matchTrigger:    deps.MatchTrigger,
		contextProvider: deps.ContextProvider,
	}, nil
}

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
type StartIngestionInput struct {
	ContextID uuid.UUID
	SourceID  uuid.UUID
	FileName  string
	FileSize  int64
	Format    string
	Reader    io.Reader
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

	ctx, span := uc.startIngestionSpan(ctx, contextID, sourceID)
	if span != nil {
		defer span.End()
	}

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

// startIngestionSpan creates a tracing span for ingestion.
func (uc *UseCase) startIngestionSpan(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
) (context.Context, trace.Span) {
	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "command.ingestion.start_ingestion")

	if span != nil {
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
			nil,
		)
	}

	return ctx, span
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
			nil,
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
func (uc *UseCase) createAndStartJob(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
	fileName string,
	fileSize int64,
) (*entities.IngestionJob, error) {
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, fileName, fileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	if err := job.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start job: %w", err)
	}

	createdJob, err := uc.jobRepo.Create(ctx, job)
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	return createdJob, nil
}

// cleanupOnFailure clears dedup keys if processing failed.
func (uc *UseCase) cleanupOnFailure(ctx context.Context, state *ingestionState) {
	if state == nil || state.job == nil {
		return
	}

	if state.succeeded || len(state.markedHashes) == 0 {
		return
	}

	if clearErr := uc.dedupe.ClearBatch(ctx, state.job.ContextID, state.markedHashes); clearErr != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(libLog.Any("error", clearErr.Error())).Log(ctx, libLog.LevelWarn, "failed to clear dedup keys on failure")
	}
}

// clearDedupKeys removes dedup keys after successful ingestion.
// This allows legitimate re-uploads once the ingestion job completes.
// Errors are logged but do not affect the result since the job already succeeded.
func (uc *UseCase) clearDedupKeys(ctx context.Context, state *ingestionState) {
	if state == nil || state.job == nil || len(state.markedHashes) == 0 {
		return
	}

	if clearErr := uc.dedupe.ClearBatch(ctx, state.job.ContextID, state.markedHashes); clearErr != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(libLog.Any("error", clearErr.Error())).Log(ctx, libLog.LevelWarn, "failed to clear dedup keys after successful ingestion")
	}
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

// completeIngestionJob finalizes the job and publishes the completion event.
func (uc *UseCase) completeIngestionJob(
	ctx context.Context,
	state *ingestionState,
	span trace.Span,
) (*entities.IngestionJob, error) {
	if err := state.job.Complete(ctx, state.totalRows, state.totalErrors); err != nil {
		return nil, fmt.Errorf("failed to complete job: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"ingestion",
			struct {
				RecordsCount   int `json:"recordsCount"`
				RecordsValid   int `json:"recordsValid"`
				RecordsInvalid int `json:"recordsInvalid"`
			}{
				RecordsCount:   state.totalRows,
				RecordsValid:   state.totalRows - state.totalErrors,
				RecordsInvalid: state.totalErrors,
			},
			nil,
		)
	}

	return uc.persistCompletedJob(ctx, state)
}

// persistCompletedJob updates the job and creates the outbox event in a transaction.
func (uc *UseCase) persistCompletedJob(
	ctx context.Context,
	state *ingestionState,
) (*entities.IngestionJob, error) {
	var updatedJob *entities.IngestionJob

	err := uc.jobTxRunner.WithTx(ctx, func(tx *sql.Tx) error {
		updated, err := uc.jobRepoTx.UpdateWithTx(ctx, tx, state.job)
		if err != nil {
			return fmt.Errorf("failed to update job: %w", err)
		}

		effectiveDateRange := state.dateRange
		if effectiveDateRange == nil {
			effectiveDateRange = &ports.DateRange{Start: updated.StartedAt, End: updated.StartedAt}
		}

		event, err := entities.NewIngestionCompletedEvent(
			ctx,
			updated,
			state.totalInserted,
			effectiveDateRange.Start,
			effectiveDateRange.End,
			state.totalRows,
			state.totalErrors,
		)
		if err != nil {
			return fmt.Errorf("failed to create completed event: %w", err)
		}

		body, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		outboxEvent, err := shared.NewOutboxEvent(ctx, event.EventType, event.JobID, body)
		if err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		if _, err := uc.outboxRepoTx.CreateWithTx(ctx, tx, outboxEvent); err != nil {
			return fmt.Errorf("failed to create outbox entry: %w", err)
		}

		updatedJob = updated

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to complete ingestion: %w", err)
	}

	return updatedJob, nil
}

// filterAndInsertChunk performs bulk deduplication check and inserts a chunk of transactions.
// Returns the number of inserted transactions, marked hashes for cleanup, and any error.
func (uc *UseCase) filterAndInsertChunk(
	ctx context.Context,
	job *entities.IngestionJob,
	transactions []*shared.Transaction,
) (int, []string, error) {
	if len(transactions) == 0 {
		return 0, nil, nil
	}

	keys := make([]ingestionRepositories.ExternalIDKey, 0, len(transactions))
	for _, tx := range transactions {
		keys = append(keys, ingestionRepositories.ExternalIDKey{
			SourceID:   tx.SourceID,
			ExternalID: tx.ExternalID,
		})
	}

	existsMap, err := uc.transactionRepo.ExistsBulkBySourceAndExternalID(ctx, keys)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to check existing transactions: %w", err)
	}

	filtered := make([]*shared.Transaction, 0, len(transactions))
	markedHashes := make([]string, 0, len(transactions))

	for _, tx := range transactions {
		hash := uc.dedupe.CalculateHash(tx.SourceID, tx.ExternalID)
		if err := uc.dedupe.MarkSeenWithRetry(ctx, job.ContextID, hash, uc.currentDedupeTTL(), defaultDedupeRetries); err != nil {
			if errors.Is(err, ports.ErrDuplicateTransaction) {
				continue
			}

			return 0, markedHashes, err
		}

		markedHashes = append(markedHashes, hash)

		key := ingestionRepositories.ExternalIDKey{SourceID: tx.SourceID, ExternalID: tx.ExternalID}
		if existsMap[key] {
			continue
		}

		filtered = append(filtered, tx)
	}

	if len(filtered) > 0 {
		if _, err := uc.transactionRepo.CreateBatch(ctx, filtered); err != nil {
			return 0, markedHashes, fmt.Errorf("failed to insert transactions: %w", err)
		}
	}

	return len(filtered), markedHashes, nil
}

// IgnoreTransactionInput contains the data required to ignore a transaction.
type IgnoreTransactionInput struct {
	TransactionID uuid.UUID
	ContextID     uuid.UUID
	Reason        string
}

// IgnoreTransaction sets a transaction's status to IGNORED.
func (uc *UseCase) IgnoreTransaction(
	ctx context.Context,
	input IgnoreTransactionInput,
) (*shared.Transaction, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "command.ingestion.ignore_transaction")

	if span != nil {
		defer span.End()

		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"ignore_transaction",
			struct {
				TransactionID string `json:"transactionId"`
				ContextID     string `json:"contextId"`
				Reason        string `json:"reason"`
			}{
				TransactionID: input.TransactionID.String(),
				ContextID:     input.ContextID.String(),
				Reason:        input.Reason,
			},
			nil,
		)
	}

	if strings.TrimSpace(input.Reason) == "" {
		return nil, ErrReasonRequired
	}

	existing, err := uc.transactionRepo.FindByID(ctx, input.TransactionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTransactionNotFound
		}

		return nil, fmt.Errorf("failed to find transaction: %w", err)
	}

	if existing == nil {
		return nil, ErrTransactionNotFound
	}

	if existing.Status != shared.TransactionStatusUnmatched {
		return nil, ErrTransactionNotIgnorable
	}

	updated, err := uc.transactionRepo.UpdateStatus(
		ctx,
		input.TransactionID,
		input.ContextID,
		shared.TransactionStatusIgnored,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTransactionNotFound
		}

		return nil, fmt.Errorf("failed to update transaction status: %w", err)
	}

	return updated, nil
}

func (uc *UseCase) failJob(
	ctx context.Context, //nolint:contextcheck // cleanup function intentionally uses context.WithoutCancel to outlive parent cancellation
	job *entities.IngestionJob,
	cause error,
	markedHashes []string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	persistCtx := context.WithoutCancel(ctx)

	logger, _, _, _ := libCommons.NewTrackingFromContext(persistCtx) //nolint:dogsled // only logger needed

	// Clean up dedup keys to allow retries
	if job != nil && len(markedHashes) > 0 {
		if clearErr := uc.dedupe.ClearBatch(persistCtx, job.ContextID, markedHashes); clearErr != nil {
			logger.With(libLog.Any("error", clearErr.Error())).Log(persistCtx, libLog.LevelWarn, "failed to clear dedup keys on job failure")
		}
	}

	if job == nil {
		return cause
	}

	if err := job.Fail(persistCtx, cause.Error()); err != nil {
		return fmt.Errorf("failed to transition job to failed: %w", err)
	}

	err := uc.jobTxRunner.WithTx(persistCtx, func(tx *sql.Tx) error {
		updated, err := uc.jobRepoTx.UpdateWithTx(persistCtx, tx, job)
		if err != nil {
			return fmt.Errorf("failed to update job after error: %w", err)
		}

		event, err := entities.NewIngestionFailedEvent(persistCtx, updated)
		if err != nil {
			return fmt.Errorf("failed to create failed event: %w", err)
		}

		body, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal ingestion failed event: %w", err)
		}

		outboxEvent, err := shared.NewOutboxEvent(persistCtx, event.EventType, event.JobID, body)
		if err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		if _, err := uc.outboxRepoTx.CreateWithTx(persistCtx, tx, outboxEvent); err != nil {
			return fmt.Errorf("failed to enqueue ingestion failure: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to save failed job: %w", err)
	}

	// Cleanup runs in a separate best-effort transaction so a cleanup SQL failure
	// cannot poison the primary transaction that persists FAILED status + outbox event.
	uc.cleanupPartialTransactionsBestEffort(persistCtx, job.ID)

	return cause
}

func (uc *UseCase) cleanupPartialTransactionsBestEffort(ctx context.Context, jobID uuid.UUID) {
	if uc == nil || uc.jobTxRunner == nil || uc.txCleanupRepoTx == nil {
		return
	}

	if ctx == nil {
		ctx = context.Background() //nolint:contextcheck // best-effort cleanup intentionally uses fresh context when nil
	}

	logger, tracer, _, headerID := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.ingestion.cleanup_partial_transactions")
	defer span.End()

	if err := uc.jobTxRunner.WithTx(ctx, func(tx *sql.Tx) error {
		if err := uc.txCleanupRepoTx.CleanupFailedJobTransactionsWithTx(ctx, tx, jobID); err != nil {
			return fmt.Errorf("cleanup failed job transactions: %w", err)
		}

		return nil
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to execute best-effort partial transaction cleanup", err)

		if logger != nil {
			logger.With(
				libLog.String("job_id", jobID.String()),
				libLog.Any("header_id", headerID),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "failed to execute best-effort partial transaction cleanup")
		}
	}
}

// triggerAutoMatchIfEnabled checks whether auto-match on upload is enabled
// for the context and triggers an asynchronous match run if so.
// This is fire-and-forget; errors are logged but do not affect ingestion.
func (uc *UseCase) triggerAutoMatchIfEnabled(ctx context.Context, contextID uuid.UUID) {
	if isNilInterface(uc.contextProvider) || isNilInterface(uc.matchTrigger) {
		return
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed

	ctx, span := tracer.Start(ctx, "command.ingestion.trigger_auto_match")
	defer span.End()

	enabled, err := uc.contextProvider.IsAutoMatchEnabled(ctx, contextID)
	if err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(
			libLog.String("context_id", contextID.String()),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "failed to check auto-match status")

		return
	}

	if !enabled {
		return
	}

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(
			libLog.String("tenant_id", tenantIDStr),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "auto-match skipped: invalid tenant ID")

		return
	}

	uc.matchTrigger.TriggerMatchForContext(ctx, tenantID, contextID)
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

// maxErrorDetails limits how many row-level errors are included in metadata.
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

func (uc *UseCase) currentDedupeTTL() time.Duration {
	if uc == nil {
		return 0
	}

	if uc.dedupeTTLGetter != nil {
		if ttl := uc.dedupeTTLGetter(); ttl > 0 {
			return ttl
		}
	}

	return uc.dedupeTTL
}
