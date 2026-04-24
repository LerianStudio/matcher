// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package command provides command use cases for the ingestion service.
package command

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"

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
	jobRepo           ingestionRepositories.JobRepository
	transactionRepo   ingestionRepositories.TransactionRepository
	dedupe            ports.DedupeService
	dedupeTTL         time.Duration
	dedupeTTLResolver func(context.Context) time.Duration
	dedupeTTLGetter   func() time.Duration
	publisher         sharedPorts.IngestionEventPublisher
	outboxRepo        sharedPorts.OutboxRepository
	jobTxRunner       jobTxRunner
	jobRepoTx         jobTxUpdater
	txCleanupRepoTx   transactionCleanupTxUpdater
	outboxRepoTx      outboxTxCreator
	parsers           ports.ParserRegistry
	fieldMapRepo      ports.FieldMapRepository
	sourceRepo        ports.SourceRepository
	matchTrigger      sharedPorts.MatchTrigger
	contextProvider   sharedPorts.ContextProvider
}

// UseCaseDeps groups all dependencies required by the ingestion UseCase.
type UseCaseDeps struct {
	JobRepo           ingestionRepositories.JobRepository
	TransactionRepo   ingestionRepositories.TransactionRepository
	Dedupe            ports.DedupeService
	Publisher         sharedPorts.IngestionEventPublisher
	OutboxRepo        sharedPorts.OutboxRepository
	Parsers           ports.ParserRegistry
	FieldMapRepo      ports.FieldMapRepository
	SourceRepo        ports.SourceRepository
	DedupeTTL         time.Duration
	DedupeTTLResolver func(context.Context) time.Duration
	DedupeTTLGetter   func() time.Duration
	MatchTrigger      sharedPorts.MatchTrigger
	ContextProvider   sharedPorts.ContextProvider
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

	if sharedPorts.IsNilValue(deps.MatchTrigger) {
		deps.MatchTrigger = nil
	}

	if sharedPorts.IsNilValue(deps.ContextProvider) {
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
		jobRepo:           deps.JobRepo,
		transactionRepo:   deps.TransactionRepo,
		dedupe:            deps.Dedupe,
		dedupeTTL:         deps.DedupeTTL,
		dedupeTTLResolver: deps.DedupeTTLResolver,
		dedupeTTLGetter:   deps.DedupeTTLGetter,
		publisher:         deps.Publisher,
		outboxRepo:        deps.OutboxRepo,
		jobTxRunner:       jobTx,
		jobRepoTx:         jobRepoTx,
		txCleanupRepoTx:   txCleanupRepoTx,
		outboxRepoTx:      outboxRepoTx,
		parsers:           deps.Parsers,
		fieldMapRepo:      deps.FieldMapRepo,
		sourceRepo:        deps.SourceRepo,
		matchTrigger:      deps.MatchTrigger,
		contextProvider:   deps.ContextProvider,
	}, nil
}
