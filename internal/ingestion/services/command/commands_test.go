//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errOutbox is a sentinel error for outbox failures.
var errOutbox = errors.New("outbox")

// errParse is a sentinel error for parse failures.
var errParse = errors.New("parse")

// errDatabaseConnection is a sentinel error for database connection failures.
var errDatabaseConnection = errors.New("database connection failed")

// errFieldMapQuery is a sentinel error for field map query failures.
var errFieldMapQuery = errors.New("field map query failed")

type fakeRegistry struct{ parser ports.Parser }

func (f fakeRegistry) GetParser(_ string) (ports.Parser, error) { return f.parser, nil }
func (f fakeRegistry) Register(_ ports.Parser)                  {}

type fakeDedupe struct{ err error }

func (f fakeDedupe) CalculateHash(_ uuid.UUID, _ string) string { return "hash" }
func (f fakeDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return false, nil
}

func (f fakeDedupe) MarkSeen(_ context.Context, _ uuid.UUID, _ string, _ time.Duration) error {
	return nil
}

func (f fakeDedupe) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ time.Duration,
	_ int,
) error {
	return f.err
}

func (f fakeDedupe) Clear(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (f fakeDedupe) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}

type recordingDedupe struct{ lastTTL time.Duration }

func (r *recordingDedupe) CalculateHash(_ uuid.UUID, _ string) string { return "hash" }
func (r *recordingDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return false, nil
}

func (r *recordingDedupe) MarkSeen(_ context.Context, _ uuid.UUID, _ string, ttl time.Duration) error {
	r.lastTTL = ttl
	return nil
}

func (r *recordingDedupe) MarkSeenWithRetry(_ context.Context, _ uuid.UUID, _ string, ttl time.Duration, _ int) error {
	r.lastTTL = ttl
	return nil
}

func (r *recordingDedupe) Clear(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (r *recordingDedupe) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}

func TestUseCase_CurrentDedupeTTL_PrefersResolver(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		dedupeTTL:         time.Minute,
		dedupeTTLResolver: func(context.Context) time.Duration { return 2 * time.Minute },
	}

	require.Equal(t, 2*time.Minute, uc.currentDedupeTTL(context.Background()))
}

func TestUseCase_FilterAndInsertChunk_UsesResolverTTLForDedupeMarking(t *testing.T) {
	t.Parallel()

	dedupe := &recordingDedupe{}
	uc := &UseCase{
		transactionRepo:   fakeTxRepo{},
		dedupe:            dedupe,
		dedupeTTL:         time.Minute,
		dedupeTTLResolver: func(context.Context) time.Duration { return 2 * time.Minute },
	}

	job := &entities.IngestionJob{ContextID: uuid.New()}
	transactions := []*shared.Transaction{{SourceID: uuid.New(), ExternalID: "ext-1"}}

	inserted, markedHashes, err := uc.filterAndInsertChunk(context.Background(), job, transactions)
	require.NoError(t, err)
	require.Equal(t, 1, inserted)
	require.Len(t, markedHashes, 1)
	require.Equal(t, 2*time.Minute, dedupe.lastTTL)
}

type fakePublisher struct{ called bool }

func (f *fakePublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *entities.IngestionCompletedEvent,
) error {
	f.called = true

	return nil
}

func (f *fakePublisher) PublishIngestionFailed(
	_ context.Context,
	_ *entities.IngestionFailedEvent,
) error {
	f.called = true

	return nil
}

type fakeJobRepo struct {
	createErr            error
	created              *entities.IngestionJob
	withTxErr            error
	updateErr            error
	updated              *entities.IngestionJob
	byExtraction         map[string]*entities.IngestionJob
	findByExtractionErr  error
	findByExtractionCall int
}

func (jobRepo *fakeJobRepo) Create(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return jobRepo.created, jobRepo.createErr
}

func (jobRepo *fakeJobRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (jobRepo *fakeJobRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (jobRepo *fakeJobRepo) Update(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (jobRepo *fakeJobRepo) FindLatestByExtractionID(
	_ context.Context,
	extractionID uuid.UUID,
) (*entities.IngestionJob, error) {
	jobRepo.findByExtractionCall++

	if jobRepo.findByExtractionErr != nil {
		return nil, jobRepo.findByExtractionErr
	}

	if jobRepo.byExtraction == nil {
		return nil, nil
	}

	if existing, ok := jobRepo.byExtraction[extractionID.String()]; ok {
		return existing, nil
	}

	return nil, nil
}

func (jobRepo *fakeJobRepo) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if jobRepo.withTxErr != nil {
		return jobRepo.withTxErr
	}

	return fn(&sql.Tx{})
}

func (jobRepo *fakeJobRepo) UpdateWithTx(
	_ context.Context,
	_ *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	if jobRepo.updateErr != nil {
		return nil, jobRepo.updateErr
	}

	if jobRepo.updated != nil {
		return jobRepo.updated, nil
	}

	return job, nil
}

type fakeOutboxRepo struct {
	createErr error
	created   *shared.OutboxEvent
}

func (f *fakeOutboxRepo) Create(
	_ context.Context,
	_ *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return f.created, f.createErr
}

func (f *fakeOutboxRepo) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return f.created, f.createErr
}

func (f *fakeOutboxRepo) ListPending(
	_ context.Context,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.OutboxEvent, error) {
	return f.created, f.createErr
}

func (f *fakeOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (f *fakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	return nil
}

func (f *fakeOutboxRepo) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

type fakeTxRepo struct {
	exists   bool
	batchErr error
}

func (t fakeTxRepo) Create(_ context.Context, _ *shared.Transaction) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) CreateBatch(
	_ context.Context,
	_ []*shared.Transaction,
) ([]*shared.Transaction, error) {
	return nil, t.batchErr
}

func (t fakeTxRepo) FindByID(_ context.Context, _ uuid.UUID) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) FindByJobID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (t fakeTxRepo) FindByJobAndContextID(
	_ context.Context,
	_, _ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (t fakeTxRepo) FindBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) ExistsBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (bool, error) {
	return t.exists, nil
}

func (t fakeTxRepo) ExistsBulkBySourceAndExternalID(
	_ context.Context,
	_ []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	return make(map[repositories.ExternalIDKey]bool), nil
}

func (t fakeTxRepo) UpdateStatus(
	_ context.Context,
	_, _ uuid.UUID,
	_ shared.TransactionStatus,
) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) SearchTransactions(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.TransactionSearchParams,
) ([]*shared.Transaction, int64, error) {
	return nil, 0, nil
}

func (t fakeTxRepo) CleanupFailedJobTransactionsWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
) error {
	return nil
}

type fakeFieldMapRepo struct {
	fieldMap *shared.FieldMap
	err      error
}

func (f *fakeFieldMapRepo) FindBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.FieldMap, error) {
	return f.fieldMap, f.err
}

type fakeSourceRepo struct {
	source *shared.ReconciliationSource
	err    error
}

func (f *fakeSourceRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*shared.ReconciliationSource, error) {
	return f.source, f.err
}

type fakeContextProvider struct {
	enabled bool
	err     error
}

func (f fakeContextProvider) IsAutoMatchEnabled(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.enabled, f.err
}

type fakeMatchTrigger struct {
	called    bool
	tenantID  uuid.UUID
	contextID uuid.UUID
}

func (f *fakeMatchTrigger) TriggerMatchForContext(_ context.Context, tenantID, contextID uuid.UUID) {
	f.called = true
	f.tenantID = tenantID
	f.contextID = contextID
}

func newTestDeps() UseCaseDeps {
	return UseCaseDeps{
		JobRepo:         &fakeJobRepo{},
		TransactionRepo: fakeTxRepo{},
		Dedupe:          fakeDedupe{},
		Publisher:       &fakePublisher{},
		OutboxRepo:      &fakeOutboxRepo{},
		Parsers:         fakeRegistry{parser: parserWithRange{}},
		FieldMapRepo:    &fakeFieldMapRepo{},
		SourceRepo:      &fakeSourceRepo{},
	}
}

func TestNewUseCaseRequiresDependencies(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	testDeps := deps
	testDeps.JobRepo = nil
	_, err := NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilJobRepository)

	testDeps = deps
	testDeps.TransactionRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilTransactionRepository)

	testDeps = deps
	testDeps.Dedupe = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilDedupeService)

	testDeps = deps
	testDeps.Publisher = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilEventPublisher)

	testDeps = deps
	testDeps.OutboxRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilOutboxRepository)

	testDeps = deps
	testDeps.Parsers = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilParserRegistry)

	testDeps = deps
	testDeps.FieldMapRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilFieldMapRepository)

	testDeps = deps
	testDeps.SourceRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestNewUseCase_NormalizesTypedNilOptionalDeps(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	var typedNilTrigger *fakeMatchTrigger
	var typedNilProvider *fakeContextProvider

	deps.MatchTrigger = sharedPorts.MatchTrigger(typedNilTrigger)
	deps.ContextProvider = sharedPorts.ContextProvider(typedNilProvider)

	uc, err := NewUseCase(deps)
	require.NoError(t, err)
	require.Nil(t, uc.matchTrigger)
	require.Nil(t, uc.contextProvider)
}

func TestTriggerAutoMatchIfEnabled_IgnoresTypedNilMatchTrigger(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, uuid.NewString())

	var typedNilTrigger *fakeMatchTrigger
	uc := &UseCase{
		contextProvider: fakeContextProvider{enabled: true},
		matchTrigger:    sharedPorts.MatchTrigger(typedNilTrigger),
	}

	require.NotPanics(t, func() {
		uc.triggerAutoMatchIfEnabled(ctx, contextID)
	})
}

func TestTriggerAutoMatchIfEnabled_IgnoresTypedNilContextProvider(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, uuid.NewString())
	trigger := &fakeMatchTrigger{}

	var typedNilProvider *fakeContextProvider
	uc := &UseCase{
		contextProvider: sharedPorts.ContextProvider(typedNilProvider),
		matchTrigger:    trigger,
	}

	require.NotPanics(t, func() {
		uc.triggerAutoMatchIfEnabled(ctx, contextID)
	})
	require.False(t, trigger.called)
}

type noTxJobRepo struct{}

func (noTxJobRepo) Create(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (noTxJobRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.IngestionJob, error) {
	return nil, nil
}

func (noTxJobRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (noTxJobRepo) Update(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (noTxJobRepo) FindLatestByExtractionID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func TestNewUseCaseRequiresTxInterfaces(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.JobRepo = repositories.JobRepository(noTxJobRepo{})

	_, err := NewUseCase(deps)
	require.ErrorIs(t, err, ErrJobRepoNotTxRunner)
}

// noTxUpdaterJobRepo implements WithTx but not UpdateWithTx.
type noTxUpdaterJobRepo struct {
	noTxJobRepo
}

func (noTxUpdaterJobRepo) WithTx(_ context.Context, fn func(*sql.Tx) error) error {
	return fn(&sql.Tx{})
}

func TestNewUseCaseRequiresJobTxUpdater(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.JobRepo = noTxUpdaterJobRepo{}

	_, err := NewUseCase(deps)
	require.ErrorIs(t, err, ErrJobRepoNotTxUpdater)
}

func TestNewUseCaseRequiresOutboxTxCreator(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	_, ok := deps.OutboxRepo.(outboxTxCreator)
	require.True(t, ok, "fakeOutboxRepo must implement outboxTxCreator")

	uc, err := NewUseCase(deps)
	require.NoError(t, err)
	require.NotNil(t, uc)
	require.NotNil(t, uc.outboxRepoTx)
}

func TestNewUseCaseDedupeTTLValidation(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.DedupeTTL = time.Second
	_, err := NewUseCase(deps)
	require.ErrorIs(t, err, ErrDedupeTTLTooShort)

	deps = newTestDeps()
	uc, err := NewUseCase(deps)
	require.NoError(t, err)
	require.Equal(t, defaultDedupeTTL, uc.dedupeTTL)

	deps = newTestDeps()
	deps.DedupeTTL = 2 * time.Minute
	uc, err = NewUseCase(deps)
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, uc.dedupeTTL)
}

func TestStartIngestionParserError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.SourceRepo = &fakeSourceRepo{source: nil} // Source not found

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
}

func TestStartIngestion_OutboxError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.OutboxRepo = &fakeOutboxRepo{createErr: errOutbox}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
}

func TestStartIngestionDateRangeFallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
}

type errorParser struct{}

func (errorParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return nil, errParse
}

func (errorParser) SupportedFormat() string { return "csv" }

type parserWithRange struct{}

func (parserWithRange) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: []*shared.Transaction{
			{
				ID:               uuid.New(),
				IngestionJobID:   uuid.New(),
				SourceID:         uuid.New(),
				ExternalID:       "ext",
				Amount:           decimal.RequireFromString("10"),
				Currency:         "USD",
				Date:             time.Now().UTC(),
				ExtractionStatus: shared.ExtractionStatusComplete,
				Status:           shared.TransactionStatusMatched,
			},
		},
		DateRange: &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
	}, nil
}

func (parserWithRange) SupportedFormat() string { return "csv" }

func TestStartIngestion_NilUseCase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var uc *UseCase

	_, err := uc.StartIngestion(
		ctx,
		uuid.New(),
		uuid.New(),
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestStartIngestion_EmptyFormat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(ctx, contextID, sourceID, "file.csv", 10, "", strings.NewReader(""))
	require.ErrorIs(t, err, ErrFormatRequiredUC)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"   ",
		strings.NewReader(""),
	)
	require.ErrorIs(t, err, ErrFormatRequiredUC)
}

func TestStartIngestion_SourceNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.SourceRepo = &fakeSourceRepo{source: nil}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.ErrorIs(t, err, ErrSourceNotFound)
}

func TestStartIngestion_FieldMapNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.FieldMapRepo = &fakeFieldMapRepo{fieldMap: nil}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.ErrorIs(t, err, ErrFieldMapNotFound)
}

func TestStartIngestion_SourceLoadError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.SourceRepo = &fakeSourceRepo{err: errDatabaseConnection}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errDatabaseConnection)
	require.Contains(t, err.Error(), "failed to load source")
}

func TestStartIngestion_FieldMapLoadError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.FieldMapRepo = &fakeFieldMapRepo{err: errFieldMapQuery}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errFieldMapQuery)
	require.Contains(t, err.Error(), "failed to load field map")
}

func TestStartIngestion_ParserError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: errorParser{}}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader("invalid data"),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errParse)
}

func TestStartIngestion_EmptyFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	emptyParser := fakeStreamingParser{
		result: &ports.StreamingParseResult{
			TotalRecords: 0,
			TotalErrors:  0,
		},
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: emptyParser}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"empty.csv",
		10,
		"csv",
		strings.NewReader("header1,header2\n"),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEmptyFile)
}

// errTransactionUpdate is a sentinel error for transaction update failures.
var errTransactionUpdate = errors.New("transaction update failed")

// errTransactionFind is a sentinel error for transaction find failures.
var errTransactionFind = errors.New("transaction find failed")

// fakeIgnoreTxRepo is a mock transaction repository for IgnoreTransaction tests.
type fakeIgnoreTxRepo struct {
	fakeTxRepo
	findByIDResult *shared.Transaction
	findByIDErr    error
	updateResult   *shared.Transaction
	updateErr      error
}

func (f *fakeIgnoreTxRepo) FindByID(_ context.Context, _ uuid.UUID) (*shared.Transaction, error) {
	return f.findByIDResult, f.findByIDErr
}

func (f *fakeIgnoreTxRepo) UpdateStatus(
	_ context.Context,
	_, _ uuid.UUID,
	status shared.TransactionStatus,
) (*shared.Transaction, error) {
	if f.updateResult != nil {
		f.updateResult.Status = status
	}

	return f.updateResult, f.updateErr
}

func TestIgnoreTransaction_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "test reason",
	})
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestIgnoreTransaction_EmptyReason(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "",
	})
	require.ErrorIs(t, err, ErrReasonRequired)
}

func TestIgnoreTransaction_WhitespaceOnlyReason(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "   \t\n  ",
	})
	require.ErrorIs(t, err, ErrReasonRequired)
}

func TestIgnoreTransaction_TransactionNotFound(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: nil,
		findByIDErr:    sql.ErrNoRows,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "duplicate entry",
	})
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

func TestIgnoreTransaction_TransactionNilResult(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: nil,
		findByIDErr:    nil,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "duplicate entry",
	})
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

func TestIgnoreTransaction_TransactionFindError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: nil,
		findByIDErr:    errTransactionFind,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "duplicate entry",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errTransactionFind)
	require.Contains(t, err.Error(), "failed to find transaction")
}

func TestIgnoreTransaction_OnlyUnmatchedCanBeIgnored(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		status shared.TransactionStatus
	}{
		{"matched", shared.TransactionStatusMatched},
		{"ignored", shared.TransactionStatusIgnored},
		{"pending_review", shared.TransactionStatusPendingReview},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			txID := uuid.New()
			contextID := uuid.New()

			deps := newTestDeps()
			deps.TransactionRepo = &fakeIgnoreTxRepo{
				findByIDResult: &shared.Transaction{
					ID:     txID,
					Status: tc.status,
				},
				findByIDErr: nil,
			}

			uc, err := NewUseCase(deps)
			require.NoError(t, err)

			_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
				TransactionID: txID,
				ContextID:     contextID,
				Reason:        "test reason",
			})
			require.ErrorIs(t, err, ErrTransactionNotIgnorable)
		})
	}
}

func TestIgnoreTransaction_UpdateError(t *testing.T) {
	t.Parallel()

	txID := uuid.New()
	contextID := uuid.New()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: &shared.Transaction{
			ID:     txID,
			Status: shared.TransactionStatusUnmatched,
		},
		findByIDErr: nil,
		updateErr:   errTransactionUpdate,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: txID,
		ContextID:     contextID,
		Reason:        "duplicate entry",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errTransactionUpdate)
	require.Contains(t, err.Error(), "failed to update transaction status")
}

func TestIgnoreTransaction_UpdateReturnsNoRows(t *testing.T) {
	t.Parallel()

	txID := uuid.New()
	contextID := uuid.New()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: &shared.Transaction{
			ID:     txID,
			Status: shared.TransactionStatusUnmatched,
		},
		findByIDErr: nil,
		updateErr:   sql.ErrNoRows,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: txID,
		ContextID:     contextID,
		Reason:        "duplicate entry",
	})
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

func TestIgnoreTransaction_Success(t *testing.T) {
	t.Parallel()

	txID := uuid.New()
	contextID := uuid.New()

	originalTx := &shared.Transaction{
		ID:       txID,
		Status:   shared.TransactionStatusUnmatched,
		Amount:   decimal.NewFromFloat(100.50),
		Currency: "USD",
	}

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: originalTx,
		findByIDErr:    nil,
		updateResult: &shared.Transaction{
			ID:       txID,
			Status:   shared.TransactionStatusUnmatched,
			Amount:   decimal.NewFromFloat(100.50),
			Currency: "USD",
		},
		updateErr: nil,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: txID,
		ContextID:     contextID,
		Reason:        "duplicate entry from legacy system",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, txID, result.ID)
	require.Equal(t, shared.TransactionStatusIgnored, result.Status)
}

// errGetParser is a sentinel error for parser retrieval failures.
var errGetParser = errors.New("parser not found")

// failingRegistry returns an error when getting a parser.
type failingRegistry struct{}

func (failingRegistry) GetParser(_ string) (ports.Parser, error) { return nil, errGetParser }
func (failingRegistry) Register(_ ports.Parser)                  {}

func TestStartIngestion_GetParserError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = failingRegistry{}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errGetParser)
	require.Contains(t, err.Error(), "failed to get parser")
}

// errJobCreate is a sentinel error for job creation failures.
var errJobCreate = errors.New("job creation failed")

func TestStartIngestion_JobCreateError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{createErr: errJobCreate}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errJobCreate)
	require.Contains(t, err.Error(), "failed to create job")
}

// errBatchInsert is a sentinel error for batch insert failures.
var errBatchInsert = errors.New("batch insert failed")

// fakeTxRepoWithBatchError simulates batch insert errors.
type fakeTxRepoWithBatchError struct {
	fakeTxRepo
}

func (fakeTxRepoWithBatchError) CreateBatch(
	_ context.Context,
	_ []*shared.Transaction,
) ([]*shared.Transaction, error) {
	return nil, errBatchInsert
}

func TestStartIngestion_BatchInsertError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.TransactionRepo = fakeTxRepoWithBatchError{}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errBatchInsert)
}

// fakeDedupeWithDuplicate simulates duplicate detection.
type fakeDedupeWithDuplicate struct {
	duplicateHash string
}

func (f fakeDedupeWithDuplicate) CalculateHash(_ uuid.UUID, externalID string) string {
	return externalID
}

func (f fakeDedupeWithDuplicate) IsDuplicate(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (bool, error) {
	return false, nil
}

func (f fakeDedupeWithDuplicate) MarkSeen(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ time.Duration,
) error {
	return nil
}

func (f fakeDedupeWithDuplicate) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
	_ int,
) error {
	if hash == f.duplicateHash {
		return ports.ErrDuplicateTransaction
	}

	return nil
}

func (f fakeDedupeWithDuplicate) Clear(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (f fakeDedupeWithDuplicate) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}

// parserReturningMultipleTransactions returns multiple transactions for testing.
type parserReturningMultipleTransactions struct {
	transactions []*shared.Transaction
}

func (p parserReturningMultipleTransactions) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: p.transactions,
		DateRange:    &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
	}, nil
}

func (p parserReturningMultipleTransactions) SupportedFormat() string { return "csv" }

func TestStartIngestion_DuplicateTransactionSkipped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	tx1 := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   job.ID,
		SourceID:         sourceID,
		ExternalID:       "ext1",
		Amount:           decimal.RequireFromString("10"),
		Currency:         "USD",
		Date:             time.Now().UTC(),
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
	}
	tx2 := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   job.ID,
		SourceID:         sourceID,
		ExternalID:       "ext2_duplicate",
		Amount:           decimal.RequireFromString("20"),
		Currency:         "USD",
		Date:             time.Now().UTC(),
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Dedupe = fakeDedupeWithDuplicate{duplicateHash: "ext2_duplicate"}
	deps.Parsers = fakeRegistry{
		parser: parserReturningMultipleTransactions{transactions: []*shared.Transaction{tx1, tx2}},
	}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// errDedupeMarkSeen is a sentinel error for dedupe mark seen failures.
var errDedupeMarkSeen = errors.New("dedupe mark seen failed")

func TestStartIngestion_DedupeMarkSeenError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Dedupe = fakeDedupe{err: errDedupeMarkSeen}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errDedupeMarkSeen)
}

// errExistsBulk is a sentinel error for bulk exists check failures.
var errExistsBulk = errors.New("bulk exists check failed")

// fakeTxRepoWithExistsBulkError simulates bulk exists check errors.
type fakeTxRepoWithExistsBulkError struct {
	fakeTxRepo
}

func (fakeTxRepoWithExistsBulkError) ExistsBulkBySourceAndExternalID(
	_ context.Context,
	_ []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	return nil, errExistsBulk
}

func TestStartIngestion_ExistsBulkError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.TransactionRepo = fakeTxRepoWithExistsBulkError{}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errExistsBulk)
}

// errJobUpdate is a sentinel error for job update failures.
var errJobUpdate = errors.New("job update failed")

func TestStartIngestion_JobUpdateError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{updateErr: errJobUpdate}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errJobUpdate)
}

// parserWithErrors returns transactions with validation errors.
type parserWithErrors struct{}

func (parserWithErrors) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: []*shared.Transaction{
			{
				ID:               uuid.New(),
				IngestionJobID:   uuid.New(),
				SourceID:         uuid.New(),
				ExternalID:       "ext",
				Amount:           decimal.RequireFromString("10"),
				Currency:         "USD",
				Date:             time.Now().UTC(),
				ExtractionStatus: shared.ExtractionStatusComplete,
				Status:           shared.TransactionStatusUnmatched,
			},
		},
		Errors:    []ports.ParseError{{Row: 2, Field: "amount", Message: "invalid amount"}},
		DateRange: &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
	}, nil
}

func (parserWithErrors) SupportedFormat() string { return "csv" }

func TestStartIngestion_WithParseErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: parserWithErrors{}}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result.Metadata.Error, "rows failed validation")
}

// parserWithNilDateRange returns transactions without a date range.
type parserWithNilDateRange struct{}

func (parserWithNilDateRange) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: []*shared.Transaction{
			{
				ID:               uuid.New(),
				IngestionJobID:   uuid.New(),
				SourceID:         uuid.New(),
				ExternalID:       "ext",
				Amount:           decimal.RequireFromString("10"),
				Currency:         "USD",
				Date:             time.Now().UTC(),
				ExtractionStatus: shared.ExtractionStatusComplete,
				Status:           shared.TransactionStatusUnmatched,
			},
		},
		DateRange: nil,
	}, nil
}

func (parserWithNilDateRange) SupportedFormat() string { return "csv" }

func TestStartIngestion_NilDateRangeFallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: parserWithNilDateRange{}}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// parserWithEmptyTransactions returns no transactions.
type parserWithEmptyTransactions struct{}

func (parserWithEmptyTransactions) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: []*shared.Transaction{},
		DateRange:    &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
	}, nil
}

func (parserWithEmptyTransactions) SupportedFormat() string { return "csv" }

func TestStartIngestion_EmptyTransactions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: parserWithEmptyTransactions{}}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEmptyFile)
}

// fakeTxRepoWithExistingTransactions simulates existing transactions in the database.
type fakeTxRepoWithExistingTransactions struct {
	fakeTxRepo
	existingKeys map[repositories.ExternalIDKey]bool
}

func (f fakeTxRepoWithExistingTransactions) ExistsBulkBySourceAndExternalID(
	_ context.Context,
	keys []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	result := make(map[repositories.ExternalIDKey]bool)
	for _, key := range keys {
		if f.existingKeys[key] {
			result[key] = true
		}
	}

	return result, nil
}

func TestStartIngestion_ExistingTransactionSkipped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	tx1 := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   job.ID,
		SourceID:         sourceID,
		ExternalID:       "existing_ext",
		Amount:           decimal.RequireFromString("10"),
		Currency:         "USD",
		Date:             time.Now().UTC(),
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
	}

	existingKeys := map[repositories.ExternalIDKey]bool{
		{SourceID: sourceID, ExternalID: "existing_ext"}: true,
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.TransactionRepo = fakeTxRepoWithExistingTransactions{existingKeys: existingKeys}
	deps.Parsers = fakeRegistry{
		parser: parserReturningMultipleTransactions{transactions: []*shared.Transaction{tx1}},
	}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// errWithTx is a sentinel error for transaction wrapper failures.
var errWithTx = errors.New("transaction wrapper failed")

func TestStartIngestion_WithTxError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{withTxErr: errWithTx}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errWithTx)
}

// fakeSourceRepoWithSQLNoRows returns sql.ErrNoRows.
type fakeSourceRepoWithSQLNoRows struct{}

func (fakeSourceRepoWithSQLNoRows) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*shared.ReconciliationSource, error) {
	return nil, sql.ErrNoRows
}

func TestStartIngestion_SourceSQLNoRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.SourceRepo = fakeSourceRepoWithSQLNoRows{}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.ErrorIs(t, err, ErrSourceNotFound)
}

// fakeFieldMapRepoWithSQLNoRows returns sql.ErrNoRows.
type fakeFieldMapRepoWithSQLNoRows struct{}

func (fakeFieldMapRepoWithSQLNoRows) FindBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.FieldMap, error) {
	return nil, sql.ErrNoRows
}

func TestStartIngestion_FieldMapSQLNoRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}
	deps.FieldMapRepo = fakeFieldMapRepoWithSQLNoRows{}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.ErrorIs(t, err, ErrFieldMapNotFound)
}

// fakeStreamingParser implements StreamingParser for testing.
type fakeStreamingParser struct {
	result     *ports.StreamingParseResult
	parseErr   error
	callbackFn func(chunk []*shared.Transaction, errors []ports.ParseError) error
}

func (f fakeStreamingParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{}, nil
}

func (f fakeStreamingParser) SupportedFormat() string { return "csv" }

func (f fakeStreamingParser) ParseStreaming(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
	_ int,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	if f.parseErr != nil {
		return nil, f.parseErr
	}

	if f.callbackFn != nil {
		if err := f.callbackFn(nil, nil); err != nil {
			return nil, err
		}
	}

	return f.result, nil
}

func TestStartIngestion_StreamingParserSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	streamingParser := fakeStreamingParser{
		result: &ports.StreamingParseResult{
			TotalRecords: 5,
			TotalErrors:  0,
			DateRange:    &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
		},
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: streamingParser}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// errStreamingParse is a sentinel error for streaming parse failures.
var errStreamingParse = errors.New("streaming parse failed")

func TestStartIngestion_StreamingParserError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	streamingParser := fakeStreamingParser{
		parseErr: errStreamingParse,
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: streamingParser}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errStreamingParse)
}

func TestStartIngestion_StreamingParserWithErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	streamingParser := fakeStreamingParser{
		result: &ports.StreamingParseResult{
			TotalRecords: 10,
			TotalErrors:  3,
			DateRange:    &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
		},
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: streamingParser}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result.Metadata.Error, "rows failed validation")
}

// fakeStreamingParserWithChunks calls callback with actual transactions.
type fakeStreamingParserWithChunks struct {
	transactions []*shared.Transaction
	chunkErrors  []ports.ParseError
	result       *ports.StreamingParseResult
}

func (f fakeStreamingParserWithChunks) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{}, nil
}

func (f fakeStreamingParserWithChunks) SupportedFormat() string { return "csv" }

func (f fakeStreamingParserWithChunks) ParseStreaming(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
	_ int,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	if err := callback(f.transactions, f.chunkErrors); err != nil {
		return nil, err
	}

	return f.result, nil
}

func TestStartIngestion_StreamingParserWithChunks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	tx := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   job.ID,
		SourceID:         sourceID,
		ExternalID:       "ext1",
		Amount:           decimal.RequireFromString("10"),
		Currency:         "USD",
		Date:             time.Now().UTC(),
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
	}

	streamingParser := fakeStreamingParserWithChunks{
		transactions: []*shared.Transaction{tx},
		chunkErrors:  []ports.ParseError{{Row: 2, Message: "test error"}},
		result: &ports.StreamingParseResult{
			TotalRecords: 1,
			TotalErrors:  1,
			DateRange:    &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
		},
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: streamingParser}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// errChunkCallback is a sentinel error for chunk callback failures.
var errChunkCallback = errors.New("chunk callback failed")

func TestStartIngestion_StreamingParserChunkError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	streamingParser := fakeStreamingParser{
		callbackFn: func(_ []*shared.Transaction, _ []ports.ParseError) error {
			return errChunkCallback
		},
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: streamingParser}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errChunkCallback)
}

// fakeDedupeWithClearError simulates clear batch errors.
type fakeDedupeWithClearError struct {
	fakeDedupe
	clearErr error
}

func (f fakeDedupeWithClearError) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return f.clearErr
}

func TestStartIngestion_CleanupOnFailureWithClearError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	// Parser that returns transactions to create marked hashes, then fails
	streamingParser := fakeStreamingParserWithChunks{
		transactions: []*shared.Transaction{
			{
				ID:               uuid.New(),
				IngestionJobID:   job.ID,
				SourceID:         sourceID,
				ExternalID:       "ext1",
				Amount:           decimal.RequireFromString("10"),
				Currency:         "USD",
				Date:             time.Now().UTC(),
				ExtractionStatus: shared.ExtractionStatusComplete,
				Status:           shared.TransactionStatusUnmatched,
			},
		},
		result: &ports.StreamingParseResult{
			TotalRecords: 1,
			TotalErrors:  0,
			DateRange:    &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
		},
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: streamingParser}
	deps.Dedupe = fakeDedupeWithClearError{clearErr: errors.New("clear failed")}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	// This should succeed - clear errors are logged but don't fail the operation
	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestStartIngestion_StreamingParserNilDateRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 10)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	streamingParser := fakeStreamingParser{
		result: &ports.StreamingParseResult{
			TotalRecords: 5,
			TotalErrors:  0,
			DateRange:    nil,
		},
	}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.Parsers = fakeRegistry{parser: streamingParser}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"file.csv",
		10,
		"csv",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestConvertParseErrors(t *testing.T) {
	t.Parallel()

	t.Run("nil errors returns nil", func(t *testing.T) {
		t.Parallel()
		result := convertParseErrors(nil)
		require.Nil(t, result)
	})

	t.Run("empty errors returns nil", func(t *testing.T) {
		t.Parallel()
		result := convertParseErrors([]ports.ParseError{})
		require.Nil(t, result)
	})

	t.Run("converts parse errors correctly", func(t *testing.T) {
		t.Parallel()
		errs := []ports.ParseError{
			{Row: 1, Field: "amount", Message: "invalid format"},
			{Row: 5, Field: "date", Message: "cannot parse"},
		}
		result := convertParseErrors(errs)
		require.Len(t, result, 2)
		require.Equal(t, 1, result[0].Row)
		require.Equal(t, "amount", result[0].Field)
		require.Equal(t, "invalid format", result[0].Message)
		require.Equal(t, 5, result[1].Row)
		require.Equal(t, "date", result[1].Field)
	})

	t.Run("limits to maxErrorDetails", func(t *testing.T) {
		t.Parallel()
		errs := make([]ports.ParseError, 100)
		for i := range errs {
			errs[i] = ports.ParseError{Row: i + 1, Field: "field", Message: "error"}
		}
		result := convertParseErrors(errs)
		require.Len(t, result, maxErrorDetails)
		require.Equal(t, 1, result[0].Row)
		require.Equal(t, maxErrorDetails, result[maxErrorDetails-1].Row)
	})
}

type captureLogger struct {
	mu       sync.Mutex
	messages []string
}

func (logger *captureLogger) Log(_ context.Context, _ libLog.Level, msg string, _ ...libLog.Field) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	logger.messages = append(logger.messages, msg)
}

//nolint:ireturn
func (logger *captureLogger) With(_ ...libLog.Field) libLog.Logger {
	return logger
}

//nolint:ireturn
func (logger *captureLogger) WithGroup(_ string) libLog.Logger {
	return logger
}

func (*captureLogger) Enabled(_ libLog.Level) bool {
	return true
}

func (*captureLogger) Sync(_ context.Context) error {
	return nil
}

func (logger *captureLogger) joinedMessages() string {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	return strings.Join(logger.messages, "\n")
}

type fakeCleanupTxRepo struct {
	calls int
	err   error
}

func (repo *fakeCleanupTxRepo) CleanupFailedJobTransactionsWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
) error {
	repo.calls++

	return repo.err
}

func TestCleanupPartialTransactionsBestEffort_CallsCleanupRepository(t *testing.T) {
	t.Parallel()

	runner := &sequenceTxRunner{}
	cleanupRepo := &fakeCleanupTxRepo{}

	uc := &UseCase{
		jobTxRunner:     runner,
		txCleanupRepoTx: cleanupRepo,
	}

	ctx := libCommons.ContextWithLogger(context.Background(), &captureLogger{})
	uc.cleanupPartialTransactionsBestEffort(ctx, uuid.New())

	require.Equal(t, 1, runner.calls)
	require.Equal(t, 1, cleanupRepo.calls)
}

func TestCleanupPartialTransactionsBestEffort_LogsWarningOnCleanupError(t *testing.T) {
	t.Parallel()

	runner := &sequenceTxRunner{}
	cleanupRepo := &fakeCleanupTxRepo{err: errors.New("cleanup failed")}
	logger := &captureLogger{}

	uc := &UseCase{
		jobTxRunner:     runner,
		txCleanupRepoTx: cleanupRepo,
	}

	ctx := libCommons.ContextWithLogger(context.Background(), logger)
	uc.cleanupPartialTransactionsBestEffort(ctx, uuid.New())

	require.Equal(t, 1, runner.calls)
	require.Equal(t, 1, cleanupRepo.calls)
	require.Contains(t, logger.joinedMessages(), "failed to execute best-effort partial transaction cleanup")
}

func TestCleanupPartialTransactionsBestEffort_NoOpWithoutCleanupRepository(t *testing.T) {
	t.Parallel()

	runner := &sequenceTxRunner{}

	uc := &UseCase{
		jobTxRunner: runner,
	}

	ctx := libCommons.ContextWithLogger(context.Background(), &captureLogger{})
	uc.cleanupPartialTransactionsBestEffort(ctx, uuid.New())

	require.Equal(t, 0, runner.calls)
}

type sequenceTxRunner struct {
	calls     int
	errOnCall map[int]error
}

func (runner *sequenceTxRunner) WithTx(_ context.Context, fn func(*sql.Tx) error) error {
	runner.calls++

	if err, exists := runner.errOnCall[runner.calls]; exists {
		return err
	}

	return fn(&sql.Tx{})
}

type cancelAwareTxRunner struct {
	calls int
}

func (runner *cancelAwareTxRunner) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	runner.calls++

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return fn(&sql.Tx{})
}

func TestFailJob_CleanupRunsBestEffortOutsidePrimaryTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 1)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))

	runner := &sequenceTxRunner{
		errOnCall: map[int]error{2: errors.New("cleanup tx failed")},
	}

	uc := &UseCase{
		dedupe:      fakeDedupe{},
		jobTxRunner: runner,
		jobRepoTx: &fakeJobRepo{
			updated: job,
		},
		txCleanupRepoTx: &fakeCleanupTxRepo{},
		outboxRepoTx:    &fakeOutboxRepo{},
	}

	cause := errors.New("ingestion parse failed")
	err = uc.failJob(ctx, job, cause, nil)

	require.ErrorIs(t, err, cause)
	require.Equal(t, 2, runner.calls, "expected one tx for fail persistence and one best-effort cleanup tx")
}

func TestFailJob_UsesDetachedContextForPersistenceAndCleanup(t *testing.T) {
	t.Parallel()

	parentCtx, cancel := context.WithCancel(context.Background())
	cancel()

	contextID := uuid.New()
	sourceID := uuid.New()

	job, err := entities.NewIngestionJob(context.Background(), contextID, sourceID, "file.csv", 1)
	require.NoError(t, err)
	require.NoError(t, job.Start(context.Background()))

	runner := &cancelAwareTxRunner{}
	cleanupRepo := &fakeCleanupTxRepo{}

	uc := &UseCase{
		dedupe:          fakeDedupe{},
		jobTxRunner:     runner,
		jobRepoTx:       &fakeJobRepo{updated: job},
		txCleanupRepoTx: cleanupRepo,
		outboxRepoTx:    &fakeOutboxRepo{},
	}

	cause := errors.New("ingestion parse failed")
	err = uc.failJob(parentCtx, job, cause, nil)

	require.ErrorIs(t, err, cause)
	require.Equal(t, 2, runner.calls, "expected fail persistence tx plus best-effort cleanup tx")
	require.Equal(t, 1, cleanupRepo.calls)
}
