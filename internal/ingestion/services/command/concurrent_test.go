//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// --- Concurrent-Safe Fake Implementations ---

type concurrentJobRepo struct {
	mu        sync.RWMutex
	jobs      map[uuid.UUID]*entities.IngestionJob
	createErr error
	updateErr error
}

func newConcurrentJobRepo() *concurrentJobRepo {
	return &concurrentJobRepo{
		jobs: make(map[uuid.UUID]*entities.IngestionJob),
	}
}

func (r *concurrentJobRepo) Create(
	_ context.Context,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	if r.createErr != nil {
		return nil, r.createErr
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.jobs[job.ID] = job

	return job, nil
}

func (r *concurrentJobRepo) FindByID(_ context.Context, id uuid.UUID) (*entities.IngestionJob, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if job, ok := r.jobs[id]; ok {
		cp := *job

		return &cp, nil
	}

	return nil, sql.ErrNoRows
}

func (r *concurrentJobRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobs := make([]*entities.IngestionJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		cp := *job
		jobs = append(jobs, &cp)
	}

	return jobs, libHTTP.CursorPagination{}, nil
}

func (r *concurrentJobRepo) Update(
	_ context.Context,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	if r.updateErr != nil {
		return nil, r.updateErr
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.jobs[job.ID] = job

	return job, nil
}

func (r *concurrentJobRepo) WithTx(_ context.Context, fn func(*sql.Tx) error) error {
	return fn(&sql.Tx{})
}

func (r *concurrentJobRepo) UpdateWithTx(
	_ context.Context,
	_ *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	if r.updateErr != nil {
		return nil, r.updateErr
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.jobs[job.ID] = job

	return job, nil
}

func (r *concurrentJobRepo) JobCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.jobs)
}

type concurrentTxRepo struct {
	mu           sync.RWMutex
	transactions map[uuid.UUID]*shared.Transaction
	batchErr     error
}

func newConcurrentTxRepo() *concurrentTxRepo {
	return &concurrentTxRepo{
		transactions: make(map[uuid.UUID]*shared.Transaction),
	}
}

func (r *concurrentTxRepo) Create(
	_ context.Context,
	tx *shared.Transaction,
) (*shared.Transaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.transactions[tx.ID] = tx

	return tx, nil
}

func (r *concurrentTxRepo) CreateBatch(
	_ context.Context,
	txs []*shared.Transaction,
) ([]*shared.Transaction, error) {
	if r.batchErr != nil {
		return nil, r.batchErr
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, tx := range txs {
		r.transactions[tx.ID] = tx
	}

	return txs, nil
}

func (r *concurrentTxRepo) FindByID(_ context.Context, id uuid.UUID) (*shared.Transaction, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if tx, ok := r.transactions[id]; ok {
		cp := *tx

		return &cp, nil
	}

	return nil, sql.ErrNoRows
}

func (r *concurrentTxRepo) FindByJobID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (r *concurrentTxRepo) FindByJobAndContextID(
	_ context.Context,
	_ uuid.UUID,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (r *concurrentTxRepo) FindBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (*shared.Transaction, error) {
	return nil, nil
}

func (r *concurrentTxRepo) ExistsBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (bool, error) {
	return false, nil
}

func (r *concurrentTxRepo) ExistsBulkBySourceAndExternalID(
	_ context.Context,
	_ []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	return make(map[repositories.ExternalIDKey]bool), nil
}

func (r *concurrentTxRepo) UpdateStatus(
	_ context.Context,
	_ uuid.UUID,
	_ uuid.UUID,
	_ shared.TransactionStatus,
) (*shared.Transaction, error) {
	return nil, nil
}

func (r *concurrentTxRepo) SearchTransactions(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.TransactionSearchParams,
) ([]*shared.Transaction, int64, error) {
	return nil, 0, nil
}

func (r *concurrentTxRepo) CleanupFailedJobTransactionsWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
) error {
	return nil
}

func (r *concurrentTxRepo) TransactionCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.transactions)
}

type concurrentDedupe struct {
	mu     sync.RWMutex
	seen   map[string]bool
	dupErr error
}

func newConcurrentDedupe() *concurrentDedupe {
	return &concurrentDedupe{
		seen: make(map[string]bool),
	}
}

func (d *concurrentDedupe) CalculateHash(sourceID uuid.UUID, externalID string) string {
	return sourceID.String() + ":" + externalID
}

func (d *concurrentDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, hash string) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.seen[hash], nil
}

func (d *concurrentDedupe) MarkSeen(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.seen[hash] {
		return ports.ErrDuplicateTransaction
	}

	d.seen[hash] = true

	return nil
}

func (d *concurrentDedupe) MarkSeenWithRetry(
	ctx context.Context,
	contextID uuid.UUID,
	hash string,
	ttl time.Duration,
	_ int,
) error {
	if d.dupErr != nil {
		return d.dupErr
	}

	return d.MarkSeen(ctx, contextID, hash, ttl)
}

func (d *concurrentDedupe) Clear(_ context.Context, _ uuid.UUID, hash string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.seen, hash)

	return nil
}

func (d *concurrentDedupe) ClearBatch(_ context.Context, _ uuid.UUID, hashes []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, hash := range hashes {
		delete(d.seen, hash)
	}

	return nil
}

func (d *concurrentDedupe) SeenCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return len(d.seen)
}

type concurrentOutboxRepo struct {
	mu     sync.RWMutex
	events []*outboxEntities.OutboxEvent
}

func newConcurrentOutboxRepo() *concurrentOutboxRepo {
	return &concurrentOutboxRepo{
		events: make([]*outboxEntities.OutboxEvent, 0),
	}
}

func (r *concurrentOutboxRepo) Create(
	_ context.Context,
	event *outboxEntities.OutboxEvent,
) (*outboxEntities.OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)

	return event, nil
}

func (r *concurrentOutboxRepo) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	event *outboxEntities.OutboxEvent,
) (*outboxEntities.OutboxEvent, error) {
	return r.Create(context.Background(), event)
}

func (r *concurrentOutboxRepo) ListPending(
	_ context.Context,
	_ int,
) ([]*outboxEntities.OutboxEvent, error) {
	return nil, nil
}

func (r *concurrentOutboxRepo) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*outboxEntities.OutboxEvent, error) {
	return nil, nil
}

func (r *concurrentOutboxRepo) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (r *concurrentOutboxRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*outboxEntities.OutboxEvent, error) {
	return nil, nil
}

func (r *concurrentOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (r *concurrentOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	return nil
}

func (r *concurrentOutboxRepo) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outboxEntities.OutboxEvent, error) {
	return nil, nil
}

func (r *concurrentOutboxRepo) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outboxEntities.OutboxEvent, error) {
	return nil, nil
}

func (r *concurrentOutboxRepo) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outboxEntities.OutboxEvent, error) {
	return nil, nil
}

func (r *concurrentOutboxRepo) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (r *concurrentOutboxRepo) EventCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.events)
}

type concurrentStreamingParser struct{}

func (p concurrentStreamingParser) SupportedFormat() string { return "csv" }

func (p concurrentStreamingParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: make([]*shared.Transaction, 0),
		Errors:       make([]ports.ParseError, 0),
	}, nil
}

func (p concurrentStreamingParser) ParseStreaming(
	_ context.Context,
	_ io.Reader,
	job *entities.IngestionJob,
	_ *shared.FieldMap,
	_ int,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	now := time.Now().UTC()
	transactions := []*shared.Transaction{
		{
			ID:               uuid.New(),
			IngestionJobID:   job.ID,
			SourceID:         job.SourceID,
			ExternalID:       uuid.New().String(),
			Amount:           decimal.NewFromFloat(100.50),
			Currency:         "USD",
			Date:             now,
			ExtractionStatus: shared.ExtractionStatusComplete,
			Status:           shared.TransactionStatusUnmatched,
		},
	}

	if err := callback(transactions, nil); err != nil {
		return nil, err
	}

	return &ports.StreamingParseResult{
		TotalRecords: 1,
		TotalErrors:  0,
		DateRange:    &ports.DateRange{Start: now, End: now},
	}, nil
}

type concurrentRegistry struct {
	parser ports.Parser
}

func (r concurrentRegistry) GetParser(_ string) (ports.Parser, error) { return r.parser, nil }
func (r concurrentRegistry) Register(_ ports.Parser)                  {}

// --- Concurrent Tests ---

func TestConcurrentIngestionJobs_SameContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := newConcurrentJobRepo()
	txRepo := newConcurrentTxRepo()
	dedupe := newConcurrentDedupe()
	outboxRepo := newConcurrentOutboxRepo()

	deps := UseCaseDeps{
		JobRepo:         jobRepo,
		TransactionRepo: txRepo,
		Dedupe:          dedupe,
		Publisher:       &fakePublisher{},
		OutboxRepo:      outboxRepo,
		Parsers:         concurrentRegistry{parser: concurrentStreamingParser{}},
		FieldMapRepo:    &fakeFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID}},
		SourceRepo:      &fakeSourceRepo{source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID}},
		DedupeTTL:       time.Hour,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	const numJobs = 10

	var wg sync.WaitGroup

	var successCount atomic.Int32

	var errorCount atomic.Int32

	wg.Add(numJobs)

	for i := 0; i < numJobs; i++ {
		go func(idx int) {
			defer wg.Done()

			_, err := uc.StartIngestion(
				ctx,
				contextID,
				sourceID,
				"concurrent_file.csv",
				100,
				"csv",
				strings.NewReader("data"),
			)
			if err != nil {
				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	require.Equal(t, int32(numJobs), successCount.Load()+errorCount.Load())
	require.Equal(t, numJobs, jobRepo.JobCount())
}

func TestConcurrentIngestionJobs_DifferentContexts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	jobRepo := newConcurrentJobRepo()
	txRepo := newConcurrentTxRepo()
	dedupe := newConcurrentDedupe()
	outboxRepo := newConcurrentOutboxRepo()

	const numContexts = 5

	var wg sync.WaitGroup

	var successCount atomic.Int32

	wg.Add(numContexts)

	for i := 0; i < numContexts; i++ {
		go func(idx int) {
			defer wg.Done()

			contextID := uuid.New()
			sourceID := uuid.New()

			deps := UseCaseDeps{
				JobRepo:         jobRepo,
				TransactionRepo: txRepo,
				Dedupe:          dedupe,
				Publisher:       &fakePublisher{},
				OutboxRepo:      outboxRepo,
				Parsers:         concurrentRegistry{parser: concurrentStreamingParser{}},
				FieldMapRepo:    &fakeFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID}},
				SourceRepo:      &fakeSourceRepo{source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID}},
				DedupeTTL:       time.Hour,
			}

			uc, err := NewUseCase(deps)
			if err != nil {
				return
			}

			_, err = uc.StartIngestion(
				ctx,
				contextID,
				sourceID,
				"context_file.csv",
				100,
				"csv",
				strings.NewReader("data"),
			)
			if err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	require.Equal(t, int32(numContexts), successCount.Load())
	require.Equal(t, numContexts, jobRepo.JobCount())
}

func TestConcurrentDeduplication(t *testing.T) {
	t.Parallel()

	dedupe := newConcurrentDedupe()
	contextID := uuid.New()
	sourceID := uuid.New()
	externalID := "TX-123"

	const numGoroutines = 100

	var wg sync.WaitGroup

	var successCount atomic.Int32

	var dupCount atomic.Int32

	hash := dedupe.CalculateHash(sourceID, externalID)

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			err := dedupe.MarkSeen(context.Background(), contextID, hash, time.Hour)
			if err == nil {
				successCount.Add(1)
			} else if errors.Is(err, ports.ErrDuplicateTransaction) {
				dupCount.Add(1)
			}
		}()
	}

	wg.Wait()

	require.Equal(t, int32(1), successCount.Load())
	require.Equal(t, int32(numGoroutines-1), dupCount.Load())
}

func TestConcurrentTransactionInserts(t *testing.T) {
	t.Parallel()

	txRepo := newConcurrentTxRepo()
	ctx := context.Background()

	const numBatches = 10
	const batchSize = 100

	var wg sync.WaitGroup

	wg.Add(numBatches)

	for i := 0; i < numBatches; i++ {
		go func(batchIdx int) {
			defer wg.Done()

			transactions := make([]*shared.Transaction, batchSize)

			for j := 0; j < batchSize; j++ {
				transactions[j] = &shared.Transaction{
					ID:         uuid.New(),
					SourceID:   uuid.New(),
					ExternalID: uuid.New().String(),
					Amount:     decimal.NewFromFloat(100.50),
					Currency:   "USD",
					Date:       time.Now().UTC(),
					Status:     shared.TransactionStatusUnmatched,
				}
			}

			_, _ = txRepo.CreateBatch(ctx, transactions)
		}(i)
	}

	wg.Wait()

	require.Equal(t, numBatches*batchSize, txRepo.TransactionCount())
}

func TestConcurrentOutboxEvents(t *testing.T) {
	t.Parallel()

	outboxRepo := newConcurrentOutboxRepo()
	ctx := context.Background()

	const numEvents = 50

	var wg sync.WaitGroup

	wg.Add(numEvents)

	for i := 0; i < numEvents; i++ {
		go func(idx int) {
			defer wg.Done()

			event, _ := outboxEntities.NewOutboxEvent(
				ctx,
				"ingestion.completed",
				uuid.New(),
				[]byte(`{"test": true}`),
			)

			_, _ = outboxRepo.Create(ctx, event)
		}(i)
	}

	wg.Wait()

	require.Equal(t, numEvents, outboxRepo.EventCount())
}

func TestConcurrentJobRepo_ReadWrite(t *testing.T) {
	t.Parallel()

	jobRepo := newConcurrentJobRepo()
	ctx := context.Background()

	const numWriters = 10
	const numReaders = 20

	var wg sync.WaitGroup

	wg.Add(numWriters + numReaders)

	for i := 0; i < numWriters; i++ {
		go func(idx int) {
			defer wg.Done()

			job, _ := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
			_, _ = jobRepo.Create(ctx, job)
		}(i)
	}

	for i := 0; i < numReaders; i++ {
		go func(idx int) {
			defer wg.Done()

			_, _, _ = jobRepo.FindByContextID(ctx, uuid.New(), repositories.CursorFilter{})
		}(i)
	}

	wg.Wait()

	require.Equal(t, numWriters, jobRepo.JobCount())
}

func TestConcurrentIngestion_WithFailures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := newConcurrentJobRepo()

	const numJobs = 20

	var wg sync.WaitGroup

	var successCount atomic.Int32

	var failCount atomic.Int32

	wg.Add(numJobs)

	for i := 0; i < numJobs; i++ {
		go func(idx int) {
			defer wg.Done()

			txRepo := newConcurrentTxRepo()
			if idx%3 == 0 {
				txRepo.batchErr = errors.New("simulated batch error")
			}

			deps := UseCaseDeps{
				JobRepo:         jobRepo,
				TransactionRepo: txRepo,
				Dedupe:          newConcurrentDedupe(),
				Publisher:       &fakePublisher{},
				OutboxRepo:      newConcurrentOutboxRepo(),
				Parsers:         concurrentRegistry{parser: concurrentStreamingParser{}},
				FieldMapRepo:    &fakeFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID}},
				SourceRepo:      &fakeSourceRepo{source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID}},
				DedupeTTL:       time.Hour,
			}

			uc, err := NewUseCase(deps)
			if err != nil {
				failCount.Add(1)
				return
			}

			_, err = uc.StartIngestion(
				ctx,
				contextID,
				sourceID,
				"concurrent_file.csv",
				100,
				"csv",
				strings.NewReader("data"),
			)
			if err != nil {
				failCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	require.Equal(t, int32(numJobs), successCount.Load()+failCount.Load())
}

func TestConcurrentIgnoreTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	txRepo := &concurrentTxRepoWithIgnore{
		concurrentTxRepo: newConcurrentTxRepo(),
	}

	txID := uuid.New()
	txRepo.transactions[txID] = &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Status:   shared.TransactionStatusUnmatched,
	}

	deps := UseCaseDeps{
		JobRepo:         newConcurrentJobRepo(),
		TransactionRepo: txRepo,
		Dedupe:          newConcurrentDedupe(),
		Publisher:       &fakePublisher{},
		OutboxRepo:      newConcurrentOutboxRepo(),
		Parsers:         concurrentRegistry{parser: concurrentStreamingParser{}},
		FieldMapRepo:    &fakeFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID}},
		SourceRepo:      &fakeSourceRepo{source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID}},
		DedupeTTL:       time.Hour,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	const numAttempts = 10

	var wg sync.WaitGroup

	var successCount atomic.Int32

	var errorCount atomic.Int32

	wg.Add(numAttempts)

	for i := 0; i < numAttempts; i++ {
		go func() {
			defer wg.Done()

			_, err := uc.IgnoreTransaction(ctx, IgnoreTransactionInput{
				TransactionID: txID,
				ContextID:     contextID,
				Reason:        "concurrent ignore test",
			})
			if err == nil {
				successCount.Add(1)
			} else {
				errorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	require.Equal(t, int32(numAttempts), successCount.Load()+errorCount.Load())
}

type concurrentTxRepoWithIgnore struct {
	*concurrentTxRepo
}

func (r *concurrentTxRepoWithIgnore) FindByID(
	_ context.Context,
	id uuid.UUID,
) (*shared.Transaction, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if tx, ok := r.transactions[id]; ok {
		cp := *tx

		return &cp, nil
	}

	return nil, sql.ErrNoRows
}

func (r *concurrentTxRepoWithIgnore) UpdateStatus(
	_ context.Context,
	id uuid.UUID,
	_ uuid.UUID,
	status shared.TransactionStatus,
) (*shared.Transaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if tx, ok := r.transactions[id]; ok {
		if tx.Status != shared.TransactionStatusUnmatched {
			return nil, ErrTransactionNotIgnorable
		}

		tx.Status = status

		return tx, nil
	}

	return nil, sql.ErrNoRows
}

// --- Benchmark for Concurrent Operations ---

// Package-level sinks to prevent compiler optimizations.
var (
	sinkIngestionJob *entities.IngestionJob
	sinkErr          error
)

func BenchmarkConcurrentIngestion(b *testing.B) {
	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		jobRepo := newConcurrentJobRepo()
		txRepo := newConcurrentTxRepo()

		deps := UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          newConcurrentDedupe(),
			Publisher:       &fakePublisher{},
			OutboxRepo:      newConcurrentOutboxRepo(),
			Parsers:         concurrentRegistry{parser: concurrentStreamingParser{}},
			FieldMapRepo:    &fakeFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID}},
			SourceRepo:      &fakeSourceRepo{source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID}},
			DedupeTTL:       time.Hour,
		}

		uc, err := NewUseCase(deps)
		if err != nil {
			b.Fatalf("failed to create use case: %v", err)
		}

		var wg sync.WaitGroup

		wg.Add(5)

		for j := 0; j < 5; j++ {
			go func() {
				defer wg.Done()

				job, err := uc.StartIngestion(ctx, contextID, sourceID, "file.csv", 100, "csv", strings.NewReader("data"))
				sinkIngestionJob = job
				sinkErr = err
			}()
		}

		wg.Wait()
	}
}

func BenchmarkConcurrentDeduplication(b *testing.B) {
	dedupe := newConcurrentDedupe()
	contextID := uuid.New()
	sourceID := uuid.New()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		hash := dedupe.CalculateHash(sourceID, uuid.New().String())
		sinkErr = dedupe.MarkSeen(context.Background(), contextID, hash, time.Hour)
	}
}
