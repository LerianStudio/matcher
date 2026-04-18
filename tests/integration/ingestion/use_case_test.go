//go:build integration

package ingestion

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	outboxServices "github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTransactionRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionVO "github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

type noopDedupe struct{}

type noopPublisher struct{}

type fieldMapStub struct {
	fieldMap *shared.FieldMap
}

type capturePublisher struct {
	noopPublisher
	mu        sync.Mutex
	completed int
	failed    int
}

type sourceStub struct {
	contextID uuid.UUID
	sourceID  uuid.UUID
}

type integrationFakeDedupe struct {
	mu         sync.RWMutex
	duplicates map[string]bool
}

type noopMatchPublisher struct{}

func (f *fieldMapStub) FindBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.FieldMap, error) {
	return f.fieldMap, nil
}

func (c *capturePublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *ingestionEntities.IngestionCompletedEvent,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.completed++
	return nil
}

func (c *capturePublisher) PublishIngestionFailed(
	_ context.Context,
	_ *ingestionEntities.IngestionFailedEvent,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failed++
	return nil
}

func newSourceStub(contextID, sourceID uuid.UUID) *sourceStub {
	return &sourceStub{contextID: contextID, sourceID: sourceID}
}

func (s *sourceStub) FindByID(
	_ context.Context,
	contextID, id uuid.UUID,
) (*shared.ReconciliationSource, error) {
	if s.contextID == uuid.Nil || s.sourceID == uuid.Nil {
		return nil, fmt.Errorf("source not found")
	}

	if s.contextID != contextID || s.sourceID != id {
		return nil, fmt.Errorf("source not found")
	}

	return &shared.ReconciliationSource{ID: s.sourceID, ContextID: s.contextID}, nil
}

func (noopDedupe) CalculateHash(sourceID uuid.UUID, externalID string) string {
	return fmt.Sprintf("%s:%s", sourceID.String(), externalID)
}

func (noopDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return false, nil
}

func (noopDedupe) MarkSeen(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ time.Duration,
) error {
	return nil
}

func (noopDedupe) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ time.Duration,
	_ int,
) error {
	return nil
}

func (noopDedupe) Clear(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (noopDedupe) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}

func (noopPublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *ingestionEntities.IngestionCompletedEvent,
) error {
	return nil
}

func (noopPublisher) PublishIngestionFailed(
	_ context.Context,
	_ *ingestionEntities.IngestionFailedEvent,
) error {
	return nil
}

func (f *integrationFakeDedupe) CalculateHash(sourceID uuid.UUID, externalID string) string {
	return sourceID.String() + ":" + externalID
}

func (f *integrationFakeDedupe) IsDuplicate(
	_ context.Context,
	_ uuid.UUID,
	hash string,
) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.duplicates[hash], nil
}

func (f *integrationFakeDedupe) MarkSeen(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.duplicates == nil {
		f.duplicates = map[string]bool{}
	}

	f.duplicates[hash] = true

	return nil
}

func (f *integrationFakeDedupe) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
	_ int,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.duplicates == nil {
		f.duplicates = map[string]bool{}
	}

	if f.duplicates[hash] {
		return ports.ErrDuplicateTransaction
	}

	f.duplicates[hash] = true

	return nil
}

func (f *integrationFakeDedupe) Clear(_ context.Context, _ uuid.UUID, hash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.duplicates, hash)

	return nil
}

func (f *integrationFakeDedupe) ClearBatch(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, hash := range hashes {
		delete(f.duplicates, hash)
	}

	return nil
}

func (n *noopMatchPublisher) PublishMatchConfirmed(
	_ context.Context,
	_ *shared.MatchConfirmedEvent,
) error {
	return nil
}

func (n *noopMatchPublisher) PublishMatchUnmatched(
	_ context.Context,
	_ *shared.MatchUnmatchedEvent,
) error {
	return nil
}

var (
	_ ports.DedupeService                 = (*noopDedupe)(nil)
	_ sharedPorts.IngestionEventPublisher = (*noopPublisher)(nil)
	_ ports.FieldMapRepository            = (*fieldMapStub)(nil)
	_ ports.SourceRepository              = (*sourceStub)(nil)
	_ shared.MatchEventPublisher          = (*noopMatchPublisher)(nil)
)

func TestIntegrationDedupeJobState(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		jobRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTransactionRepo.NewRepository(provider)

		fieldMap := &shared.FieldMap{Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
		}}

		fieldMapRepo := &fieldMapStub{fieldMap: fieldMap}

		dedupe := &integrationFakeDedupe{duplicates: map[string]bool{}}
		publisher := &noopPublisher{}
		registry := parsers.NewParserRegistry()
		registry.Register(parsers.NewCSVParser())

		contextID := h.Seed.ContextID
		sourceID := h.Seed.SourceID

		outbox := integration.NewTestOutboxRepository(t, h.Connection)
		useCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          dedupe,
			Publisher:       publisher,
			OutboxRepo:      outbox,
			Parsers:         registry,
			FieldMapRepo:    fieldMapRepo,
			SourceRepo:      newSourceStub(contextID, sourceID),
			DedupeTTL:       0,
		})
		require.NoError(t, err)

		ctx := context.Background()
		job, err := ingestionEntities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 100)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		_, err = jobRepo.Create(ctx, job)
		require.NoError(t, err)

		dupHash := dedupe.CalculateHash(job.SourceID, "1")
		require.NoError(t, dedupe.MarkSeen(context.Background(), job.ContextID, dupHash, time.Minute))

		csvData := "id,amount,currency,date\n1,10.00,USD,2024-01-01\n2,10.00,USD,2024-01-01\n"
		result, err := useCase.StartIngestion(
			context.Background(),
			contextID,
			sourceID,
			"file.csv",
			int64(len(csvData)),
			"csv",
			strings.NewReader(csvData),
		)
		require.NoError(t, err)
		require.Equal(t, ingestionVO.JobStatusCompleted, result.Status)
		require.Equal(t, 0, result.Metadata.FailedRows)
		require.Equal(t, 2, result.Metadata.TotalRows)
	})
}

func TestIntegrationUploadFlow(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		jobRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTransactionRepo.NewRepository(provider)

		fieldMap := &shared.FieldMap{Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
			"description": "desc",
		}}

		contextID := h.Seed.ContextID
		sourceID := h.Seed.SourceID
		fieldMapRepo := &fieldMapStub{fieldMap: fieldMap}
		sourceRepo := newSourceStub(contextID, sourceID)

		dedupe := &noopDedupe{}
		publisher := &noopPublisher{}
		registry := parsers.NewParserRegistry()
		registry.Register(parsers.NewCSVParser())

		outbox := integration.NewTestOutboxRepository(t, h.Connection)
		useCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          dedupe,
			Publisher:       publisher,
			OutboxRepo:      outbox,
			Parsers:         registry,
			FieldMapRepo:    fieldMapRepo,
			SourceRepo:      sourceRepo,
			DedupeTTL:       0,
		})
		require.NoError(t, err)

		csvData := "id,amount,currency,date,desc\n1,10.00,USD,2024-01-01,payment\n"

		job, err := useCase.StartIngestion(
			context.Background(),
			contextID,
			sourceID,
			"file.csv",
			int64(len(csvData)),
			"csv",
			strings.NewReader(csvData),
		)
		require.NoError(t, err)
		require.Equal(t, ingestionVO.JobStatusCompleted, job.Status)
		require.Equal(t, 1, job.Metadata.TotalRows)
	})
}

func TestIntegrationEventPublication(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		jobRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTransactionRepo.NewRepository(provider)

		fieldMap := &shared.FieldMap{Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
		}}

		fieldMapRepo := &fieldMapStub{fieldMap: fieldMap}

		publisher := &capturePublisher{}
		registry := parsers.NewParserRegistry()
		registry.Register(parsers.NewCSVParser())

		contextID := h.Seed.ContextID
		sourceID := h.Seed.SourceID
		outbox := integration.NewTestOutboxRepository(t, h.Connection)
		useCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          &noopDedupe{},
			Publisher:       publisher,
			OutboxRepo:      outbox,
			Parsers:         registry,
			FieldMapRepo:    fieldMapRepo,
			SourceRepo:      newSourceStub(contextID, sourceID),
			DedupeTTL:       0,
		})
		require.NoError(t, err)

		csvData := "id,amount,currency,date\n1,10.00,USD,2024-01-01\n"

		_, err = useCase.StartIngestion(
			context.Background(),
			contextID,
			sourceID,
			"file.csv",
			int64(len(csvData)),
			"csv",
			strings.NewReader(csvData),
		)
		require.NoError(t, err)
		require.Equal(t, 0, publisher.completed)
		require.Equal(t, 0, publisher.failed)

		pending, err := outbox.ListPending(context.Background(), 10)
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, ingestionEntities.EventTypeIngestionCompleted, pending[0].EventType)
	})
}

func TestIntegrationEventPublicationFailure(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		jobRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTransactionRepo.NewRepository(provider)

		fieldMap := &shared.FieldMap{Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
		}}

		fieldMapRepo := &fieldMapStub{fieldMap: fieldMap}

		publisher := &capturePublisher{}
		registry := parsers.NewParserRegistry()
		registry.Register(parsers.NewCSVParser())

		contextID := h.Seed.ContextID
		sourceID := h.Seed.SourceID
		outbox := integration.NewTestOutboxRepository(t, h.Connection)
		useCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          &noopDedupe{},
			Publisher:       publisher,
			OutboxRepo:      outbox,
			Parsers:         registry,
			FieldMapRepo:    fieldMapRepo,
			SourceRepo:      newSourceStub(contextID, sourceID),
			DedupeTTL:       0,
		})
		require.NoError(t, err)

		csvData := "\"id,amount,currency,date\n1,10.00,USD,2024-01-01\n"

		_, err = useCase.StartIngestion(
			context.Background(),
			contextID,
			sourceID,
			"file.csv",
			int64(len(csvData)),
			"csv",
			strings.NewReader(csvData),
		)
		require.Error(t, err)
		require.Equal(t, 0, publisher.completed)
		require.Equal(t, 0, publisher.failed)

		pending, err := outbox.ListPending(context.Background(), 10)
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, ingestionEntities.EventTypeIngestionFailed, pending[0].EventType)
	})
}

func TestOutboxDispatcherPublishesPending(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		jobRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTransactionRepo.NewRepository(provider)
		outbox := integration.NewTestOutboxRepository(t, h.Connection)

		fieldMap := &shared.FieldMap{Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
		}}

		fieldMapRepo := &fieldMapStub{fieldMap: fieldMap}

		publisher := &capturePublisher{}
		registry := parsers.NewParserRegistry()
		registry.Register(parsers.NewCSVParser())

		contextID := h.Seed.ContextID
		sourceID := h.Seed.SourceID
		useCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          &noopDedupe{},
			Publisher:       publisher,
			OutboxRepo:      outbox,
			Parsers:         registry,
			FieldMapRepo:    fieldMapRepo,
			SourceRepo:      newSourceStub(contextID, sourceID),
			DedupeTTL:       0,
		})
		require.NoError(t, err)

		csvData := "id,amount,currency,date\n1,10.00,USD,2024-01-01\n"

		_, err = useCase.StartIngestion(
			context.Background(),
			contextID,
			sourceID,
			"file.csv",
			int64(len(csvData)),
			"csv",
			strings.NewReader(csvData),
		)
		require.NoError(t, err)

		handlers := outboxServices.NewHandlerRegistry()
		err = handlers.Register(shared.EventTypeIngestionCompleted,
			func(_ context.Context, _ *outboxServices.OutboxEvent) error {
				publisher.mu.Lock()
				publisher.completed++
				publisher.mu.Unlock()
				return nil
			},
		)
		require.NoError(t, err)

		dispatcher, err := outboxServices.NewDispatcher(
			outbox,
			handlers,
			nil,
			noop.NewTracerProvider().Tracer("test"),
		)
		require.NoError(t, err)

		dispatcher.DispatchOnce(context.Background())

		require.Equal(t, 1, publisher.completed)

		pending, err := outbox.ListPending(context.Background(), 10)
		require.NoError(t, err)
		require.Empty(t, pending)
	})
}
