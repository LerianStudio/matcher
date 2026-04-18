//go:build unit

package command

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// countingDedupe wraps the existing mark-seen behavior with a call counter,
// so tests can assert the trusted-stream path reuses the dedupe pipeline.
type countingDedupe struct {
	markSeenCalls atomic.Int64
}

func (d *countingDedupe) CalculateHash(_ uuid.UUID, externalID string) string {
	return "hash-" + externalID
}

func (d *countingDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return false, nil
}

func (d *countingDedupe) MarkSeen(_ context.Context, _ uuid.UUID, _ string, _ time.Duration) error {
	d.markSeenCalls.Add(1)
	return nil
}

func (d *countingDedupe) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ time.Duration,
	_ int,
) error {
	d.markSeenCalls.Add(1)
	return nil
}

func (d *countingDedupe) Clear(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (d *countingDedupe) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}

// fetcherShapedParser emulates parsing Fetcher-shaped JSON content. The specific
// content bytes are irrelevant for this RED-phase cornerstone: the parser returns
// a deterministic transaction so the pipeline can mark it via dedupe.
type fetcherShapedParser struct{}

func (fetcherShapedParser) Parse(
	_ context.Context,
	_ io.Reader,
	job *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: []*shared.Transaction{
			{
				ID:               uuid.New(),
				IngestionJobID:   job.ID,
				SourceID:         job.SourceID,
				ExternalID:       "fetcher-ext-1",
				Amount:           decimal.RequireFromString("42.00"),
				Currency:         "USD",
				Date:             time.Now().UTC(),
				ExtractionStatus: shared.ExtractionStatusComplete,
				Status:           shared.TransactionStatusUnmatched,
			},
		},
		DateRange: &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
	}, nil
}

func (fetcherShapedParser) SupportedFormat() string { return "json" }

// fetcherShapedJSON is a representative Fetcher extraction payload (minimal).
// Used only to prove the trusted-stream intake accepts Fetcher-produced content.
const fetcherShapedJSON = `{"records":[{"external_id":"fetcher-ext-1","amount":"42.00","currency":"USD"}]}`

func TestIngestFromTrustedStream_RedPhaseCornerstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	newValidInput := func() IngestFromTrustedStreamInput {
		return IngestFromTrustedStreamInput{
			ContextID: contextID,
			SourceID:  sourceID,
			Format:    "json",
			Content:   strings.NewReader(fetcherShapedJSON),
			SourceMetadata: map[string]string{
				"fetcher_job_id": "job-abc-123",
			},
		}
	}

	newValidDeps := func() (UseCaseDeps, *countingDedupe) {
		jobRepo := &fakeJobRepo{}
		job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "fetcher-stream", 0)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		jobRepo.created = job

		dedupe := &countingDedupe{}

		deps := newTestDeps()
		deps.JobRepo = jobRepo
		deps.Dedupe = dedupe
		deps.Parsers = fakeRegistry{parser: fetcherShapedParser{}}
		deps.FieldMapRepo = &fakeFieldMapRepo{
			fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
		}
		deps.SourceRepo = &fakeSourceRepo{
			source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
		}

		return deps, dedupe
	}

	t.Run("valid trusted stream produces ingestion job id and reuses dedupe pipeline", func(t *testing.T) {
		t.Parallel()

		deps, dedupe := newValidDeps()
		uc, err := NewUseCase(deps)
		require.NoError(t, err)

		output, err := uc.IngestFromTrustedStream(ctx, newValidInput())
		require.NoError(t, err)
		require.NotNil(t, output)
		require.NotEqual(t, uuid.Nil, output.IngestionJobID, "IngestionJobID must be a non-nil UUID")

		// Prove the trusted-stream path flows through the shared dedup stage.
		require.GreaterOrEqual(
			t,
			dedupe.markSeenCalls.Load(),
			int64(1),
			"dedupe.MarkSeenWithRetry must be invoked at least once to prove pipeline reuse (AC-F2)",
		)
	})

	t.Run("nil content reader returns ErrIngestFromTrustedStreamContentRequired", func(t *testing.T) {
		t.Parallel()

		deps, _ := newValidDeps()
		uc, err := NewUseCase(deps)
		require.NoError(t, err)

		input := newValidInput()
		input.Content = nil

		output, err := uc.IngestFromTrustedStream(ctx, input)
		require.Nil(t, output)
		require.Error(t, err)
		require.True(
			t,
			errors.Is(err, ErrIngestFromTrustedStreamContentRequired),
			"expected ErrIngestFromTrustedStreamContentRequired, got: %v",
			err,
		)
	})

	t.Run("empty source id returns ErrIngestFromTrustedStreamSourceRequired", func(t *testing.T) {
		t.Parallel()

		deps, _ := newValidDeps()
		uc, err := NewUseCase(deps)
		require.NoError(t, err)

		input := newValidInput()
		input.SourceID = uuid.Nil

		output, err := uc.IngestFromTrustedStream(ctx, input)
		require.Nil(t, output)
		require.Error(t, err)
		require.True(
			t,
			errors.Is(err, ErrIngestFromTrustedStreamSourceRequired),
			"expected ErrIngestFromTrustedStreamSourceRequired, got: %v",
			err,
		)
	})
}
