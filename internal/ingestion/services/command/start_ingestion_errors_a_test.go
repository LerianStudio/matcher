// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

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

func (f fakeDedupeWithDuplicate) MarkSeenBulk(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
	_ time.Duration,
) (map[string]bool, error) {
	result := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		result[h] = h != f.duplicateHash
	}

	return result, nil
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
