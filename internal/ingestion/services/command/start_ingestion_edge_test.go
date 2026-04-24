// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

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
