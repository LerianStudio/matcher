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
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

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
