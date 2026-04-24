// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
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
