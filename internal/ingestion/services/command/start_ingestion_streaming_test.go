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
