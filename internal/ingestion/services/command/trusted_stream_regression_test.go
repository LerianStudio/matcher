// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// TestStartIngestion_UploadPath_SurvivesTrustedStreamRefactor is a smoke test
// that proves the StartIngestion (upload) path still orchestrates the same
// prepare → process → complete pipeline that IngestFromTrustedStream now
// shares with it. If the extraction of the shared helpers had broken the
// upload path, the happy-path here would fail first.
func TestStartIngestion_UploadPath_SurvivesTrustedStreamRefactor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "upload.csv", 42)
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

	result, err := uc.StartIngestion(
		ctx,
		contextID,
		sourceID,
		"upload.csv",
		42,
		"csv",
		strings.NewReader("stub content — parser is mocked"),
	)
	require.NoError(t, err)
	require.NotNil(t, result, "upload path must still return a completed job")
}

// TestIngestFromTrustedStream_UnsupportedFormat_ReturnsSentinel exercises the
// format-unsupported sentinel path so the registry-lookup guard is not dead
// code. The fakeRegistry in newTestDeps always returns a parser; failing
// registry lookup is modelled with a dedicated failingRegistry.
func TestIngestFromTrustedStream_UnsupportedFormat_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.Parsers = failingRegistry{}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	output, err := uc.IngestFromTrustedStream(context.Background(), IngestFromTrustedStreamInput{
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Format:    "unsupported-format",
		Content:   strings.NewReader(`[]`),
	})
	require.Nil(t, output)
	require.ErrorIs(t, err, ErrIngestFromTrustedStreamFormatUnsupported)
}

// TestIngestFromTrustedStream_MissingContext_ReturnsSentinel exercises the
// context-id invariant that the cornerstone RED test does not cover directly.
func TestIngestFromTrustedStream_MissingContext_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	output, err := uc.IngestFromTrustedStream(context.Background(), IngestFromTrustedStreamInput{
		ContextID: uuid.Nil,
		SourceID:  uuid.New(),
		Format:    "csv",
		Content:   strings.NewReader(""),
	})
	require.Nil(t, output)
	require.ErrorIs(t, err, ErrIngestFromTrustedStreamContextRequired)
}

// TestIngestFromTrustedStream_EmptyFormat_ReturnsSentinel exercises the
// format-required invariant.
func TestIngestFromTrustedStream_EmptyFormat_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	output, err := uc.IngestFromTrustedStream(context.Background(), IngestFromTrustedStreamInput{
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Format:    "   ",
		Content:   strings.NewReader(""),
	})
	require.Nil(t, output)
	require.ErrorIs(t, err, ErrIngestFromTrustedStreamFormatRequired)
}

// TestIngestFromTrustedStream_NilUseCase_ReturnsSentinel guards against
// calling the method on a nil receiver (matches the StartIngestion pattern).
func TestIngestFromTrustedStream_NilUseCase_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	output, err := uc.IngestFromTrustedStream(context.Background(), IngestFromTrustedStreamInput{})
	require.Nil(t, output)
	require.ErrorIs(t, err, ErrNilUseCase)
}

// TestIngestFromTrustedStream_PrepareFailure_WrapsError exercises the
// prepare-stage error branch of runTrustedStreamPipeline by pointing the
// SourceRepo at a missing source. Covers the `prepare trusted stream
// ingestion: %w` wrap (AC-Q1: pipeline error isolation).
func TestIngestFromTrustedStream_PrepareFailure_WrapsError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	deps := newTestDeps()
	deps.Parsers = fakeRegistry{parser: fetcherShapedParser{}}
	deps.SourceRepo = &fakeSourceRepo{source: nil} // triggers ErrSourceNotFound
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	output, err := uc.IngestFromTrustedStream(ctx, IngestFromTrustedStreamInput{
		ContextID: contextID,
		SourceID:  sourceID,
		Format:    "json",
		Content:   strings.NewReader(fetcherShapedJSON),
	})

	require.Nil(t, output)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSourceNotFound)
	require.Contains(
		t,
		err.Error(),
		"prepare trusted stream ingestion",
		"error must be wrapped by the trusted-stream pipeline prefix",
	)
}

// TestIngestFromTrustedStream_ProcessFailure_ReturnsPipelineError exercises
// the processing-stage error branch of runTrustedStreamPipeline by injecting
// a parser that always fails. The pipeline must call failJob and surface a
// non-nil error (AC-Q1).
func TestIngestFromTrustedStream_ProcessFailure_ReturnsPipelineError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "fetcher-stream", 0)
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

	output, err := uc.IngestFromTrustedStream(ctx, IngestFromTrustedStreamInput{
		ContextID: contextID,
		SourceID:  sourceID,
		Format:    "json",
		Content:   strings.NewReader(fetcherShapedJSON),
	})

	require.Nil(t, output)
	require.Error(t, err, "processing failure must propagate")
	require.Contains(t, err.Error(), "parse", "error should reference parser failure")
}

// TestIngestFromTrustedStream_CompleteFailure_WrapsError exercises the
// completion-stage error branch of runTrustedStreamPipeline by failing the
// outbox create call. Covers the `complete trusted stream ingestion: %w`
// wrap (AC-Q1: pipeline error isolation).
func TestIngestFromTrustedStream_CompleteFailure_WrapsError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobRepo := &fakeJobRepo{}
	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "fetcher-stream", 0)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	jobRepo.created = job

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	deps.OutboxRepo = &fakeOutboxRepo{createErr: errOutbox}
	deps.Parsers = fakeRegistry{parser: fetcherShapedParser{}}
	deps.FieldMapRepo = &fakeFieldMapRepo{
		fieldMap: &shared.FieldMap{ID: uuid.New(), SourceID: sourceID},
	}
	deps.SourceRepo = &fakeSourceRepo{
		source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	output, err := uc.IngestFromTrustedStream(ctx, IngestFromTrustedStreamInput{
		ContextID: contextID,
		SourceID:  sourceID,
		Format:    "json",
		Content:   strings.NewReader(fetcherShapedJSON),
	})

	require.Nil(t, output)
	require.Error(t, err, "completion failure must propagate")
	require.Contains(
		t,
		err.Error(),
		"complete trusted stream ingestion",
		"error should include completion stage prefix",
	)
}

// TestResolveTrustedStreamFileName_MetadataOverride verifies the metadata
// filename override is wired through.
func TestResolveTrustedStreamFileName_MetadataOverride(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		metadata map[string]string
		expected string
	}{
		{"nil metadata", nil, defaultTrustedStreamFileName},
		{"empty metadata", map[string]string{}, defaultTrustedStreamFileName},
		{"filename set", map[string]string{"filename": "custom.csv"}, "custom.csv"},
		{"filename whitespace", map[string]string{"filename": "   "}, defaultTrustedStreamFileName},
		{"filename trimmed", map[string]string{"filename": "  trimmed.csv  "}, "trimmed.csv"},
		{"unrelated keys", map[string]string{"fetcher_job_id": "abc"}, defaultTrustedStreamFileName},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, resolveTrustedStreamFileName(tc.metadata))
		})
	}
}
