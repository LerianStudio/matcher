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

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// TestIngestFromTrustedStream_OrphanPrevention_ShortCircuitsOnRetry is the
// T-005 P1 cornerstone test: a second IngestTrustedContent call for the
// same extraction_id MUST return the existing IngestionJob without creating
// a new one. Without this short-circuit, the second tick creates an empty
// duplicate job (the original orphan-job bug).
func TestIngestFromTrustedStream_OrphanPrevention_ShortCircuitsOnRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()
	extractionID := uuid.New()
	priorJobID := uuid.New()

	priorJob, err := entities.NewIngestionJob(ctx, contextID, sourceID, "fetcher-stream", 0)
	require.NoError(t, err)
	priorJob.ID = priorJobID
	priorJob.Metadata.ExtractionID = extractionID.String()
	priorJob.Metadata.TotalRows = 100

	jobRepo := &fakeJobRepo{
		byExtraction: map[string]*entities.IngestionJob{
			extractionID.String(): priorJob,
		},
	}

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

	output, err := uc.IngestFromTrustedStream(ctx, sharedPorts.TrustedContentInput{
		ContextID: contextID,
		SourceID:  sourceID,
		Format:    "json",
		Content:   strings.NewReader(`{"records":[]}`),
		SourceMetadata: map[string]string{
			"extraction_id": extractionID.String(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, output)

	// Short-circuit: the prior job's ID is returned, NOT a freshly-created
	// one. TransactionCount mirrors the prior job's TotalRows.
	assert.Equal(t, priorJobID, output.IngestionJobID, "must reuse existing job")
	assert.Equal(t, 100, output.TransactionCount)

	// FindLatestByExtractionID was the only repo call; no Create was
	// invoked.
	assert.Equal(t, 1, jobRepo.findByExtractionCall)
	assert.Nil(t, jobRepo.created, "Create must NOT be called when short-circuit hits")
}

// TestFindExistingTrustedStreamJob_NoExtractionID_ReturnsNil asserts the
// short-circuit helper ONLY applies when the bridge stamps an extraction_id.
// A regular trusted-stream call without the metadata key returns (nil, nil)
// from the helper so the pipeline proceeds with normal ingest (preserving
// backward compatibility for non-bridge callers).
//
// Renamed from TestIngestFromTrustedStream_NoExtractionID_DoesNotShortCircuit
// in Polish Fix 8 because it never actually called IngestFromTrustedStream —
// the assertion findByExtractionCall == 0 was tautological (counter starts at
// 0). The helper-level direct test is the meaningful coverage.
func TestFindExistingTrustedStreamJob_NoExtractionID_ReturnsNil(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	jobRepo := &fakeJobRepo{}
	deps := newTestDeps()
	deps.JobRepo = jobRepo

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	// Direct unit test of the helper: no extraction_id key in metadata →
	// helper returns (nil, nil) without touching the repo.
	got, err := uc.findExistingTrustedStreamJob(ctx, map[string]string{"filename": "x"})
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, 0, jobRepo.findByExtractionCall, "repo lookup must NOT happen without extraction_id")
}

// TestFindExistingTrustedStreamJob_PriorFailedJob_ProceedsToCreate is the
// service-layer counterpart to the SQL-layer Polish Fix 1 test. The repo
// (post-fix) returns nil when the only prior row is FAILED — the SQL filter
// excludes it. The pipeline must then proceed to create a fresh job rather
// than short-circuit into linking against a failed remnant.
//
// We model this by NOT seeding the byExtraction map, simulating the post-fix
// repo behavior (FAILED row excluded by status='COMPLETED' predicate). The
// helper returns (nil, nil); the orchestrator caller proceeds with normal
// ingest.
func TestFindExistingTrustedStreamJob_PriorFailedJob_ProceedsToCreate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	extractionID := uuid.New()

	// Empty byExtraction map = repo returns nil = simulates SQL filter
	// excluding a FAILED prior job. Without Polish Fix 1's status='COMPLETED'
	// filter, the repo would have returned a FAILED job here, and the
	// short-circuit would have linked the extraction to it.
	jobRepo := &fakeJobRepo{}
	deps := newTestDeps()
	deps.JobRepo = jobRepo
	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	got, err := uc.findExistingTrustedStreamJob(ctx, map[string]string{
		"extraction_id": extractionID.String(),
	})
	require.NoError(t, err)
	assert.Nil(t, got, "no COMPLETED prior job → helper returns nil → caller proceeds to create fresh job")
	assert.Equal(t, 1, jobRepo.findByExtractionCall, "repo IS consulted (then returns nil)")
}

// TestIngestFromTrustedStream_MalformedExtractionID_FallsThrough asserts
// the helper treats invalid uuid strings as "no metadata" so the normal
// pipeline proceeds. The bridge's classifier will pick up the resulting
// non-idempotent failure on retry.
func TestIngestFromTrustedStream_MalformedExtractionID_FallsThrough(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	jobRepo := &fakeJobRepo{}
	deps := newTestDeps()
	deps.JobRepo = jobRepo
	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	got, err := uc.findExistingTrustedStreamJob(ctx, map[string]string{
		"extraction_id": "not-a-uuid",
	})
	require.NoError(t, err)
	assert.Nil(t, got)
	// The malformed input short-circuits BEFORE the repo call.
	assert.Equal(t, 0, jobRepo.findByExtractionCall)
}

// TestIngestFromTrustedStream_RepoError_PropagatesAndAborts.
func TestIngestFromTrustedStream_RepoError_PropagatesAndAborts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := errors.New("db down")
	jobRepo := &fakeJobRepo{findByExtractionErr: wantErr}

	deps := newTestDeps()
	deps.JobRepo = jobRepo
	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.findExistingTrustedStreamJob(ctx, map[string]string{
		"extraction_id": uuid.NewString(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, wantErr))
}
