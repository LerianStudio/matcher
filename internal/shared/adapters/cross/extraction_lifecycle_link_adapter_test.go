// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	discoveryEntities "github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	discoveryRepositories "github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errLinkWriterBackend is a sentinel error used for simulating downstream
// failure scenarios in the extraction lifecycle link adapter tests.
var errLinkWriterBackend = errors.New("link writer backend failure")

// fakeExtractionRepo is a compact manual mock for the ExtractionRepository
// interface. The T-003 hardening uses the atomic LinkIfUnlinked method
// exclusively for the write path; FindByID is still called for
// state-machine domain validation.
type fakeExtractionRepo struct {
	findResult     *discoveryEntities.ExtractionRequest
	findErr        error
	updateErr      error
	updateCall     *discoveryEntities.ExtractionRequest
	linkErr        error
	linkCallExID   uuid.UUID
	linkCallJobID  uuid.UUID
	linkCallCount  int
	eligibleResult []*discoveryEntities.ExtractionRequest
	eligibleErr    error
}

func (repo *fakeExtractionRepo) Create(_ context.Context, _ *discoveryEntities.ExtractionRequest) error {
	return nil
}

func (repo *fakeExtractionRepo) CreateWithTx(_ context.Context, _ sharedPorts.Tx, _ *discoveryEntities.ExtractionRequest) error {
	return nil
}

func (repo *fakeExtractionRepo) Update(_ context.Context, req *discoveryEntities.ExtractionRequest) error {
	repo.updateCall = req

	return repo.updateErr
}

func (repo *fakeExtractionRepo) UpdateIfUnchanged(
	_ context.Context,
	_ *discoveryEntities.ExtractionRequest,
	_ time.Time,
) error {
	return nil
}

func (repo *fakeExtractionRepo) UpdateIfUnchangedWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ *discoveryEntities.ExtractionRequest,
	_ time.Time,
) error {
	return nil
}

func (repo *fakeExtractionRepo) UpdateWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ *discoveryEntities.ExtractionRequest,
) error {
	return nil
}

func (repo *fakeExtractionRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*discoveryEntities.ExtractionRequest, error) {
	return repo.findResult, repo.findErr
}

func (repo *fakeExtractionRepo) LinkIfUnlinked(
	_ context.Context,
	extractionID uuid.UUID,
	ingestionJobID uuid.UUID,
) error {
	repo.linkCallCount++
	repo.linkCallExID = extractionID
	repo.linkCallJobID = ingestionJobID

	return repo.linkErr
}

func (repo *fakeExtractionRepo) MarkBridgeFailed(
	_ context.Context,
	_ *discoveryEntities.ExtractionRequest,
) error {
	return nil
}

func (repo *fakeExtractionRepo) MarkBridgeFailedWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ *discoveryEntities.ExtractionRequest,
) error {
	return nil
}

func (repo *fakeExtractionRepo) IncrementBridgeAttempts(
	_ context.Context,
	_ uuid.UUID,
	_ int,
) error {
	return nil
}

func (repo *fakeExtractionRepo) IncrementBridgeAttemptsWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ uuid.UUID,
	_ int,
) error {
	return nil
}

func (repo *fakeExtractionRepo) FindEligibleForBridge(
	_ context.Context,
	_ int,
) ([]*discoveryEntities.ExtractionRequest, error) {
	return repo.eligibleResult, repo.eligibleErr
}

func (repo *fakeExtractionRepo) CountBridgeReadiness(
	_ context.Context,
	_ time.Duration,
) (discoveryRepositories.BridgeReadinessCounts, error) {
	return discoveryRepositories.BridgeReadinessCounts{}, nil
}

func (repo *fakeExtractionRepo) ListBridgeCandidates(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Time,
	_ uuid.UUID,
	_ int,
) ([]*discoveryEntities.ExtractionRequest, error) {
	return nil, nil
}

// completeExtraction builds a COMPLETE extraction suitable for linking.
// Domain validation requires Status=COMPLETE, so tests that want the link
// path to succeed must stage one.
func completeExtraction(id uuid.UUID) *discoveryEntities.ExtractionRequest {
	return &discoveryEntities.ExtractionRequest{
		ID:           id,
		ConnectionID: uuid.New(),
		Status:       vo.ExtractionStatusComplete,
		FetcherJobID: "fetcher-job",
		ResultPath:   "/path/to/result.json",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
}

func TestNewExtractionLifecycleLinkWriterAdapter_RejectsNilRepo(t *testing.T) {
	t.Parallel()

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(nil)
	require.Nil(t, adapter)
	require.ErrorIs(t, err, sharedPorts.ErrNilExtractionLifecycleLinkWriter)
}

func TestLinkExtractionToIngestion_HappyPath_CallsAtomicLink(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	ingestionID := uuid.New()

	repo := &fakeExtractionRepo{
		findResult: completeExtraction(extractionID),
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, ingestionID)
	require.NoError(t, err)

	require.Equal(t, 1, repo.linkCallCount, "atomic link must be called exactly once")
	require.Equal(t, extractionID, repo.linkCallExID)
	require.Equal(t, ingestionID, repo.linkCallJobID)
	require.Nil(t, repo.updateCall, "legacy Update must not be called anymore")
}

func TestLinkExtractionToIngestion_AtomicAlreadyLinked_ReturnsIdempotencySentinel(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()

	// The repository has a COMPLETE extraction with NO in-memory ingestion
	// job id (FindByID returned an unlinked snapshot), but the atomic SQL
	// UPDATE sees ingestion_job_id IS NOT NULL (concurrent writer beat us).
	repo := &fakeExtractionRepo{
		findResult: completeExtraction(extractionID),
		linkErr:    sharedPorts.ErrExtractionAlreadyLinked,
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, uuid.New())
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)
	require.Equal(t, 1, repo.linkCallCount)
}

func TestLinkExtractionToIngestion_NonCompleteStatus_RejectedAtDomain(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()

	// Extraction in SUBMITTED (not COMPLETE) must be rejected at the
	// domain layer before the atomic SQL is even attempted.
	extraction := completeExtraction(extractionID)
	extraction.Status = vo.ExtractionStatusSubmitted

	repo := &fakeExtractionRepo{findResult: extraction}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, uuid.New())
	require.ErrorIs(t, err, discoveryEntities.ErrInvalidTransition)
	require.Equal(t, 0, repo.linkCallCount, "atomic link must not be called when domain rejects")
}

func TestLinkExtractionToIngestion_FailedExtraction_RejectedAtDomain(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()

	extraction := completeExtraction(extractionID)
	extraction.Status = vo.ExtractionStatusFailed

	repo := &fakeExtractionRepo{findResult: extraction}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, uuid.New())
	require.ErrorIs(t, err, discoveryEntities.ErrInvalidTransition)
	require.Equal(t, 0, repo.linkCallCount)
}

func TestLinkExtractionToIngestion_NotFound_SurfacesSentinel(t *testing.T) {
	t.Parallel()

	repo := &fakeExtractionRepo{
		findErr: discoveryRepositories.ErrExtractionNotFound,
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, discoveryRepositories.ErrExtractionNotFound)
	require.Equal(t, 0, repo.linkCallCount)
}

func TestLinkExtractionToIngestion_MissingExtractionID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(&fakeExtractionRepo{}),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.Nil, uuid.New())
	require.ErrorIs(t, err, sharedPorts.ErrLinkExtractionIDRequired)
}

func TestLinkExtractionToIngestion_MissingIngestionJobID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(&fakeExtractionRepo{}),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.Nil)
	require.ErrorIs(t, err, sharedPorts.ErrLinkIngestionJobIDRequired)
}

func TestLinkExtractionToIngestion_AtomicLinkError_WrapsUnderlying(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	repo := &fakeExtractionRepo{
		findResult: completeExtraction(extractionID),
		linkErr:    errLinkWriterBackend,
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, errLinkWriterBackend)
	require.Contains(t, err.Error(), "persist extraction link")
}

// TestLinkExtractionToIngestion_NilAdapter_ReturnsSentinel exercises the
// defensive nil-receiver guard that mirrors the intake-adapter behavior.
func TestLinkExtractionToIngestion_NilAdapter_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var adapter *ExtractionLifecycleLinkWriterAdapter

	err := adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, sharedPorts.ErrNilExtractionLifecycleLinkWriter)
}

// TestLinkExtractionToIngestion_FindByIDNonSentinelError_WrapsError
// exercises the FindByID error branch where the underlying error is NOT the
// ErrExtractionNotFound sentinel. The adapter must wrap it with the
// "load extraction for link" prefix.
func TestLinkExtractionToIngestion_FindByIDNonSentinelError_WrapsError(t *testing.T) {
	t.Parallel()

	repo := &fakeExtractionRepo{findErr: errLinkWriterBackend}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, errLinkWriterBackend)
	require.Contains(t, err.Error(), "load extraction for link")
}

// TestLinkExtractionToIngestion_FindByIDReturnsNil_ReturnsNotFoundSentinel
// exercises the branch where FindByID returns (nil, nil) — i.e. "not found"
// expressed via a nil result rather than the sentinel error.
func TestLinkExtractionToIngestion_FindByIDReturnsNil_ReturnsNotFoundSentinel(t *testing.T) {
	t.Parallel()

	repo := &fakeExtractionRepo{findResult: nil, findErr: nil}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, discoveryRepositories.ErrExtractionNotFound)
}

// TestLinkExtractionToIngestion_ReplayWithSameJobID_AtomicSQLReturnsAlreadyLinked
// exercises the replay path: the in-memory entity is already linked to the
// SAME ingestion job id, so LinkToIngestion is a no-op (the domain treats
// same-id replays as idempotent). The adapter then proceeds to the atomic
// SQL guard, which observes the row as already linked and returns
// ErrExtractionAlreadyLinked — which the adapter forwards verbatim.
//
// Renamed from the prior "_SucceedsViaDomainSkip" suffix after Fix 6: the
// fall-through branch no longer exists because the domain's same-id path
// is a no-op (returns nil), not a rejection that the adapter has to
// special-case. The atomic SQL is the single authority for the
// already-linked verdict.
func TestLinkExtractionToIngestion_ReplayWithSameJobID_AtomicSQLReturnsAlreadyLinked(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	ingestionID := uuid.New()

	extraction := completeExtraction(extractionID)
	extraction.IngestionJobID = ingestionID // already linked to same job

	repo := &fakeExtractionRepo{
		findResult: extraction,
		linkErr:    sharedPorts.ErrExtractionAlreadyLinked,
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, ingestionID)
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)
	require.Equal(t, 1, repo.linkCallCount, "atomic link is still attempted for replay-same-id case")
}

// TestLinkExtractionToIngestion_CrossJobCollision_RejectedAsAlreadyLinked
// exercises the cross-job collision path: the in-memory entity is already
// linked to a DIFFERENT ingestion job id. The domain method now wraps this
// case as sharedPorts.ErrExtractionAlreadyLinked (Fix 6 + Fix 6a), so the
// adapter never reaches the atomic SQL — the rejection happens at the
// domain layer with the canonical sentinel that callers can errors.Is on.
func TestLinkExtractionToIngestion_CrossJobCollision_RejectedAsAlreadyLinked(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	priorJobID := uuid.New()
	newJobID := uuid.New()

	extraction := completeExtraction(extractionID)
	extraction.IngestionJobID = priorJobID // already linked to a DIFFERENT job

	repo := &fakeExtractionRepo{findResult: extraction}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, newJobID)
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)
	require.Equal(t, 0, repo.linkCallCount, "atomic link must not be called when domain rejects cross-job collision")
}
