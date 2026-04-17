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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// fakeIntake captures trusted-content calls and returns a staged outcome.
type fakeIntake struct {
	called bool
	input  sharedPorts.TrustedContentInput
	result sharedPorts.TrustedContentOutcome
	err    error
}

func (f *fakeIntake) IngestTrustedContent(
	_ context.Context,
	input sharedPorts.TrustedContentInput,
) (sharedPorts.TrustedContentOutcome, error) {
	f.called = true
	f.input = input

	if f.err != nil {
		return sharedPorts.TrustedContentOutcome{}, f.err
	}

	return f.result, nil
}

// fakeLinkWriter stubs the cross-context link writer.
type fakeLinkWriter struct {
	calls []struct {
		extractionID uuid.UUID
		jobID        uuid.UUID
	}
	err error
}

func (f *fakeLinkWriter) LinkExtractionToIngestion(
	_ context.Context,
	extractionID uuid.UUID,
	jobID uuid.UUID,
) error {
	f.calls = append(f.calls, struct {
		extractionID uuid.UUID
		jobID        uuid.UUID
	}{extractionID, jobID})

	return f.err
}

// fakeSourceResolver returns a staged (source, context, format) target.
type fakeSourceResolver struct {
	target sharedPorts.BridgeSourceTarget
	err    error
	calls  int
}

func (f *fakeSourceResolver) ResolveSourceForConnection(
	_ context.Context,
	_ uuid.UUID,
) (sharedPorts.BridgeSourceTarget, error) {
	f.calls++

	if f.err != nil {
		return sharedPorts.BridgeSourceTarget{}, f.err
	}

	return f.target, nil
}

// bridgeFakeExtractionRepo provides controlled FindByID + LinkIfUnlinked
// semantics for orchestrator tests.
type bridgeFakeExtractionRepo struct {
	entity      *entities.ExtractionRequest
	findErr     error
	linkErr     error
	linkCalls   int
	linkedJobID uuid.UUID
	// custodyDeletedCalls records MarkCustodyDeleted invocations so
	// orchestrator tests can assert the happy-path cleanup is persisted
	// (Polish Fix 1, T-006).
	custodyDeletedCalls   int
	lastCustodyDeletedID  uuid.UUID
	lastCustodyDeletedAt  time.Time
	markCustodyDeletedErr error
}

func (r *bridgeFakeExtractionRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.ExtractionRequest, error) {
	return r.entity, r.findErr
}

func (r *bridgeFakeExtractionRepo) LinkIfUnlinked(
	_ context.Context,
	_ uuid.UUID,
	ingestionJobID uuid.UUID,
) error {
	r.linkCalls++
	r.linkedJobID = ingestionJobID

	return r.linkErr
}

func (r *bridgeFakeExtractionRepo) MarkBridgeFailed(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) MarkBridgeFailedWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) IncrementBridgeAttempts(_ context.Context, _ uuid.UUID, _ int) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) IncrementBridgeAttemptsWithTx(_ context.Context, _ sharedPorts.Tx, _ uuid.UUID, _ int) error {
	return nil
}

// Other methods are unused by the orchestrator directly.
func (r *bridgeFakeExtractionRepo) Create(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) CreateWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) Update(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) UpdateIfUnchanged(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) UpdateIfUnchangedWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) UpdateWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *bridgeFakeExtractionRepo) FindEligibleForBridge(_ context.Context, _ int) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (r *bridgeFakeExtractionRepo) CountBridgeReadiness(_ context.Context, _ time.Duration) (repositories.BridgeReadinessCounts, error) {
	return repositories.BridgeReadinessCounts{}, nil
}

func (r *bridgeFakeExtractionRepo) ListBridgeCandidates(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Time,
	_ uuid.UUID,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (r *bridgeFakeExtractionRepo) MarkCustodyDeleted(
	_ context.Context,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	r.custodyDeletedCalls++
	r.lastCustodyDeletedID = id
	r.lastCustodyDeletedAt = deletedAt

	return r.markCustodyDeletedErr
}

func (r *bridgeFakeExtractionRepo) MarkCustodyDeletedWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	return r.MarkCustodyDeleted(context.Background(), id, deletedAt)
}

func (r *bridgeFakeExtractionRepo) FindBridgeRetentionCandidates(
	_ context.Context,
	_ time.Duration,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

// bridgeFakeCustody records Store/Open/Delete calls.
type bridgeFakeCustody struct {
	ref         *sharedPorts.ArtifactCustodyReference
	storeCalls  int
	openCalls   int
	deleteCalls int
	lastDelRef  sharedPorts.ArtifactCustodyReference
	openErr     error
	deleteErr   error
	openPayload []byte
}

func (c *bridgeFakeCustody) Store(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyWriteInput,
) (*sharedPorts.ArtifactCustodyReference, error) {
	c.storeCalls++

	return c.ref, nil
}

func (c *bridgeFakeCustody) Open(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyReference,
) (io.ReadCloser, error) {
	c.openCalls++

	if c.openErr != nil {
		return nil, c.openErr
	}

	return io.NopCloser(strings.NewReader(string(c.openPayload))), nil
}

func (c *bridgeFakeCustody) Delete(
	_ context.Context,
	ref sharedPorts.ArtifactCustodyReference,
) error {
	c.deleteCalls++
	c.lastDelRef = ref

	return c.deleteErr
}

// stageOrchestrator builds a BridgeExtractionOrchestrator backed by minimal
// stubs for the happy path. Individual tests can swap one stub.
func stageOrchestrator(
	t *testing.T,
	extraction *entities.ExtractionRequest,
	ingestionJobID uuid.UUID,
) (
	*BridgeExtractionOrchestrator,
	*bridgeFakeExtractionRepo,
	*bridgeFakeCustody,
	*fakeIntake,
	*fakeLinkWriter,
	*fakeSourceResolver,
) {
	t.Helper()

	repo := &bridgeFakeExtractionRepo{entity: extraction}

	// The verified-artifact orchestrator is wired with in-memory stubs that
	// produce a fixed custody reference when the pipeline runs. The
	// orchestrator has its own test suite; here we just want it to emit a
	// ref so the bridge can continue.
	custodyRef := &sharedPorts.ArtifactCustodyReference{
		URI:      "custody://tenant/fetcher-artifacts/x.json",
		Key:      "tenant/fetcher-artifacts/x.json",
		SHA256:   "sha",
		StoredAt: time.Now().UTC(),
	}

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: io.NopCloser(strings.NewReader("ciphertext")),
			HMAC:    "hmac",
			IV:      "iv",
		},
	}
	verifier := &fakeTrustVerifier{plaintext: []byte(`{"ds":{"t":[{"k":"v"}]}}`)}
	outerCustody := &fakeCustodyStore{ref: custodyRef}

	verifiedOrch, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, outerCustody)
	require.NoError(t, err)

	fakeCust := &bridgeFakeCustody{
		ref:         custodyRef,
		openPayload: []byte(`{"ds":{"t":[{"k":"v"}]}}`),
	}
	intake := &fakeIntake{
		result: sharedPorts.TrustedContentOutcome{
			IngestionJobID:   ingestionJobID,
			TransactionCount: 1,
		},
	}
	linkWriter := &fakeLinkWriter{}
	resolver := &fakeSourceResolver{
		target: sharedPorts.BridgeSourceTarget{
			SourceID:  uuid.New(),
			ContextID: uuid.New(),
			Format:    "json",
		},
	}

	cfg := BridgeOrchestratorConfig{
		FetcherBaseURLGetter: func() string { return "http://fetcher.local:4006" },
		MaxExtractionBytes:   1 << 20,
		Flatten: func(in io.Reader, _ int64) (io.Reader, error) {
			// In tests, pass-through the custody bytes unchanged — they
			// are already shaped as a flat JSON array in the fixture.
			return in, nil
		},
	}

	orch, err := NewBridgeExtractionOrchestrator(
		repo,
		verifiedOrch,
		fakeCust,
		intake,
		linkWriter,
		resolver,
		cfg,
	)
	require.NoError(t, err)

	return orch, repo, fakeCust, intake, linkWriter, resolver
}

func completeExtractionForBridge(id, connID uuid.UUID) *entities.ExtractionRequest {
	return &entities.ExtractionRequest{
		ID:           id,
		ConnectionID: connID,
		Status:       vo.ExtractionStatusComplete,
		FetcherJobID: "fetcher-job",
		ResultPath:   "/v1/fetcher/jobs/" + id.String() + "/result",
		CreatedAt:    time.Now().UTC().Add(-time.Minute),
		UpdatedAt:    time.Now().UTC(),
	}
}

func TestNewBridgeExtractionOrchestrator_RejectsNilDeps(t *testing.T) {
	t.Parallel()

	repo := &bridgeFakeExtractionRepo{}
	cust := &bridgeFakeCustody{}
	intake := &fakeIntake{}
	lw := &fakeLinkWriter{}
	resolver := &fakeSourceResolver{}

	verifiedOrch, verr := NewVerifiedArtifactRetrievalOrchestrator(
		&fakeRetrievalGateway{},
		&fakeTrustVerifier{},
		&fakeCustodyStore{},
	)
	require.NoError(t, verr)

	_, err := NewBridgeExtractionOrchestrator(nil, verifiedOrch, cust, intake, lw, resolver, BridgeOrchestratorConfig{FetcherBaseURLGetter: func() string { return "x" }})
	require.ErrorIs(t, err, ErrNilBridgeExtractionRepo)

	_, err = NewBridgeExtractionOrchestrator(repo, nil, cust, intake, lw, resolver, BridgeOrchestratorConfig{FetcherBaseURLGetter: func() string { return "x" }})
	require.ErrorIs(t, err, ErrNilBridgeVerifiedArtifactOrchestr)

	_, err = NewBridgeExtractionOrchestrator(repo, verifiedOrch, nil, intake, lw, resolver, BridgeOrchestratorConfig{FetcherBaseURLGetter: func() string { return "x" }})
	require.ErrorIs(t, err, ErrNilBridgeCustody)

	_, err = NewBridgeExtractionOrchestrator(repo, verifiedOrch, cust, nil, lw, resolver, BridgeOrchestratorConfig{FetcherBaseURLGetter: func() string { return "x" }})
	require.ErrorIs(t, err, ErrNilBridgeIntake)

	_, err = NewBridgeExtractionOrchestrator(repo, verifiedOrch, cust, intake, nil, resolver, BridgeOrchestratorConfig{FetcherBaseURLGetter: func() string { return "x" }})
	require.ErrorIs(t, err, ErrNilBridgeLinkWriter)

	_, err = NewBridgeExtractionOrchestrator(repo, verifiedOrch, cust, intake, lw, nil, BridgeOrchestratorConfig{FetcherBaseURLGetter: func() string { return "x" }})
	require.ErrorIs(t, err, ErrNilBridgeSourceResolver)

	_, err = NewBridgeExtractionOrchestrator(repo, verifiedOrch, cust, intake, lw, resolver, BridgeOrchestratorConfig{})
	require.ErrorIs(t, err, ErrNilBridgeFetcherBaseURL)

	// Missing flatten port.
	_, err = NewBridgeExtractionOrchestrator(
		repo, verifiedOrch, cust, intake, lw, resolver,
		BridgeOrchestratorConfig{FetcherBaseURLGetter: func() string { return "x" }},
	)
	require.ErrorIs(t, err, ErrNilBridgeContentFlattener)
}

func TestBridgeExtraction_HappyPath_LinksAndDeletesCustody(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	connID := uuid.New()
	ingestionID := uuid.New()

	extraction := completeExtractionForBridge(extractionID, connID)

	orch, repo, cust, intake, lw, resolver := stageOrchestrator(t, extraction, ingestionID)

	outcome, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
		TenantID:     "tenant-x",
	})
	require.NoError(t, err)
	require.NotNil(t, outcome)

	assert.Equal(t, ingestionID, outcome.IngestionJobID)
	assert.Equal(t, 1, outcome.TransactionCount)
	assert.True(t, outcome.CustodyDeleted, "successful bridge must delete custody")

	assert.Equal(t, 1, resolver.calls, "source resolver called once")
	assert.True(t, intake.called, "ingestion intake called")
	require.Len(t, lw.calls, 1, "link writer called once with the ingestion job id")
	assert.Equal(t, ingestionID, lw.calls[0].jobID)
	assert.Equal(t, extractionID, lw.calls[0].extractionID)
	assert.Equal(t, 1, cust.openCalls)
	assert.Equal(t, 1, cust.deleteCalls)

	// Polish Fix 1 (T-006): the happy-path cleanupCustody hook MUST persist
	// the convergence marker so the retention sweep stops re-examining
	// this row.
	assert.Equal(t, 1, repo.custodyDeletedCalls,
		"happy-path must call MarkCustodyDeleted exactly once")
	assert.Equal(t, extractionID, repo.lastCustodyDeletedID,
		"marker must target the bridged extraction's ID")
	assert.False(t, repo.lastCustodyDeletedAt.IsZero(),
		"marker timestamp must be a real value, not zero-time")
}

func TestBridgeExtraction_MissingExtractionID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	orch, _, _, _, _, _ := stageOrchestrator(
		t,
		completeExtractionForBridge(uuid.New(), uuid.New()),
		uuid.New(),
	)

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		TenantID: "tenant-x",
	})
	require.ErrorIs(t, err, sharedPorts.ErrBridgeExtractionIDRequired)
}

func TestBridgeExtraction_MissingTenantID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	orch, _, _, _, _, _ := stageOrchestrator(
		t,
		completeExtractionForBridge(extractionID, uuid.New()),
		uuid.New(),
	)

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
	})
	require.ErrorIs(t, err, sharedPorts.ErrBridgeTenantIDRequired)
}

func TestBridgeExtraction_NonCompleteExtraction_IsIneligible(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	connID := uuid.New()

	extraction := completeExtractionForBridge(extractionID, connID)
	extraction.Status = vo.ExtractionStatusSubmitted

	orch, _, _, intake, lw, resolver := stageOrchestrator(t, extraction, uuid.New())

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
		TenantID:     "tenant-x",
	})
	require.ErrorIs(t, err, sharedPorts.ErrBridgeExtractionIneligible)

	assert.Equal(t, 0, resolver.calls, "source resolver not called for ineligible")
	assert.False(t, intake.called)
	assert.Len(t, lw.calls, 0)
}

func TestBridgeExtraction_AlreadyLinked_IsIneligible(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	connID := uuid.New()

	extraction := completeExtractionForBridge(extractionID, connID)
	extraction.IngestionJobID = uuid.New() // already linked

	orch, _, _, intake, lw, _ := stageOrchestrator(t, extraction, uuid.New())

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
		TenantID:     "tenant-x",
	})
	require.ErrorIs(t, err, sharedPorts.ErrBridgeExtractionIneligible)

	assert.False(t, intake.called, "ingestion not called when already linked")
	assert.Len(t, lw.calls, 0)
}

func TestBridgeExtraction_SourceUnresolvable_Surfaces(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	connID := uuid.New()

	orch, _, _, intake, lw, resolver := stageOrchestrator(
		t,
		completeExtractionForBridge(extractionID, connID),
		uuid.New(),
	)
	resolver.err = sharedPorts.ErrBridgeSourceUnresolvable

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
		TenantID:     "tenant-x",
	})
	require.ErrorIs(t, err, sharedPorts.ErrBridgeSourceUnresolvable)

	assert.False(t, intake.called, "ingestion not called when source unresolvable")
	assert.Len(t, lw.calls, 0)
}

func TestBridgeExtraction_IngestionError_PropagatesTransient(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	connID := uuid.New()

	orch, _, _, intake, lw, _ := stageOrchestrator(
		t,
		completeExtractionForBridge(extractionID, connID),
		uuid.New(),
	)
	intake.err = errors.New("rabbit unreachable")

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
		TenantID:     "tenant-x",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rabbit unreachable")

	assert.True(t, intake.called)
	assert.Len(t, lw.calls, 0, "link writer not called when ingestion fails")
}

func TestBridgeExtraction_LinkAlreadyLinked_TreatedAsIdempotent(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	connID := uuid.New()
	ingestionID := uuid.New()

	orch, _, cust, intake, lw, _ := stageOrchestrator(
		t,
		completeExtractionForBridge(extractionID, connID),
		ingestionID,
	)
	lw.err = sharedPorts.ErrExtractionAlreadyLinked

	outcome, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
		TenantID:     "tenant-x",
	})
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)
	require.NotNil(t, outcome, "outcome returned for caller audit even on already-linked")
	assert.Equal(t, ingestionID, outcome.IngestionJobID)

	assert.True(t, intake.called)
	// Custody delete is NOT called on the idempotent path because the link
	// write technically failed; a retention sweep picks up the orphan.
	assert.Equal(t, 0, cust.deleteCalls)
}

func TestBridgeExtraction_EmptyFetcherBaseURL_FailsRetrieval(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	connID := uuid.New()

	orch, _, _, intake, _, _ := stageOrchestrator(
		t,
		completeExtractionForBridge(extractionID, connID),
		uuid.New(),
	)

	orch.cfg.FetcherBaseURLGetter = func() string { return "" }

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: extractionID,
		TenantID:     "tenant-x",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
	assert.False(t, intake.called)
}

func TestBridgeExtraction_ExtractionNotFound_SurfacesSentinel(t *testing.T) {
	t.Parallel()

	orch, repo, _, intake, _, _ := stageOrchestrator(
		t,
		completeExtractionForBridge(uuid.New(), uuid.New()),
		uuid.New(),
	)
	repo.findErr = repositories.ErrExtractionNotFound

	_, err := orch.BridgeExtraction(context.Background(), sharedPorts.BridgeExtractionInput{
		ExtractionID: uuid.New(),
		TenantID:     "tenant-x",
	})
	require.ErrorIs(t, err, repositories.ErrExtractionNotFound)
	assert.False(t, intake.called)
}
