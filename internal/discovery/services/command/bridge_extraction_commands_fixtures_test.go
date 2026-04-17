// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
	extraction sharedPorts.LinkableExtraction,
	jobID uuid.UUID,
) error {
	var extractionID uuid.UUID
	if extraction != nil {
		extractionID = extraction.GetID()
	}

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
