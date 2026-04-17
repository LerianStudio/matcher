// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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
