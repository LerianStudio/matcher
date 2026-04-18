// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestEnsureBridgeOperational_NilBundle_Fails asserts that callers with a
// nil bundle pointer get a clear, actionable error.
func TestEnsureBridgeOperational_NilBundle_Fails(t *testing.T) {
	t.Parallel()

	err := EnsureBridgeOperational(nil)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "bridge adapter bundle is nil")
}

// TestEnsureBridgeOperational_MissingIntake_Fails ensures the T-001
// intake adapter must be present.
func TestEnsureBridgeOperational_MissingIntake_Fails(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{}

	err := EnsureBridgeOperational(bundle)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "intake adapter is not wired")
}

// TestEnsureBridgeOperational_MissingLinkWriter_Fails ensures the T-001
// lifecycle link writer must be present.
func TestEnsureBridgeOperational_MissingLinkWriter_Fails(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{
		Intake: &stubBridgeIntake{},
	}

	err := EnsureBridgeOperational(bundle)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "lifecycle link writer is not wired")
}

// TestEnsureBridgeOperational_MissingOrchestrator_FailsWithContext
// verifies the canonical T-003 P4 precondition: the bridge cannot start
// without the T-002 verified-artifact orchestrator (which implies
// APP_ENC_KEY set and object storage reachable).
func TestEnsureBridgeOperational_MissingOrchestrator_FailsWithContext(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{
		Intake:    &stubBridgeIntake{},
		LinkWrite: &stubBridgeLinkWriter{},
	}

	err := EnsureBridgeOperational(bundle)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "verified-artifact orchestrator is nil",
		"must call out the crypto precondition explicitly")
}

// TestEnsureBridgeOperational_MissingArtifactCustody_ReturnsError covers
// C23: NewBridgeExtractionOrchestrator requires a non-nil custody store,
// so the operational gate must reject a bundle whose ArtifactCustody is
// nil. Without this check, bootstrap would fail later at orchestrator
// construction with a less informative error.
func TestEnsureBridgeOperational_MissingArtifactCustody_ReturnsError(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{
		Intake:                       &stubBridgeIntake{},
		LinkWrite:                    &stubBridgeLinkWriter{},
		VerifiedArtifactOrchestrator: &discoveryCommand.VerifiedArtifactRetrievalOrchestrator{},
		// ArtifactCustody intentionally nil.
	}

	err := EnsureBridgeOperational(bundle)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "artifact custody store")
}

// TestEnsureBridgeOperational_AllWired_Succeeds asserts that a fully-
// wired bundle — including the C23 ArtifactCustody check — passes the
// operational gate.
func TestEnsureBridgeOperational_AllWired_Succeeds(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{
		Intake:                       &stubBridgeIntake{},
		LinkWrite:                    &stubBridgeLinkWriter{},
		VerifiedArtifactOrchestrator: &discoveryCommand.VerifiedArtifactRetrievalOrchestrator{},
		ArtifactCustody:              &stubBridgeCustody{},
	}

	err := EnsureBridgeOperational(bundle)
	require.NoError(t, err)
}

// TestInitFetcherBridgeWorker_EnabledButBundleNil_ReturnsError covers C30:
// when FETCHER_ENABLED=true but the bridge adapter bundle is nil, the
// init function must surface the integration bug as a hard error. The
// prior behaviour returned (nil, nil) with only a warn log, which
// silently disabled the bridge without operator visibility.
func TestInitFetcherBridgeWorker_EnabledButBundleNil_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	worker, err := initFetcherBridgeWorker(
		context.Background(),
		cfg,
		nil, // configGetter
		nil, // provider
		nil, // extractionRepo
		nil, // tenantLister
		nil, // bundle
		&libLog.NopLogger{},
	)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "bridge adapter bundle is nil")
	assert.Nil(t, worker)
}

// TestInitFetcherBridgeWorker_Disabled_ReturnsNilNil pins the
// "Fetcher disabled" happy path: when cfg.Fetcher.Enabled is false, init
// must return (nil, nil) without inspecting any upstream dependency.
// C30 hard-fail must NOT regress this carve-out.
func TestInitFetcherBridgeWorker_Disabled_ReturnsNilNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{} // Fetcher.Enabled defaults to false.

	worker, err := initFetcherBridgeWorker(
		context.Background(),
		cfg,
		nil, nil, nil, nil, nil, // all deps nil — must not matter when disabled.
		&libLog.NopLogger{},
	)
	require.NoError(t, err)
	assert.Nil(t, worker)
}

// TestInitCustodyRetentionWorker_EnabledButBundleNil_ReturnsError covers
// the C30 hard-fail path for the retention worker. Previously the worker
// silently declined to start when the custody store was absent; operators
// could end up with orphan custody objects accumulating indefinitely.
func TestInitCustodyRetentionWorker_EnabledButBundleNil_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	worker, err := initCustodyRetentionWorker(
		context.Background(),
		cfg,
		nil, // extractionRepo
		nil, // tenantLister
		nil, // provider
		nil, // bundle
		&libLog.NopLogger{},
	)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "artifact custody store")
	assert.Nil(t, worker)
}

// TestInitCustodyRetentionWorker_Disabled_ReturnsNilNil pins the
// "Fetcher disabled" happy path for the retention worker. Same reasoning
// as the bridge worker's disabled test.
func TestInitCustodyRetentionWorker_Disabled_ReturnsNilNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{} // Fetcher.Enabled defaults to false.

	worker, err := initCustodyRetentionWorker(
		context.Background(),
		cfg,
		nil, nil, nil, nil, // all deps nil — must not matter when disabled.
		&libLog.NopLogger{},
	)
	require.NoError(t, err)
	assert.Nil(t, worker)
}

// --- test doubles ---

type stubBridgeIntake struct{}

func (s *stubBridgeIntake) IngestTrustedContent(
	_ context.Context,
	_ sharedPorts.TrustedContentInput,
) (sharedPorts.TrustedContentOutcome, error) {
	return sharedPorts.TrustedContentOutcome{}, nil
}

type stubBridgeLinkWriter struct{}

func (s *stubBridgeLinkWriter) LinkExtractionToIngestion(
	_ context.Context,
	_ sharedPorts.LinkableExtraction,
	_ uuid.UUID,
) error {
	return nil
}

type stubBridgeCustody struct{}

func (s *stubBridgeCustody) Store(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyWriteInput,
) (*sharedPorts.ArtifactCustodyReference, error) {
	return nil, nil //nolint:nilnil // test stub — never called in bootstrap tests
}

func (s *stubBridgeCustody) Open(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyReference,
) (io.ReadCloser, error) {
	return nil, nil //nolint:nilnil // test stub — never called in bootstrap tests
}

func (s *stubBridgeCustody) Delete(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyReference,
) error {
	return nil
}
