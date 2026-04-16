// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestEnsureBridgeOperational_AllWired_Succeeds asserts that a fully-
// wired bundle passes the operational gate.
func TestEnsureBridgeOperational_AllWired_Succeeds(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{
		Intake:                       &stubBridgeIntake{},
		LinkWrite:                    &stubBridgeLinkWriter{},
		VerifiedArtifactOrchestrator: &discoveryCommand.VerifiedArtifactRetrievalOrchestrator{},
	}

	err := EnsureBridgeOperational(bundle)
	require.NoError(t, err)
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
	_ uuid.UUID,
	_ uuid.UUID,
) error {
	return nil
}
