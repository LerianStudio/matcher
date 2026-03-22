//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
)

// Compile-time interface satisfaction check.
var _ ports.BundleReconciler = (*PublisherReconciler)(nil)

// --- NewPublisherReconciler ---

func TestNewPublisherReconciler_WithLogger(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}
	reconciler := NewPublisherReconciler(logger)

	require.NotNil(t, reconciler)
	assert.Equal(t, logger, reconciler.logger)
}

func TestNewPublisherReconciler_NilLogger(t *testing.T) {
	t.Parallel()

	reconciler := NewPublisherReconciler(nil)

	require.NotNil(t, reconciler)
	assert.NotNil(t, reconciler.logger, "nil logger must be replaced with NopLogger")
}

// --- Name ---

func TestPublisherReconciler_Name(t *testing.T) {
	t.Parallel()

	reconciler := NewPublisherReconciler(&libLog.NopLogger{})

	name := reconciler.Name()

	assert.Equal(t, "publisher-reconciler", name)
}

// --- Phase ---

func TestPublisherReconciler_Phase(t *testing.T) {
	t.Parallel()

	reconciler := NewPublisherReconciler(&libLog.NopLogger{})

	phase := reconciler.Phase()

	assert.Equal(t, domain.PhaseValidation, phase)
}

// --- Reconcile ---

func TestPublisherReconciler_Reconcile_NilCandidate(t *testing.T) {
	t.Parallel()

	reconciler := NewPublisherReconciler(&libLog.NopLogger{})

	err := reconciler.Reconcile(context.Background(), nil, nil, domain.Snapshot{})

	assert.NoError(t, err)
}

func TestPublisherReconciler_Reconcile_NonMatcherBundle(t *testing.T) {
	t.Parallel()

	reconciler := NewPublisherReconciler(&libLog.NopLogger{})

	// Pass a non-MatcherBundle candidate: the type assertion should return false.
	err := reconciler.Reconcile(context.Background(), nil, &mockRuntimeBundle{}, domain.Snapshot{})

	assert.NoError(t, err)
}

func TestPublisherReconciler_Reconcile_NilRabbitMQConn(t *testing.T) {
	t.Parallel()

	reconciler := NewPublisherReconciler(&libLog.NopLogger{})

	// MatcherBundle with no RabbitMQ connection.
	candidate := &MatcherBundle{}

	err := reconciler.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	assert.NoError(t, err)
}

// --- mockRuntimeBundle ---

// mockRuntimeBundle implements domain.RuntimeBundle for testing the type assertion path.
type mockRuntimeBundle struct{}

func (m *mockRuntimeBundle) Close(_ context.Context) error { return nil }
