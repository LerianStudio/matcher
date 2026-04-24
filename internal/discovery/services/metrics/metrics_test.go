// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package metrics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	discoveryMetrics "github.com/LerianStudio/matcher/internal/discovery/services/metrics"
)

func TestOutcomeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "success", discoveryMetrics.OutcomeSuccess)
	assert.Equal(t, "failure", discoveryMetrics.OutcomeFailure)
	assert.Equal(t, "skipped", discoveryMetrics.OutcomeSkipped)
}

func TestAttributeKeyConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "outcome", discoveryMetrics.AttrOutcome)
	assert.Equal(t, "state", discoveryMetrics.AttrState)
}

// RecordFetcherCycle, RecordExtractionState, RecordSchemaCacheHit, and
// RecordSchemaCacheMiss emit to the global OTel meter; with no reader
// attached, these are no-ops. The tests verify non-panic behaviour
// (including nil/empty inputs) rather than exporting state, because the
// global meter provider is outside the scope of package-local assertions.
func TestRecordFetcherCycle_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		discoveryMetrics.RecordFetcherCycle(ctx, discoveryMetrics.OutcomeSuccess, 125.5)
		discoveryMetrics.RecordFetcherCycle(ctx, discoveryMetrics.OutcomeSkipped, 0)
		discoveryMetrics.RecordFetcherCycle(ctx, discoveryMetrics.OutcomeFailure, 2000)
	})
}

func TestRecordExtractionState_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		discoveryMetrics.RecordExtractionState(ctx, "PENDING")
		discoveryMetrics.RecordExtractionState(ctx, "SUBMITTED")
		discoveryMetrics.RecordExtractionState(ctx, "EXTRACTING")
		discoveryMetrics.RecordExtractionState(ctx, "COMPLETE")
		discoveryMetrics.RecordExtractionState(ctx, "FAILED")
		discoveryMetrics.RecordExtractionState(ctx, "CANCELLED")
		// Empty state is silently skipped by the helper.
		discoveryMetrics.RecordExtractionState(ctx, "")
	})
}

func TestRecordSchemaCache_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		discoveryMetrics.RecordSchemaCacheHit(ctx)
		discoveryMetrics.RecordSchemaCacheMiss(ctx)
	})
}
