// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package metrics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	matchingMetrics "github.com/LerianStudio/matcher/internal/matching/services/metrics"
)

func TestOutcomeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "confirmed", matchingMetrics.OutcomeConfirmed)
	assert.Equal(t, "manual", matchingMetrics.OutcomeManual)
	assert.Equal(t, "dry_run", matchingMetrics.OutcomeDryRun)
	assert.Equal(t, "failed", matchingMetrics.OutcomeFailed)
}

func TestAttributeKeyConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "outcome", matchingMetrics.AttrOutcome)
	assert.Equal(t, "context_id", matchingMetrics.AttrContextID)
}

// RecordRun and RecordConfidence emit to the global OTel meter; with no
// reader attached, these are no-ops. The tests verify non-panic behaviour
// (including nil/empty inputs) rather than exporting state, because the
// global meter provider is outside the scope of package-local assertions.
func TestRecordRun_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		matchingMetrics.RecordRun(ctx, matchingMetrics.OutcomeConfirmed, "ctx-1", 125.5)
		matchingMetrics.RecordRun(ctx, matchingMetrics.OutcomeFailed, "", 0)
	})
}

func TestRecordConfidence_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		matchingMetrics.RecordConfidence(ctx, "ctx-1", 85)
		matchingMetrics.RecordConfidence(ctx, "", 0)
		matchingMetrics.RecordConfidence(ctx, "ctx-2", 100)
	})
}
