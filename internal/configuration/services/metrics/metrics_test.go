// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package metrics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	configurationMetrics "github.com/LerianStudio/matcher/internal/configuration/services/metrics"
)

func TestOutcomeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "fired", configurationMetrics.OutcomeSchedulerFired)
	assert.Equal(t, "lock_contention", configurationMetrics.OutcomeSchedulerLockContention)
	assert.Equal(t, "error", configurationMetrics.OutcomeSchedulerError)
}

func TestAttributeKeyConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "outcome", configurationMetrics.AttrOutcome)
}

// RecordSchedulerFiring emits to the global OTel meter; with no reader
// attached, it is a no-op. The tests verify non-panic behaviour
// (including empty outcome) rather than exporting state, because the
// global meter provider is outside the scope of package-local
// assertions.
func TestRecordSchedulerFiring_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		configurationMetrics.RecordSchedulerFiring(ctx, configurationMetrics.OutcomeSchedulerFired)
		configurationMetrics.RecordSchedulerFiring(ctx, configurationMetrics.OutcomeSchedulerLockContention)
		configurationMetrics.RecordSchedulerFiring(ctx, configurationMetrics.OutcomeSchedulerError)
	})
}
