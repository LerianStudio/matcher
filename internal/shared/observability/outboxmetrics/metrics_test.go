// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package outboxmetrics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	outboxmetrics "github.com/LerianStudio/matcher/internal/shared/observability/outboxmetrics"
)

func TestOutcomeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "success", outboxmetrics.OutcomeSuccess)
	assert.Equal(t, "failure", outboxmetrics.OutcomeFailure)
	assert.Equal(t, "skipped", outboxmetrics.OutcomeSkipped)
}

func TestAttributeKeyConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "event_type", outboxmetrics.AttrEventType)
	assert.Equal(t, "outcome", outboxmetrics.AttrOutcome)
}

// RecordHandlerInvocation emits to the global OTel meter; with no
// reader attached, it is a no-op. The test verifies non-panic behaviour
// (including negative durations, which are silently dropped for the
// histogram but still recorded on the counter) rather than exporting
// state, because the global meter provider is outside the scope of
// package-local assertions.
func TestRecordHandlerInvocation_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		outboxmetrics.RecordHandlerInvocation(ctx, "matching.match_confirmed", outboxmetrics.OutcomeSuccess, 12.5)
		outboxmetrics.RecordHandlerInvocation(ctx, "ingestion.completed", outboxmetrics.OutcomeFailure, 0)
		outboxmetrics.RecordHandlerInvocation(ctx, "governance.audit_log_created", outboxmetrics.OutcomeSkipped, 200)
		// Negative durations are silently dropped on the histogram; the
		// counter still records the invocation.
		outboxmetrics.RecordHandlerInvocation(ctx, "ingestion.failed", outboxmetrics.OutcomeSuccess, -1)
	})
}
