// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package metrics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	reportingMetrics "github.com/LerianStudio/matcher/internal/reporting/services/metrics"
)

func TestAttributeKeyConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "format", reportingMetrics.AttrFormat)
	assert.Equal(t, "status", reportingMetrics.AttrStatus)
}

func TestRecordExportJobTransition_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		reportingMetrics.RecordExportJobTransition(ctx, "CSV", "QUEUED")
		reportingMetrics.RecordExportJobTransition(ctx, "PDF", "RUNNING")
		reportingMetrics.RecordExportJobTransition(ctx, "JSON", "SUCCEEDED")
		reportingMetrics.RecordExportJobTransition(ctx, "XML", "FAILED")
		reportingMetrics.RecordExportJobTransition(ctx, "", "CANCELED")
	})
}

func TestRecordExportDuration_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		reportingMetrics.RecordExportDuration(ctx, "CSV", 1234.5)
		reportingMetrics.RecordExportDuration(ctx, "PDF", 0)
		// Negative duration is skipped — defensive against clock skew
		// between DB timestamp and local time.Now().
		reportingMetrics.RecordExportDuration(ctx, "JSON", -1.0)
	})
}
