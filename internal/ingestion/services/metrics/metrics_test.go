// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package metrics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	ingestionMetrics "github.com/LerianStudio/matcher/internal/ingestion/services/metrics"
)

func TestAttributeKeyConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "format", ingestionMetrics.AttrFormat)
	assert.Equal(t, "error_type", ingestionMetrics.AttrErrorType)
}

func TestErrorTypeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "parse", ingestionMetrics.ErrorTypeParse)
	assert.Equal(t, "validate", ingestionMetrics.ErrorTypeValidate)
	assert.Equal(t, "pipeline", ingestionMetrics.ErrorTypePipeline)
}

func TestRecordRowsProcessed_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		ingestionMetrics.RecordRowsProcessed(ctx, "csv", 100)
		ingestionMetrics.RecordRowsProcessed(ctx, "json", 0)   // skipped
		ingestionMetrics.RecordRowsProcessed(ctx, "xml", -1)   // skipped
		ingestionMetrics.RecordRowsProcessed(ctx, "", 50)
	})
}

func TestRecordDedupRate_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		// 100 parsed, 80 inserted ⇒ 20% dedup hit rate.
		ingestionMetrics.RecordDedupRate(ctx, "csv", 100, 80)
		// Zero parsed: skipped.
		ingestionMetrics.RecordDedupRate(ctx, "csv", 0, 0)
		// Inserted > parsed (impossible but must clamp): rate recorded as 0.
		ingestionMetrics.RecordDedupRate(ctx, "json", 10, 20)
	})
}

func TestRecordParsingError_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.NotPanics(t, func() {
		ingestionMetrics.RecordParsingError(ctx, "csv", ingestionMetrics.ErrorTypeParse, 1)
		ingestionMetrics.RecordParsingError(ctx, "json", ingestionMetrics.ErrorTypeValidate, 5)
		// Zero-count is skipped.
		ingestionMetrics.RecordParsingError(ctx, "xml", ingestionMetrics.ErrorTypePipeline, 0)
	})
}
