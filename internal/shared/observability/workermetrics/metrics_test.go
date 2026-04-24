// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package workermetrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	workermetrics "github.com/LerianStudio/matcher/internal/shared/observability/workermetrics"
)

func TestOutcomeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "success", workermetrics.OutcomeSuccess)
	assert.Equal(t, "failure", workermetrics.OutcomeFailure)
	assert.Equal(t, "skipped", workermetrics.OutcomeSkipped)
}

func TestAttributeKeyConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "worker", workermetrics.AttrWorker)
	assert.Equal(t, "outcome", workermetrics.AttrOutcome)
	assert.Equal(t, "matcher.worker", workermetrics.MeterScope)
}

func TestNewRecorder_RetainsName(t *testing.T) {
	t.Parallel()

	rec := workermetrics.NewRecorder("archival_worker")
	assert.NotNil(t, rec)
	assert.Equal(t, "archival_worker", rec.Name())
}

func TestRecorder_NilReceiverIsSafe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var rec *workermetrics.Recorder

	assert.NotPanics(t, func() {
		rec.RecordCycle(ctx, time.Now(), workermetrics.OutcomeSuccess)
		rec.RecordItems(ctx, 10, 2)
		rec.RecordBacklog(ctx, 100)
		_ = rec.Name()
	})
}

// RecordCycle emits to the global OTel meter; with no reader attached,
// it is a no-op. The test verifies non-panic behaviour across the
// closed outcome set.
func TestRecorder_RecordCycle_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := workermetrics.NewRecorder("export_worker")
	started := time.Now().Add(-150 * time.Millisecond)

	assert.NotPanics(t, func() {
		rec.RecordCycle(ctx, started, workermetrics.OutcomeSuccess)
		rec.RecordCycle(ctx, started, workermetrics.OutcomeFailure)
		rec.RecordCycle(ctx, started, workermetrics.OutcomeSkipped)
	})
}

func TestRecorder_RecordItems_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := workermetrics.NewRecorder("cleanup_worker")

	assert.NotPanics(t, func() {
		rec.RecordItems(ctx, 0, 0)
		rec.RecordItems(ctx, 10, 0)
		rec.RecordItems(ctx, 0, 3)
		rec.RecordItems(ctx, 5, 2)
	})
}

func TestRecorder_RecordBacklog_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := workermetrics.NewRecorder("scheduler_worker")

	assert.NotPanics(t, func() {
		rec.RecordBacklog(ctx, 0)
		rec.RecordBacklog(ctx, 25)
		rec.RecordBacklog(ctx, 25) // no-op delta
		rec.RecordBacklog(ctx, 10) // shrinking backlog
		rec.RecordBacklog(ctx, 0)
	})
}

func TestRecorder_ParallelCyclesDoNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := workermetrics.NewRecorder("bridge_worker")

	assert.NotPanics(t, func() {
		done := make(chan struct{}, 4)

		for i := 0; i < 4; i++ {
			go func() {
				defer func() { done <- struct{}{} }()

				started := time.Now()

				rec.RecordCycle(ctx, started, workermetrics.OutcomeSuccess)
				rec.RecordItems(ctx, 7, 1)
				rec.RecordBacklog(ctx, int64(10+i))
			}()
		}

		for i := 0; i < 4; i++ {
			<-done
		}
	})
}
