// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveWorkerStartWaitTimeout(t *testing.T) {
	t.Parallel()

	t.Run("returns_default_without_deadline", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		timeout := resolveWorkerStartWaitTimeout(ctx)
		assert.Equal(t, defaultWorkerStartWaitTimeout, timeout)
	})

	t.Run("uses_remaining_time_when_shorter_than_default", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		timeout := resolveWorkerStartWaitTimeout(ctx)
		assert.LessOrEqual(t, timeout, 5*time.Second)
		assert.Greater(t, timeout, time.Duration(0))
	})

	t.Run("uses_default_when_deadline_is_further", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		timeout := resolveWorkerStartWaitTimeout(ctx)
		assert.Equal(t, defaultWorkerStartWaitTimeout, timeout)
	})

	t.Run("uses_default_for_expired_context", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		timeout := resolveWorkerStartWaitTimeout(ctx)
		// Expired deadline → remaining is negative → remaining > 0 is false
		// → timeout stays at default (30s) because the function only narrows,
		// never widens. The caller's context.Done() will fire immediately anyway.
		assert.Equal(t, defaultWorkerStartWaitTimeout, timeout)
	})
}

func TestCollectCriticalWorkerFailures(t *testing.T) {
	t.Parallel()

	t.Run("empty_results", func(t *testing.T) {
		t.Parallel()

		failures := collectCriticalWorkerFailures(nil)
		assert.Empty(t, failures)

		failures = collectCriticalWorkerFailures([]workerStartResult{})
		assert.Empty(t, failures)
	})

	t.Run("no_critical_failures", func(t *testing.T) {
		t.Parallel()

		results := []workerStartResult{
			{name: "export", critical: false, err: assert.AnError},
			{name: "scheduler", critical: true, err: nil}, // critical but no error
		}

		failures := collectCriticalWorkerFailures(results)
		assert.Empty(t, failures)
	})

	t.Run("collects_only_critical_failures", func(t *testing.T) {
		t.Parallel()

		results := []workerStartResult{
			{name: "export", critical: false, err: assert.AnError},
			{name: "outbox", critical: true, err: assert.AnError},
			{name: "scheduler", critical: true, err: nil},
			{name: "cleanup", critical: true, err: assert.AnError},
		}

		failures := collectCriticalWorkerFailures(results)
		require.Len(t, failures, 2)
		assert.Equal(t, "outbox", failures[0].name)
		assert.Equal(t, "cleanup", failures[1].name)
	})
}

func TestCollectStartedWorkers(t *testing.T) {
	t.Parallel()

	t.Run("empty_results", func(t *testing.T) {
		t.Parallel()

		started := collectStartedWorkers(nil)
		assert.Empty(t, started)
	})

	t.Run("collects_only_successful", func(t *testing.T) {
		t.Parallel()

		results := []workerStartResult{
			{name: "export", err: nil},
			{name: "outbox", err: assert.AnError},
			{name: "scheduler", err: nil},
		}

		started := collectStartedWorkers(results)
		assert.Len(t, started, 2)
		assert.Contains(t, started, "export")
		assert.Contains(t, started, "scheduler")
		assert.NotContains(t, started, "outbox")
	})
}

func TestAppendMissingWorkerResults(t *testing.T) {
	t.Parallel()

	t.Run("no_missing_entries", func(t *testing.T) {
		t.Parallel()

		entries := []workerStartEntry{
			{name: "a", critical: true},
		}
		collected := []workerStartResult{
			{name: "a", critical: true, err: nil},
		}

		result := appendMissingWorkerResults(entries, collected, errWorkerStartTimeout)
		assert.Len(t, result, 1)
	})

	t.Run("appends_missing_with_error", func(t *testing.T) {
		t.Parallel()

		entries := []workerStartEntry{
			{name: "a", critical: true},
			{name: "b", critical: false},
			{name: "c", critical: true},
		}
		collected := []workerStartResult{
			{name: "a", critical: true, err: nil},
		}

		result := appendMissingWorkerResults(entries, collected, errWorkerStartTimeout)
		require.Len(t, result, 3)

		// "b" and "c" should have the timeout error.
		assert.Equal(t, "b", result[1].name)
		assert.ErrorIs(t, result[1].err, errWorkerStartTimeout)
		assert.Equal(t, "c", result[2].name)
		assert.ErrorIs(t, result[2].err, errWorkerStartTimeout)
	})
}
