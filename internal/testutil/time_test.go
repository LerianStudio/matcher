// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package testutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithFixedTime(t *testing.T) {
	t.Parallel()

	t.Run("overrides now func and returns fixed time", func(t *testing.T) {
		t.Parallel()

		originalTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		fixedTime := time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC)

		nowFunc := func() time.Time { return originalTime }

		var capturedTime time.Time

		WithFixedTime(t, fixedTime, &nowFunc, func() {
			capturedTime = nowFunc()
		})

		assert.Equal(t, fixedTime, capturedTime)
	})

	t.Run("registers cleanup to restore original function", func(t *testing.T) {
		t.Parallel()

		originalTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		fixedTime := time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC)

		nowFunc := func() time.Time { return originalTime }

		// Run inner test synchronously (no innerT.Parallel()) to ensure
		// WithFixedTime's cleanup completes before we assert nowFunc is restored.
		// Using innerT.Parallel() would cause a race: the parent could run
		// assert.Equal while cleanup is still modifying nowFunc.
		t.Run(
			"inner",
			func(innerT *testing.T) { //nolint:paralleltest // intentionally sequential for cleanup verification
				WithFixedTime(innerT, fixedTime, &nowFunc, func() {
					assert.Equal(t, fixedTime, nowFunc())
				})
			},
		)

		assert.Equal(t, originalTime, nowFunc())
	})

	t.Run("fn is called during execution", func(t *testing.T) {
		t.Parallel()

		fixedTime := time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC)
		nowFunc := time.Now

		fnCalled := false

		WithFixedTime(t, fixedTime, &nowFunc, func() {
			fnCalled = true
		})

		assert.True(t, fnCalled)
	})
}
