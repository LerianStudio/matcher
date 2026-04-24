// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package testutil provides shared test helpers for internal packages.
package testutil

import (
	"testing"
	"time"
)

// WithFixedTime temporarily overrides the provided now func to return a fixed time.
func WithFixedTime(t *testing.T, fixed time.Time, nowFunc *func() time.Time, fn func()) {
	t.Helper()

	if nowFunc == nil {
		t.Fatal("now func pointer is nil")
		return
	}

	original := *nowFunc
	*nowFunc = func() time.Time { return fixed }

	t.Cleanup(func() {
		*nowFunc = original
	})

	fn()
}
