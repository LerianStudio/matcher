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
