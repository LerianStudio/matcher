//go:build unit

package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMatcherRedactor_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	redactor := NewMatcherRedactor()

	require.NotNil(t, redactor)
}

func TestNewMatcherRedactor_SingletonAcrossCalls(t *testing.T) {
	t.Parallel()

	// The matcher uses one process-wide redactor. Returning a fresh one per
	// call would make any future hash-based rule emit different digests for
	// the same input across spans, breaking trace-level correlation.
	a := NewMatcherRedactor()
	b := NewMatcherRedactor()

	assert.Same(t, a, b)
}
