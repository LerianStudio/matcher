//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// stubBundle is a compile-time check that RuntimeBundle can be implemented.
type stubBundle struct {
	closed bool
}

func (b *stubBundle) Close(_ context.Context) error {
	b.closed = true
	return nil
}

// Compile-time interface satisfaction check.
var _ RuntimeBundle = (*stubBundle)(nil)

func TestRuntimeBundle_StubImplementation(t *testing.T) {
	t.Parallel()

	var bundle RuntimeBundle = &stubBundle{}

	err := bundle.Close(context.Background())

	assert.NoError(t, err)
	assert.True(t, bundle.(*stubBundle).closed)
}
