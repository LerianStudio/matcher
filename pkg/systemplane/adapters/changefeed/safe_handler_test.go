// Copyright 2025 Lerian Studio.

//go:build unit

package changefeed

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeInvokeHandler_NormalExecution(t *testing.T) {
	t.Parallel()

	var called bool

	err := SafeInvokeHandler(func(_ ports.ChangeSignal) { called = true }, ports.ChangeSignal{})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestSafeInvokeHandler_PanicConvertedToError(t *testing.T) {
	t.Parallel()

	err := SafeInvokeHandler(func(_ ports.ChangeSignal) { panic("boom") }, ports.ChangeSignal{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrHandlerPanic)
	// Panic detail is intentionally sanitized (MEDIUM-6 review fix).
	assert.NotContains(t, err.Error(), "boom", "panic detail should not leak into error")
}

func TestSafeInvokeHandler_ErrorPanicConvertedToError(t *testing.T) {
	t.Parallel()

	sentinelErr := assert.AnError

	err := SafeInvokeHandler(func(_ ports.ChangeSignal) { panic(sentinelErr) }, ports.ChangeSignal{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrHandlerPanic)
	// Panic detail is intentionally sanitized (MEDIUM-6 review fix).
	assert.NotContains(t, err.Error(), sentinelErr.Error(), "panic detail should not leak into error")
}
