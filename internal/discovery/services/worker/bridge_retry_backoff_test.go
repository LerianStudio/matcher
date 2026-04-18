// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBridgeRetryBackoff_Normalize_ZeroValuesGetSaneDefaults(t *testing.T) {
	t.Parallel()

	b := BridgeRetryBackoff{}.Normalize()
	assert.Equal(t, 5, b.MaxAttempts)
}

func TestBridgeRetryBackoff_Normalize_PreservesNonZero(t *testing.T) {
	t.Parallel()

	b := BridgeRetryBackoff{
		MaxAttempts: 3,
	}.Normalize()

	assert.Equal(t, 3, b.MaxAttempts)
}

func TestBridgeRetryBackoff_ShouldEscalate(t *testing.T) {
	t.Parallel()

	b := BridgeRetryBackoff{MaxAttempts: 5}
	assert.False(t, b.ShouldEscalate(1))
	assert.False(t, b.ShouldEscalate(4))
	assert.True(t, b.ShouldEscalate(5))
	assert.True(t, b.ShouldEscalate(6))
}
