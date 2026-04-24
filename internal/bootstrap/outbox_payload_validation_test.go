// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for the helpers extracted from outbox_wiring.go. The validate*Payload
// functions are exercised through outbox_wiring_test.go; this file focuses on
// the two pure helpers directly, which are easier to assert in isolation.

var errTestSentinel = errors.New("test sentinel")

func TestRequireNonZeroUUID_Nil_ReturnsWrappedSentinel(t *testing.T) {
	t.Parallel()
	err := requireNonZeroUUID(uuid.Nil, errTestSentinel, "ctx")
	require.Error(t, err)
	assert.ErrorIs(t, err, errTestSentinel)
	assert.Contains(t, err.Error(), "ctx")
}

func TestRequireNonZeroUUID_NonNil_ReturnsNil(t *testing.T) {
	t.Parallel()
	assert.NoError(t, requireNonZeroUUID(uuid.New(), errTestSentinel, "ctx"))
}

func TestRequireNonEmptyString_Empty_ReturnsWrappedSentinel(t *testing.T) {
	t.Parallel()
	err := requireNonEmptyString("", errTestSentinel, "ctx")
	require.Error(t, err)
	assert.ErrorIs(t, err, errTestSentinel)
	assert.Contains(t, err.Error(), "ctx")
}

func TestRequireNonEmptyString_NonEmpty_ReturnsNil(t *testing.T) {
	t.Parallel()
	assert.NoError(t, requireNonEmptyString("value", errTestSentinel, "ctx"))
}
