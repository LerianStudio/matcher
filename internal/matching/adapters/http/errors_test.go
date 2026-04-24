// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrRunModeRequired(t *testing.T) {
	t.Parallel()

	t.Run("is_not_nil", func(t *testing.T) {
		t.Parallel()
		require.Error(t, ErrRunModeRequired)
	})

	t.Run("returns_correct_message", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, "mode is required", ErrRunModeRequired.Error())
	})

	t.Run("can_be_matched_with_errors_Is", func(t *testing.T) {
		t.Parallel()
	})

	t.Run("can_be_wrapped_and_unwrapped", func(t *testing.T) {
		t.Parallel()

		wrapped := fmt.Errorf("validation failed: %w", ErrRunModeRequired)

		require.ErrorIs(t, wrapped, ErrRunModeRequired)
		require.Equal(t, ErrRunModeRequired, errors.Unwrap(wrapped))
	})
}
