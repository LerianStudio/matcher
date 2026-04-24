// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package resolution_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/adapters/resolution"
)

var errTestOther = errors.New("some other error")

func TestErrInvalidAdjustment(t *testing.T) {
	t.Parallel()

	t.Run("error is not nil", func(t *testing.T) {
		t.Parallel()

		require.Error(t, resolution.ErrInvalidAdjustment)
	})

	t.Run("error message is correct", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "invalid adjustment", resolution.ErrInvalidAdjustment.Error())
	})

	t.Run("error can be wrapped and unwrapped", func(t *testing.T) {
		t.Parallel()

		wrapped := fmt.Errorf("operation failed: %w", resolution.ErrInvalidAdjustment)
		require.Contains(t, wrapped.Error(), "invalid adjustment")
		require.ErrorIs(t, wrapped, resolution.ErrInvalidAdjustment)
	})

	t.Run("error is distinct from other errors", func(t *testing.T) {
		t.Parallel()

		require.NotErrorIs(t, errTestOther, resolution.ErrInvalidAdjustment)
	})
}
