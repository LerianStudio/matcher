// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package constants

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplicationName(t *testing.T) {
	t.Parallel()

	t.Run("equals_matcher", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, "matcher", ApplicationName)
	})

	t.Run("not_empty", func(t *testing.T) {
		t.Parallel()
		require.NotEmpty(t, ApplicationName)
	})

	t.Run("expected_value_unchanged", func(t *testing.T) {
		t.Parallel()
		require.Equal(
			t,
			"matcher",
			ApplicationName,
			"ApplicationName constant should not be changed",
		)
	})
}
