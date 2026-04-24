// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package match_item

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "match item repository not initialized"},
		{"ErrMatchItemEntityNeeded", ErrMatchItemEntityNeeded, "match item entity is required"},
		{"ErrMatchItemModelNeeded", ErrMatchItemModelNeeded, "match item model is required"},
		{"ErrInvalidTx", ErrInvalidTx, "match item repository invalid transaction"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			wrapped := fmt.Errorf("context: %w", tt.err)
			assert.ErrorIs(t, wrapped, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrRepoNotInitialized, ErrMatchItemEntityNeeded)
	require.NotErrorIs(t, ErrMatchItemEntityNeeded, ErrMatchItemModelNeeded)
	require.NotErrorIs(t, ErrMatchItemModelNeeded, ErrInvalidTx)
}
