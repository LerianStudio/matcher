// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package match_run

import (
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
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "match run repository not initialized"},
		{"ErrMatchRunEntityNeeded", ErrMatchRunEntityNeeded, "match run entity is required"},
		{"ErrMatchRunModelNeeded", ErrMatchRunModelNeeded, "match run model is required"},
		{"ErrInvalidTx", ErrInvalidTx, "match run repository invalid transaction"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrRepoNotInitialized, ErrMatchRunEntityNeeded)
	require.NotErrorIs(t, ErrMatchRunEntityNeeded, ErrMatchRunModelNeeded)
	require.NotErrorIs(t, ErrMatchRunModelNeeded, ErrInvalidTx)
}
