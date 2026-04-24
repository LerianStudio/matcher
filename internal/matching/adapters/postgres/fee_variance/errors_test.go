// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fee_variance

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
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "fee variance repository not initialized"},
		{
			"ErrFeeVarianceEntityNeeded",
			ErrFeeVarianceEntityNeeded,
			"fee variance entity is required",
		},
		{"ErrFeeVarianceModelNeeded", ErrFeeVarianceModelNeeded, "fee variance model is required"},
		{"ErrInvalidTx", ErrInvalidTx, "fee variance repository invalid transaction"},
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

	require.NotErrorIs(t, ErrRepoNotInitialized, ErrFeeVarianceEntityNeeded)
	require.NotErrorIs(t, ErrFeeVarianceEntityNeeded, ErrFeeVarianceModelNeeded)
	require.NotErrorIs(t, ErrFeeVarianceModelNeeded, ErrInvalidTx)
}
