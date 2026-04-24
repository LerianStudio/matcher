// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package match_group

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
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
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "match group repository not initialized"},
		{"ErrMatchGroupEntityNeeded", ErrMatchGroupEntityNeeded, "match group entity is required"},
		{"ErrMatchGroupModelNeeded", ErrMatchGroupModelNeeded, "match group model is required"},
		{"ErrInvalidTx", ErrInvalidTx, "match group repository invalid transaction"},
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

	require.NotErrorIs(t, ErrRepoNotInitialized, ErrMatchGroupEntityNeeded)
	require.NotErrorIs(t, ErrMatchGroupEntityNeeded, ErrMatchGroupModelNeeded)
	require.NotErrorIs(t, ErrMatchGroupModelNeeded, ErrInvalidTx)
}

func TestErrTransactionRequired_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	require.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
