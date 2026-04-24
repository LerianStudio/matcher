// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package transaction

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"errTxEntityRequired", errTxEntityRequired, "transaction entity is required"},
		{"errTxModelRequired", errTxModelRequired, "transaction model is required"},
		{"errTxRequired", errTxRequired, "transaction is required"},
		{"errInvalidExtractionStatus", errInvalidExtractionStatus, "invalid extraction status"},
		{"errInvalidTxStatus", errInvalidTxStatus, "invalid transaction status"},
		{"errTxRepoNotInit", errTxRepoNotInit, "transaction repository not initialized"},
		{"errContextIDRequired", errContextIDRequired, "context id is required"},
		{"errJobIDRequired", errJobIDRequired, "job id is required"},
		{"errLimitMustBePositive", errLimitMustBePositive, "limit must be greater than zero"},
		{"errOffsetMustBeNonNegative", errOffsetMustBeNonNegative, "offset must be greater or equal to zero"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestErrTxRequired_IsCanonicalTransactionRequired(t *testing.T) {
	t.Parallel()

	require.True(t, errors.Is(errTxRequired, pgcommon.ErrTransactionRequired))
}

func TestTransactionErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, errTxEntityRequired, errTxModelRequired)
	require.NotErrorIs(t, errTxModelRequired, errTxRequired)
	require.NotErrorIs(t, errTxRequired, errInvalidExtractionStatus)
	require.NotErrorIs(t, errInvalidExtractionStatus, errInvalidTxStatus)
	require.NotErrorIs(t, errInvalidTxStatus, errTxRepoNotInit)
	require.NotErrorIs(t, errTxRepoNotInit, errContextIDRequired)
	require.NotErrorIs(t, errContextIDRequired, errJobIDRequired)
	require.NotErrorIs(t, errJobIDRequired, errLimitMustBePositive)
	require.NotErrorIs(t, errLimitMustBePositive, errOffsetMustBeNonNegative)
}
