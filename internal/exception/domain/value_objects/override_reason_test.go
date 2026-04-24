// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOverrideReason_IsValid(t *testing.T) {
	t.Parallel()

	require.True(t, OverrideReasonPolicyException.IsValid())
	require.True(t, OverrideReasonOpsApproval.IsValid())
	require.True(t, OverrideReasonCustomerDispute.IsValid())
	require.True(t, OverrideReasonDataCorrection.IsValid())
	require.False(t, OverrideReason("BAD").IsValid())
	require.False(t, OverrideReason("").IsValid())
}

func TestParseOverrideReason(t *testing.T) {
	t.Parallel()

	reason, err := ParseOverrideReason(" policy_exception ")
	require.NoError(t, err)
	require.Equal(t, OverrideReasonPolicyException, reason)

	reason, err = ParseOverrideReason("OPS_APPROVAL")
	require.NoError(t, err)
	require.Equal(t, OverrideReasonOpsApproval, reason)

	_, err = ParseOverrideReason(" ")
	require.ErrorIs(t, err, ErrInvalidOverrideReason)

	_, err = ParseOverrideReason("")
	require.ErrorIs(t, err, ErrInvalidOverrideReason)

	_, err = ParseOverrideReason("INVALID")
	require.ErrorIs(t, err, ErrInvalidOverrideReason)
}
