// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGovernanceSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrAuditLogNotFound", ErrAuditLogNotFound, "audit log not found"},
		{"ErrActorMappingNotFound", ErrActorMappingNotFound, "actor mapping not found"},
		{"ErrMetadataNotFound", ErrMetadataNotFound, "archive metadata not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestGovernanceErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrAuditLogNotFound, ErrActorMappingNotFound)
	require.NotErrorIs(t, ErrActorMappingNotFound, ErrMetadataNotFound)
}
