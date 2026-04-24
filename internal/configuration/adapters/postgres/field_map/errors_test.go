// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package field_map

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
)

func TestFieldMapSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrFieldMapEntityRequired",
			err:     ErrFieldMapEntityRequired,
			message: "field map entity is required",
		},
		{
			name:    "ErrFieldMapEntityIDRequired",
			err:     ErrFieldMapEntityIDRequired,
			message: "field map entity ID is nil",
		},
		{
			name:    "ErrFieldMapModelRequired",
			err:     ErrFieldMapModelRequired,
			message: "field map model is required",
		},
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "field map repository not initialized",
		},
		{
			name:    "ErrTransactionRequired",
			err:     ErrTransactionRequired,
			message: "transaction is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.EqualError(t, tt.err, tt.message)
		})
	}
}

func TestErrTransactionRequired_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
