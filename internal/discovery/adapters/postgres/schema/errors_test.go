// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package schema

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
)

func TestSchemaSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "schema repository not initialized",
		},
		{
			name:    "ErrEntityRequired",
			err:     ErrEntityRequired,
			message: "discovered schema entity is required",
		},
		{
			name:    "ErrModelRequired",
			err:     ErrModelRequired,
			message: "discovered schema model is required",
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
