// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package extraction

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

func TestExtractionSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "extraction repository not initialized",
		},
		{
			name:    "ErrEntityRequired",
			err:     ErrEntityRequired,
			message: "extraction request entity is required",
		},
		{
			name:    "ErrModelRequired",
			err:     ErrModelRequired,
			message: "extraction request model is required",
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
