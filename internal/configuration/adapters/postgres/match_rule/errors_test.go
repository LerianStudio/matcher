// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package match_rule

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
)

func TestMatchRuleSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrMatchRuleEntityRequired",
			err:     ErrMatchRuleEntityRequired,
			message: "match rule entity is required",
		},
		{
			name:    "ErrMatchRuleModelRequired",
			err:     ErrMatchRuleModelRequired,
			message: "match rule model is required",
		},
		{
			name:    "ErrMatchRuleContextIDRequired",
			err:     ErrMatchRuleContextIDRequired,
			message: "match rule context ID is required",
		},
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "match rule repository not initialized",
		},
		{
			name:    "ErrRuleIDsRequired",
			err:     ErrRuleIDsRequired,
			message: "rule ids are required",
		},
		{
			name:    "ErrCursorNotFound",
			err:     ErrCursorNotFound,
			message: "cursor not found",
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
