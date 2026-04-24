// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fee_rule

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
)

func TestFeeRuleSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "fee rule repository not initialized",
		},
		{
			name:    "ErrFeeRuleModelNeeded",
			err:     ErrFeeRuleModelNeeded,
			message: "fee rule model is required for entity conversion",
		},
		{
			name:    "ErrFeeRuleEntityNil",
			err:     ErrFeeRuleEntityNil,
			message: "fee rule entity is required",
		},
		{
			name:    "ErrFeeRuleEntityIDNil",
			err:     ErrFeeRuleEntityIDNil,
			message: "fee rule entity ID is required",
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

func TestFeeRuleSentinelErrors_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}

func TestFeeRuleSentinelErrors_Distinct(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		ErrRepoNotInitialized,
		ErrFeeRuleModelNeeded,
		ErrFeeRuleEntityNil,
		ErrFeeRuleEntityIDNil,
	}

	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j {
				assert.NotEqual(t, a.Error(), b.Error(),
					"sentinel errors %d and %d should have distinct messages", i, j)
			}
		}
	}
}
