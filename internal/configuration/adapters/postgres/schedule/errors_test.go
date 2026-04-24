// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package schedule

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
)

func TestScheduleErrors_NotNil(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		ErrScheduleEntityRequired,
		ErrScheduleContextIDRequired,
		ErrScheduleModelRequired,
		ErrRepoNotInitialized,
	}

	for _, sentinel := range sentinels {
		assert.NotNil(t, sentinel, "sentinel error should not be nil")
		assert.NotEmpty(t, sentinel.Error(), "sentinel error message should not be empty")
	}
}

func TestScheduleErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		ErrScheduleEntityRequired,
		ErrScheduleContextIDRequired,
		ErrScheduleModelRequired,
		ErrRepoNotInitialized,
	}

	for i := 0; i < len(sentinels); i++ {
		for j := i + 1; j < len(sentinels); j++ {
			assert.False(t, errors.Is(sentinels[i], sentinels[j]),
				"error %q should not wrap %q", sentinels[i], sentinels[j])
			assert.False(t, errors.Is(sentinels[j], sentinels[i]),
				"error %q should not wrap %q", sentinels[j], sentinels[i])
		}
	}
}

func TestScheduleErrors_HaveDescriptiveMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		contains string
	}{
		{
			name:     "ErrScheduleEntityRequired",
			err:      ErrScheduleEntityRequired,
			contains: "schedule entity is required",
		},
		{
			name:     "ErrScheduleContextIDRequired",
			err:      ErrScheduleContextIDRequired,
			contains: "schedule context id is required",
		},
		{
			name:     "ErrScheduleModelRequired",
			err:      ErrScheduleModelRequired,
			contains: "schedule model is required",
		},
		{
			name:     "ErrRepoNotInitialized",
			err:      ErrRepoNotInitialized,
			contains: "schedule repository not initialized",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, tc.err.Error(), tc.contains,
				"error message should contain descriptive text")
		})
	}
}

func TestErrTransactionRequired_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
