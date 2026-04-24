// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

func TestIsScheduleClientError_MatchesKnownErrors(t *testing.T) {
	t.Parallel()

	knownErrors := []error{
		entities.ErrScheduleContextIDRequired,
		entities.ErrScheduleCronExpressionRequired,
		entities.ErrScheduleCronExpressionInvalid,
	}

	for _, knownErr := range knownErrors {
		t.Run(knownErr.Error(), func(t *testing.T) {
			t.Parallel()

			assert.True(t, isScheduleClientError(knownErr),
				"expected isScheduleClientError to return true for %v", knownErr)
		})
	}
}

func TestIsScheduleClientError_MatchesWrappedErrors(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("validation failed: %w", entities.ErrScheduleCronExpressionInvalid)

	assert.True(t, isScheduleClientError(wrapped),
		"expected isScheduleClientError to return true for wrapped sentinel error")
}

func TestIsScheduleClientError_RejectsUnknownError(t *testing.T) {
	t.Parallel()

	unknownErrors := []error{
		errors.New("some random error"),
		errors.New("context id is required for schedule"),
		fmt.Errorf("unrelated: %w", errors.New("inner")),
	}

	for _, unknownErr := range unknownErrors {
		t.Run(unknownErr.Error(), func(t *testing.T) {
			t.Parallel()

			assert.False(t, isScheduleClientError(unknownErr),
				"expected isScheduleClientError to return false for %v", unknownErr)
		})
	}
}

func TestIsScheduleClientError_RejectsNil(t *testing.T) {
	t.Parallel()

	//nolint:staticcheck // intentionally passing nil to verify behavior
	assert.False(t, isScheduleClientError(nil),
		"expected isScheduleClientError to return false for nil error")
}
