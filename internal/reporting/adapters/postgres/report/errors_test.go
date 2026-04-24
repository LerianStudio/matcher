// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTestOperationFailed = errors.New("test error")

func TestReportingRepositoryErrors_NotNil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrContextIDRequired", ErrContextIDRequired},
		{"ErrLimitMustBePositive", ErrLimitMustBePositive},
		{"ErrOffsetMustBeNonNegative", ErrOffsetMustBeNonNegative},
		{"ErrLimitExceedsMaximum", ErrLimitExceedsMaximum},
		{"ErrExportLimitExceeded", ErrExportLimitExceeded},
		{"ErrInvalidVarianceCursor", ErrInvalidVarianceCursor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.err)
			assert.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestReportingRepositoryErrors_Messages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrRepositoryNotInitialized has expected message",
			err:     ErrRepositoryNotInitialized,
			message: "report repository not initialized",
		},
		{
			name:    "ErrContextIDRequired has expected message",
			err:     ErrContextIDRequired,
			message: "context_id is required",
		},
		{
			name:    "ErrLimitMustBePositive has expected message",
			err:     ErrLimitMustBePositive,
			message: "limit must be positive",
		},
		{
			name:    "ErrOffsetMustBeNonNegative has expected message",
			err:     ErrOffsetMustBeNonNegative,
			message: "offset must be non-negative",
		},
		{
			name:    "ErrLimitExceedsMaximum has expected message",
			err:     ErrLimitExceedsMaximum,
			message: "limit exceeds maximum allowed (1000)",
		},
		{
			name:    "ErrExportLimitExceeded has expected message",
			err:     ErrExportLimitExceeded,
			message: "export record limit exceeded",
		},
		{
			name:    "ErrInvalidVarianceCursor has expected message",
			err:     ErrInvalidVarianceCursor,
			message: "invalid variance cursor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestReportingErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrRepositoryNotInitialized,
		ErrContextIDRequired,
		ErrLimitMustBePositive,
		ErrOffsetMustBeNonNegative,
		ErrLimitExceedsMaximum,
		ErrExportLimitExceeded,
		ErrInvalidVarianceCursor,
	}

	for i, err1 := range errs {
		for j, err2 := range errs {
			if i != j {
				assert.NotEqual(t, err1, err2, "errors at index %d and %d should be distinct", i, j)
			}
		}
	}
}

func TestReportingErrors_CanBeWrapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrContextIDRequired", ErrContextIDRequired},
		{"ErrExportLimitExceeded", ErrExportLimitExceeded},
		{"ErrInvalidVarianceCursor", ErrInvalidVarianceCursor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wrapped := errors.Join(errTestOperationFailed, tt.err)
			assert.ErrorIs(t, wrapped, tt.err)
		})
	}
}
