// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dashboard

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDashboardErrors_NotNil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrContextIDRequired", ErrContextIDRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.err)
			assert.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestDashboardErrors_Messages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrRepositoryNotInitialized has expected message",
			err:     ErrRepositoryNotInitialized,
			message: "dashboard repository not initialized",
		},
		{
			name:    "ErrContextIDRequired has expected message",
			err:     ErrContextIDRequired,
			message: "context_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestDashboardErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, ErrRepositoryNotInitialized, ErrContextIDRequired)
}

func TestDashboardErrors_CanBeWrapped(t *testing.T) {
	t.Parallel()

	testErr := errors.New("test error")

	wrapped := errors.Join(testErr, ErrRepositoryNotInitialized)
	assert.ErrorIs(t, wrapped, ErrRepositoryNotInitialized)

	wrapped = errors.Join(testErr, ErrContextIDRequired)
	assert.ErrorIs(t, wrapped, ErrContextIDRequired)
}
