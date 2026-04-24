// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"errJobEntityRequired", errJobEntityRequired, "ingestion job entity is required"},
		{"errJobModelRequired", errJobModelRequired, "ingestion job model is required"},
		{"errInvalidJobStatus", errInvalidJobStatus, "invalid job status"},
		{"errRepoNotInit", errRepoNotInit, "job repository not initialized"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestJobErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, errJobEntityRequired, errJobModelRequired)
	require.NotErrorIs(t, errJobModelRequired, errInvalidJobStatus)
	require.NotErrorIs(t, errInvalidJobStatus, errRepoNotInit)
}
