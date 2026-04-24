// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestMatchRunMode_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     value_objects.MatchRunMode
		expected string
	}{
		{
			name:     "DryRun mode has correct value",
			mode:     value_objects.MatchRunModeDryRun,
			expected: "DRY_RUN",
		},
		{
			name:     "Commit mode has correct value",
			mode:     value_objects.MatchRunModeCommit,
			expected: "COMMIT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.mode))
		})
	}
}

func TestMatchRunMode_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     value_objects.MatchRunMode
		expected bool
	}{
		{
			name:     "DryRun is valid",
			mode:     value_objects.MatchRunModeDryRun,
			expected: true,
		},
		{
			name:     "Commit is valid",
			mode:     value_objects.MatchRunModeCommit,
			expected: true,
		},
		{
			name:     "Empty string is invalid",
			mode:     value_objects.MatchRunMode(""),
			expected: false,
		},
		{
			name:     "Random string is invalid",
			mode:     value_objects.MatchRunMode("RANDOM"),
			expected: false,
		},
		{
			name:     "Lowercase dry_run is invalid",
			mode:     value_objects.MatchRunMode("dry_run"),
			expected: false,
		},
		{
			name:     "Lowercase commit is invalid",
			mode:     value_objects.MatchRunMode("commit"),
			expected: false,
		},
		{
			name:     "Mixed case is invalid",
			mode:     value_objects.MatchRunMode("Dry_Run"),
			expected: false,
		},
		{
			name:     "Similar but wrong value DRY is invalid",
			mode:     value_objects.MatchRunMode("DRY"),
			expected: false,
		},
		{
			name:     "Whitespace is invalid",
			mode:     value_objects.MatchRunMode(" DRY_RUN"),
			expected: false,
		},
		{
			name:     "Unknown mode is invalid",
			mode:     value_objects.MatchRunMode("UNKNOWN"),
			expected: false,
		},
		{
			name:     "DRYRUN without underscore is invalid",
			mode:     value_objects.MatchRunMode("DRYRUN"),
			expected: false,
		},
		{
			name:     "TEST mode is invalid",
			mode:     value_objects.MatchRunMode("TEST"),
			expected: false,
		},
		{
			name:     "MANUAL mode is invalid",
			mode:     value_objects.MatchRunMode("MANUAL"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.mode.IsValid())
		})
	}
}

func TestMatchRunMode_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     value_objects.MatchRunMode
		expected string
	}{
		{
			name:     "DryRun returns DRY_RUN",
			mode:     value_objects.MatchRunModeDryRun,
			expected: "DRY_RUN",
		},
		{
			name:     "Commit returns COMMIT",
			mode:     value_objects.MatchRunModeCommit,
			expected: "COMMIT",
		},
		{
			name:     "Custom value returns itself",
			mode:     value_objects.MatchRunMode("CUSTOM"),
			expected: "CUSTOM",
		},
		{
			name:     "Empty string returns empty",
			mode:     value_objects.MatchRunMode(""),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}

func TestParseMatchRunMode_ValidModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected value_objects.MatchRunMode
	}{
		{
			name:     "Parse DRY_RUN",
			input:    "DRY_RUN",
			expected: value_objects.MatchRunModeDryRun,
		},
		{
			name:     "Parse COMMIT",
			input:    "COMMIT",
			expected: value_objects.MatchRunModeCommit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseMatchRunMode(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseMatchRunMode_InvalidModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Empty string",
			input: "",
		},
		{
			name:  "Lowercase dry_run",
			input: "dry_run",
		},
		{
			name:  "Lowercase commit",
			input: "commit",
		},
		{
			name:  "Mixed case Dry_Run",
			input: "Dry_Run",
		},
		{
			name:  "Random string",
			input: "RANDOM",
		},
		{
			name:  "Unknown mode",
			input: "UNKNOWN",
		},
		{
			name:  "Similar but wrong DRY",
			input: "DRY",
		},
		{
			name:  "With leading whitespace",
			input: " DRY_RUN",
		},
		{
			name:  "With trailing whitespace",
			input: "DRY_RUN ",
		},
		{
			name:  "With surrounding whitespace",
			input: " DRY_RUN ",
		},
		{
			name:  "Numeric string",
			input: "123",
		},
		{
			name:  "Special characters",
			input: "DRY_RUN!",
		},
		{
			name:  "DRYRUN without underscore",
			input: "DRYRUN",
		},
		{
			name:  "TEST mode",
			input: "TEST",
		},
		{
			name:  "MANUAL mode",
			input: "MANUAL",
		},
		{
			name:  "RUN mode",
			input: "RUN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseMatchRunMode(tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, value_objects.ErrInvalidMatchRunMode)
			assert.Equal(t, value_objects.MatchRunMode(""), result)
		})
	}
}

func TestMatchRunMode_RoundTrip(t *testing.T) {
	t.Parallel()

	modes := []value_objects.MatchRunMode{
		value_objects.MatchRunModeDryRun,
		value_objects.MatchRunModeCommit,
	}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()

			stringVal := mode.String()
			parsed, err := value_objects.ParseMatchRunMode(stringVal)
			require.NoError(t, err)
			assert.Equal(t, mode, parsed)
		})
	}
}
