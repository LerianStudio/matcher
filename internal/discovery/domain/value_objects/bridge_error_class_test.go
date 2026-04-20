// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func TestBridgeErrorClass_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		class vo.BridgeErrorClass
		want  bool
	}{
		{"integrity_failed", vo.BridgeErrorClassIntegrityFailed, true},
		{"artifact_not_found", vo.BridgeErrorClassArtifactNotFound, true},
		{"source_unresolved", vo.BridgeErrorClassSourceUnresolved, true},
		{"max_attempts_exceeded", vo.BridgeErrorClassMaxAttemptsExceeded, true},
		{"unknown_class", vo.BridgeErrorClass("garbage"), false},
		{"empty", vo.BridgeErrorClass(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.class.IsValid())
		})
	}
}

func TestBridgeErrorClass_String_StableValues(t *testing.T) {
	t.Parallel()

	// These strings are persisted in the DB; renaming them is a breaking
	// migration. This test pins them so refactors don't accidentally drift.
	assert.Equal(t, "integrity_failed", vo.BridgeErrorClassIntegrityFailed.String())
	assert.Equal(t, "artifact_not_found", vo.BridgeErrorClassArtifactNotFound.String())
	assert.Equal(t, "source_unresolved", vo.BridgeErrorClassSourceUnresolved.String())
	assert.Equal(t, "max_attempts_exceeded", vo.BridgeErrorClassMaxAttemptsExceeded.String())
}

func TestParseBridgeErrorClass_ValidInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  vo.BridgeErrorClass
	}{
		{"integrity_failed", vo.BridgeErrorClassIntegrityFailed},
		{"INTEGRITY_FAILED", vo.BridgeErrorClassIntegrityFailed},
		{"  artifact_not_found  ", vo.BridgeErrorClassArtifactNotFound},
		{"source_unresolved", vo.BridgeErrorClassSourceUnresolved},
		{"max_attempts_exceeded", vo.BridgeErrorClassMaxAttemptsExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := vo.ParseBridgeErrorClass(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseBridgeErrorClass_InvalidInputs(t *testing.T) {
	t.Parallel()

	tests := []string{"", "   ", "unknown_class", "transient", "fail"}
	for _, in := range tests {
		t.Run(in, func(t *testing.T) {
			t.Parallel()

			_, err := vo.ParseBridgeErrorClass(in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, vo.ErrInvalidBridgeErrorClass))
		})
	}
}
