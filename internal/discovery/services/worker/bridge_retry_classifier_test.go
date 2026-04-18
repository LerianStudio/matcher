// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestClassifyBridgeError_Terminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want BridgeRetryClassification
	}{
		{
			name: "integrity failed",
			err:  sharedPorts.ErrIntegrityVerificationFailed,
			want: BridgeRetryClassification{Policy: RetryTerminal, Class: vo.BridgeErrorClassIntegrityFailed},
		},
		{
			name: "wrapped integrity failed",
			err:  fmt.Errorf("wrapped: %w", sharedPorts.ErrIntegrityVerificationFailed),
			want: BridgeRetryClassification{Policy: RetryTerminal, Class: vo.BridgeErrorClassIntegrityFailed},
		},
		{
			name: "artifact not found (404)",
			err:  sharedPorts.ErrFetcherResourceNotFound,
			want: BridgeRetryClassification{Policy: RetryTerminal, Class: vo.BridgeErrorClassArtifactNotFound},
		},
		{
			name: "wrapped artifact not found",
			err:  fmt.Errorf("retrieve: %w", sharedPorts.ErrFetcherResourceNotFound),
			want: BridgeRetryClassification{Policy: RetryTerminal, Class: vo.BridgeErrorClassArtifactNotFound},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, ClassifyBridgeError(tt.err))
		})
	}
}

func TestClassifyBridgeError_Transient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"artifact retrieval failed", sharedPorts.ErrArtifactRetrievalFailed},
		{"custody store failed", sharedPorts.ErrCustodyStoreFailed},
		{"fetcher unavailable", sharedPorts.ErrFetcherUnavailable},
		{"source unresolvable", sharedPorts.ErrBridgeSourceUnresolvable},
		{"unknown wrapped", fmt.Errorf("oops: %w", errors.New("random failure"))},
		{"plain unknown", errors.New("anything else")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cls := ClassifyBridgeError(tt.err)
			assert.Equal(t, RetryTransient, cls.Policy)
			assert.Empty(t, cls.Class, "transient classification persists no class")
		})
	}
}

func TestClassifyBridgeError_Idempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"already linked", sharedPorts.ErrExtractionAlreadyLinked},
		{"ineligible", sharedPorts.ErrBridgeExtractionIneligible},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cls := ClassifyBridgeError(tt.err)
			assert.Equal(t, RetryIdempotent, cls.Policy)
		})
	}
}

func TestClassifyBridgeError_Totality(t *testing.T) {
	t.Parallel()

	// Every sentinel mentioned in the docstring must produce a
	// classification. Missing one is a documentation/code drift bug.
	sentinels := []error{
		sharedPorts.ErrIntegrityVerificationFailed,
		sharedPorts.ErrFetcherResourceNotFound,
		sharedPorts.ErrArtifactRetrievalFailed,
		sharedPorts.ErrCustodyStoreFailed,
		sharedPorts.ErrFetcherUnavailable,
		sharedPorts.ErrBridgeSourceUnresolvable,
		sharedPorts.ErrExtractionAlreadyLinked,
		sharedPorts.ErrBridgeExtractionIneligible,
	}

	for _, sentinel := range sentinels {
		t.Run(sentinel.Error(), func(t *testing.T) {
			t.Parallel()
			cls := ClassifyBridgeError(sentinel)
			// Each must produce a recognised policy.
			assert.Contains(t, []BridgeRetryPolicy{
				RetryTransient, RetryTerminal, RetryIdempotent,
			}, cls.Policy)
		})
	}
}

func TestEscalateAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want vo.BridgeErrorClass
	}{
		{"nil", nil, vo.BridgeErrorClassMaxAttemptsExceeded},
		{"source unresolvable", sharedPorts.ErrBridgeSourceUnresolvable, vo.BridgeErrorClassSourceUnresolved},
		{"wrapped source unresolvable",
			fmt.Errorf("ctx: %w", sharedPorts.ErrBridgeSourceUnresolvable),
			vo.BridgeErrorClassSourceUnresolved,
		},
		{"random transient", errors.New("network wat"), vo.BridgeErrorClassMaxAttemptsExceeded},
		{"custody failed", sharedPorts.ErrCustodyStoreFailed, vo.BridgeErrorClassMaxAttemptsExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, EscalateAfterMaxAttempts(tt.err))
		})
	}
}

func TestBridgeRetryPolicy_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy BridgeRetryPolicy
		want   string
	}{
		{RetryTransient, "transient"},
		{RetryTerminal, "terminal"},
		{RetryIdempotent, "idempotent"},
		{BridgeRetryPolicy(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.policy.String())
		})
	}
}
