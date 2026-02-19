//go:build unit

package http

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorSentinels_AreDistinct(t *testing.T) {
	t.Parallel()

	allErrors := []error{
		ErrNilExceptionUseCase,
		ErrNilDisputeUseCase,
		ErrNilQueryUseCase,
		ErrNilDispatchUseCase,
		ErrNilExceptionProvider,
		ErrNilDisputeProvider,
		ErrNilCallbackUseCase,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionNotFound,
		ErrExceptionAccessDenied,
		ErrMissingDisputeID,
		ErrInvalidDisputeID,
		ErrDisputeNotFound,
		ErrDisputeAccessDenied,
		ErrMissingParameter,
		ErrInvalidParameter,
		errForbidden,
		ErrInvalidSortBy,
		ErrInvalidSortOrder,
	}

	for i, err1 := range allErrors {
		for j, err2 := range allErrors {
			if i != j {
				require.NotErrorIs(
					t,
					err1,
					err2,
					"errors should be distinct: %v and %v",
					err1,
					err2,
				)
			}
		}
	}
}

func TestErrorSentinels_HaveMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		contains string
	}{
		{
			name:     "ErrNilExceptionUseCase",
			err:      ErrNilExceptionUseCase,
			contains: "exception use case is required",
		},
		{
			name:     "ErrNilDisputeUseCase",
			err:      ErrNilDisputeUseCase,
			contains: "dispute use case is required",
		},
		{
			name:     "ErrNilQueryUseCase",
			err:      ErrNilQueryUseCase,
			contains: "query use case is required",
		},
		{
			name:     "ErrNilDispatchUseCase",
			err:      ErrNilDispatchUseCase,
			contains: "dispatch use case is required",
		},
		{
			name:     "ErrNilCommentQueryUseCase",
			err:      ErrNilCommentQueryUseCase,
			contains: "comment query use case is required",
		},
		{
			name:     "ErrNilCallbackUseCase",
			err:      ErrNilCallbackUseCase,
			contains: "callback use case is required",
		},
		{
			name:     "ErrNilExceptionProvider",
			err:      ErrNilExceptionProvider,
			contains: "exception provider is required",
		},
		{
			name:     "ErrNilDisputeProvider",
			err:      ErrNilDisputeProvider,
			contains: "dispute provider is required",
		},
		{
			name:     "ErrMissingExceptionID",
			err:      ErrMissingExceptionID,
			contains: "exception ID is required",
		},
		{
			name:     "ErrInvalidExceptionID",
			err:      ErrInvalidExceptionID,
			contains: "invalid exception id",
		},
		{
			name:     "ErrExceptionNotFound",
			err:      ErrExceptionNotFound,
			contains: "exception not found",
		},
		{
			name:     "ErrExceptionAccessDenied",
			err:      ErrExceptionAccessDenied,
			contains: "access to exception denied",
		},
		{
			name:     "ErrMissingDisputeID",
			err:      ErrMissingDisputeID,
			contains: "dispute ID is required",
		},
		{
			name:     "ErrInvalidDisputeID",
			err:      ErrInvalidDisputeID,
			contains: "invalid dispute id",
		},
		{
			name:     "ErrDisputeNotFound",
			err:      ErrDisputeNotFound,
			contains: "dispute not found",
		},
		{
			name:     "ErrDisputeAccessDenied",
			err:      ErrDisputeAccessDenied,
			contains: "access to dispute denied",
		},
		{
			name:     "ErrMissingParameter",
			err:      ErrMissingParameter,
			contains: "missing required parameter",
		},
		{
			name:     "ErrInvalidParameter",
			err:      ErrInvalidParameter,
			contains: "invalid parameter format",
		},
		{
			name:     "errForbidden",
			err:      errForbidden,
			contains: "forbidden",
		},
		{
			name:     "ErrInvalidSortBy",
			err:      ErrInvalidSortBy,
			contains: "invalid sort_by",
		},
		{
			name:     "ErrInvalidSortOrder",
			err:      ErrInvalidSortOrder,
			contains: "invalid sort_order",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, tt.err.Error(), tt.contains)
		})
	}
}

func TestErrorSentinels_AreNotNil(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrNilExceptionUseCase)
	require.Error(t, ErrNilDisputeUseCase)
	require.Error(t, ErrNilQueryUseCase)
	require.Error(t, ErrNilDispatchUseCase)
	require.Error(t, ErrNilExceptionProvider)
	require.Error(t, ErrNilDisputeProvider)
	require.Error(t, ErrNilCallbackUseCase)
	require.Error(t, ErrMissingExceptionID)
	require.Error(t, ErrInvalidExceptionID)
	require.Error(t, ErrExceptionNotFound)
	require.Error(t, ErrExceptionAccessDenied)
	require.Error(t, ErrMissingDisputeID)
	require.Error(t, ErrInvalidDisputeID)
	require.Error(t, ErrDisputeNotFound)
	require.Error(t, ErrDisputeAccessDenied)
	require.Error(t, ErrMissingParameter)
	require.Error(t, ErrInvalidParameter)
	require.Error(t, errForbidden)
	require.Error(t, ErrInvalidSortBy)
	require.Error(t, ErrInvalidSortOrder)
}

func TestErrorSentinels_ErrorMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrNilExceptionUseCase returns exact message",
			err:      ErrNilExceptionUseCase,
			expected: "exception use case is required",
		},
		{
			name:     "ErrNilDisputeUseCase returns exact message",
			err:      ErrNilDisputeUseCase,
			expected: "dispute use case is required",
		},
		{
			name:     "ErrNilQueryUseCase returns exact message",
			err:      ErrNilQueryUseCase,
			expected: "query use case is required",
		},
		{
			name:     "ErrNilDispatchUseCase returns exact message",
			err:      ErrNilDispatchUseCase,
			expected: "dispatch use case is required",
		},
		{
			name:     "ErrNilCallbackUseCase returns exact message",
			err:      ErrNilCallbackUseCase,
			expected: "callback use case is required",
		},
		{
			name:     "ErrNilExceptionProvider returns exact message",
			err:      ErrNilExceptionProvider,
			expected: "exception provider is required",
		},
		{
			name:     "ErrNilDisputeProvider returns exact message",
			err:      ErrNilDisputeProvider,
			expected: "dispute provider is required",
		},
		{
			name:     "ErrInvalidSortBy returns exact message",
			err:      ErrInvalidSortBy,
			expected: "invalid sort_by: must be one of id, created_at, updated_at, severity, status",
		},
		{
			name:     "ErrInvalidSortOrder returns exact message",
			err:      ErrInvalidSortOrder,
			expected: "invalid sort_order: must be one of asc, desc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}
