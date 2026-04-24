// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package matchererrors

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewError_PopulatesAPIErrorFromDefinition(t *testing.T) {
	t.Parallel()

	definition := Definition{
		Code:       "MTCH-9999",
		Title:      http.StatusText(http.StatusBadRequest),
		HTTPStatus: http.StatusBadRequest,
	}

	apiError := NewError(definition, "status specific message", map[string]any{"field": "name"}, nil)

	require.Equal(t, definition.Code, apiError.ProductCode())
	require.Equal(t, definition.Title, apiError.ProductTitle())
	require.Equal(t, definition.HTTPStatus, apiError.HTTPStatusCode())
	require.Equal(t, "status specific message", apiError.ProductMessage())
	require.Equal(t, map[string]any{"field": "name"}, apiError.ProductDetails())
}

func TestBaseError_Unwrap(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	apiError := NewError(Definition{
		Code:       "MTCH-9998",
		Title:      "Internal Server Error",
		HTTPStatus: http.StatusInternalServerError,
	}, "failed", nil, cause)

	require.ErrorIs(t, apiError, cause)
}

func TestBaseError_ErrorFallsBackToTitle(t *testing.T) {
	t.Parallel()

	apiError := NewError(Definition{
		Code:       "MTCH-9997",
		Title:      http.StatusText(http.StatusInternalServerError),
		HTTPStatus: http.StatusInternalServerError,
	}, "", nil, nil)

	require.Equal(t, http.StatusText(http.StatusInternalServerError), apiError.Error())
}
