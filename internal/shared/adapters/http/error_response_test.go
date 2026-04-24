// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"

	matchererrors "github.com/LerianStudio/matcher/pkg"
)

func TestRespondProductError_WritesMatcherErrorResponse(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		return RespondProductError(c, NewError(defInvalidRequest, "invalid payload", nil))
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	var body ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, defInvalidRequest.Code, body.Code)
	require.Equal(t, defInvalidRequest.Title, body.Title)
	require.Equal(t, "invalid payload", body.Message)
}

func TestRespondProductError_FallsBackForTypedNilMatcherError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		var typedNil *matchererrors.BaseError

		return RespondProductError(c, typedNil)
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	var body ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, defInternalServerError.Code, body.Code)
	require.Equal(t, defInternalServerError.Title, body.Title)
	require.Equal(t, "an unexpected error occurred", body.Message)
}

func TestRespondProductError_WritesDetails(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		return RespondProductError(c, matchererrors.NewError(defConflict, "conflict", map[string]any{"field": "name"}, nil))
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	var body ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, defConflict.Code, body.Code)
	require.Equal(t, map[string]any{"field": "name"}, body.Details)
}

func TestRespondProductError_RejectsNilFiberContext(t *testing.T) {
	t.Parallel()

	err := RespondProductError(nil, NewError(defInvalidRequest, "invalid payload", nil))
	require.EqualError(t, err, "fiber context is required")
}
