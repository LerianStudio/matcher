// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	matchererrors "github.com/LerianStudio/matcher/pkg"
)

var errFiberContextRequired = errors.New("fiber context is required")

// ErrorResponse is Matcher's API error response contract.
// @name MatcherErrorResponse
type ErrorResponse struct {
	Code    string         `json:"code"`
	Title   string         `json:"title"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func newErrorResponse(apiError matchererrors.APIError) ErrorResponse {
	return ErrorResponse{
		Code:    apiError.ProductCode(),
		Title:   apiError.ProductTitle(),
		Message: apiError.ProductMessage(),
		Details: apiError.ProductDetails(),
	}
}

// RespondProductError writes a Matcher API error response to Fiber.
func RespondProductError(fiberCtx *fiber.Ctx, apiError matchererrors.APIError) error {
	if fiberCtx == nil {
		return errFiberContextRequired
	}

	if isNilProductError(apiError) {
		apiError = NewError(defInternalServerError, defaultInternalErrorMessage, nil)
	}

	return fiberCtx.Status(apiError.HTTPStatusCode()).JSON(newErrorResponse(apiError))
}
