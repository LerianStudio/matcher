// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"
	"reflect"

	"github.com/gofiber/fiber/v2"

	matchererrors "github.com/LerianStudio/matcher/pkg"
)

// NewError builds a Matcher API error from a catalog definition.
func NewError(definition matchererrors.Definition, message string, cause error) matchererrors.APIError {
	if message == "" {
		message = definition.Title
	}

	return matchererrors.NewError(definition, message, nil, cause)
}

// RespondError writes a Matcher API error response using a legacy slug-to-definition lookup.
func RespondError(fiberCtx *fiber.Ctx, status int, slug, message string) error {
	definition, ok := definitionFromLegacySlug(slug)
	if !ok {
		definition = definitionForStatus(status)
	}

	return RespondProductError(fiberCtx, NewError(definition, message, nil))
}

// asProductError unwraps a generic error into a Matcher API error when available.
func asProductError(err error) (matchererrors.APIError, bool) {
	if isNilError(err) {
		return nil, false
	}

	var apiError matchererrors.APIError
	if errors.As(err, &apiError) && !isNilProductError(apiError) {
		return apiError, true
	}

	return nil, false
}

// isNilError deliberately uses reflection to catch typed-nil errors held behind interfaces.
func isNilError(err error) bool {
	if err == nil {
		return true
	}

	value := reflect.ValueOf(err)
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilProductError(apiError matchererrors.APIError) bool {
	return isNilError(apiError)
}
