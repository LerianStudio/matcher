//go:build unit

package http

import (
	"errors"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	matchererrors "github.com/LerianStudio/matcher/pkg"
)

func TestValidateContextVerificationError_AllBranches(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		err          error
		expectedCode string
		expectedHTTP int
	}{
		{name: "missing context id", err: libHTTP.ErrMissingContextID, expectedCode: defInvalidContextID.Code, expectedHTTP: http.StatusBadRequest},
		{name: "invalid context id", err: libHTTP.ErrInvalidContextID, expectedCode: defInvalidContextID.Code, expectedHTTP: http.StatusBadRequest},
		{name: "tenant not found", err: libHTTP.ErrTenantIDNotFound, expectedCode: defUnauthorized.Code, expectedHTTP: http.StatusUnauthorized},
		{name: "invalid tenant id", err: libHTTP.ErrInvalidTenantID, expectedCode: defUnauthorized.Code, expectedHTTP: http.StatusUnauthorized},
		{name: "context not found", err: libHTTP.ErrContextNotFound, expectedCode: defNotFound.Code, expectedHTTP: http.StatusNotFound},
		{name: "context not active", err: libHTTP.ErrContextNotActive, expectedCode: defContextNotActive.Code, expectedHTTP: http.StatusForbidden},
		{name: "context not owned", err: libHTTP.ErrContextNotOwned, expectedCode: defForbidden.Code, expectedHTTP: http.StatusForbidden},
		{name: "context access denied", err: libHTTP.ErrContextAccessDenied, expectedCode: defForbidden.Code, expectedHTTP: http.StatusForbidden},
		{name: "unknown error", err: errors.New("boom"), expectedCode: defInternalServerError.Code, expectedHTTP: http.StatusInternalServerError},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			apiError := ValidateContextVerificationError(testCase.err)
			require.Equal(t, testCase.expectedCode, apiError.ProductCode())
			require.Equal(t, testCase.expectedHTTP, apiError.HTTPStatusCode())
		})
	}
}

func TestValidateContextVerificationError_WithCustomNotFoundAndHiddenOwnership(t *testing.T) {
	t.Parallel()

	apiError := ValidateContextVerificationError(
		libHTTP.ErrContextNotFound,
		WithContextNotFound("configuration_context_not_found", "context not found"),
	)
	require.Equal(t, defConfigurationContextNotFound.Code, apiError.ProductCode())
	require.Equal(t, http.StatusNotFound, apiError.HTTPStatusCode())
	require.Equal(t, "context not found", apiError.ProductMessage())

	apiError = ValidateContextVerificationError(
		libHTTP.ErrContextNotOwned,
		WithHiddenContextOwnershipAsNotFound("configuration_context_not_found", "context not found"),
	)
	require.Equal(t, defConfigurationContextNotFound.Code, apiError.ProductCode())
	require.Equal(t, http.StatusNotFound, apiError.HTTPStatusCode())
	require.Equal(t, "context not found", apiError.ProductMessage())

	apiError = ValidateContextVerificationError(
		libHTTP.ErrContextAccessDenied,
		WithHiddenContextOwnershipAsNotFound("configuration_context_not_found", "context not found"),
	)
	require.Equal(t, defForbidden.Code, apiError.ProductCode())
	require.Equal(t, http.StatusForbidden, apiError.HTTPStatusCode())
	require.Equal(t, "access denied", apiError.ProductMessage())
}

func TestValidateFallbackError_PreservesMethodNotAllowedStatus(t *testing.T) {
	t.Parallel()

	apiError := ValidateFallbackError(fiber.NewError(http.StatusMethodNotAllowed, http.StatusText(http.StatusMethodNotAllowed)))
	require.Equal(t, defRequestFailed.Code, apiError.ProductCode())
	require.Equal(t, http.StatusMethodNotAllowed, apiError.HTTPStatusCode())
	require.Equal(t, http.StatusText(http.StatusMethodNotAllowed), apiError.ProductTitle())
	require.Equal(t, http.StatusText(http.StatusMethodNotAllowed), apiError.ProductMessage())
}

func TestValidateFallbackError_UsesUnprocessableDefinition(t *testing.T) {
	t.Parallel()

	apiError := ValidateFallbackError(fiber.NewError(http.StatusUnprocessableEntity, "validation failed"))
	require.Equal(t, defUnprocessableEntity.Code, apiError.ProductCode())
	require.Equal(t, http.StatusUnprocessableEntity, apiError.HTTPStatusCode())
}

func TestValidateFallbackError_UsesInternalErrorForGenericError(t *testing.T) {
	t.Parallel()

	apiError := ValidateFallbackError(errors.New("boom"))
	require.Equal(t, defInternalServerError.Code, apiError.ProductCode())
	require.Equal(t, http.StatusInternalServerError, apiError.HTTPStatusCode())
}

func TestValidateFallbackError_PreservesMatcherAPIError(t *testing.T) {
	t.Parallel()

	apiErr := NewError(defConflict, "already exists", nil)
	require.Same(t, apiErr, ValidateFallbackError(apiErr))
}

func TestValidateFallbackError_TypedNilMatcherErrorFallsBackToInternalError(t *testing.T) {
	t.Parallel()

	var typedNil *matchererrors.NotFoundError

	apiError := ValidateFallbackError(typedNil)
	require.Equal(t, defInternalServerError.Code, apiError.ProductCode())
	require.Equal(t, http.StatusInternalServerError, apiError.HTTPStatusCode())
}

func TestValidateFallbackError_NilFallsBackToInternalError(t *testing.T) {
	t.Parallel()

	apiError := ValidateFallbackError(nil)
	require.Equal(t, defInternalServerError.Code, apiError.ProductCode())
	require.Equal(t, http.StatusInternalServerError, apiError.HTTPStatusCode())
}

func TestValidateFallbackError_SanitizesUnknownClientErrorMessage(t *testing.T) {
	t.Parallel()

	apiError := ValidateFallbackError(fiber.NewError(http.StatusMethodNotAllowed, "router internals leaked"))
	require.Equal(t, defRequestFailed.Code, apiError.ProductCode())
	require.Equal(t, http.StatusMethodNotAllowed, apiError.HTTPStatusCode())
	require.Equal(t, http.StatusText(http.StatusMethodNotAllowed), apiError.ProductTitle())
	require.Equal(t, http.StatusText(http.StatusMethodNotAllowed), apiError.ProductMessage())
}

func TestValidateFallbackError_MapsKnownFiberStatuses(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		status          int
		expectedCode    string
		expectedMessage string
	}{
		{name: "bad request", status: http.StatusBadRequest, expectedCode: defInvalidRequest.Code, expectedMessage: "The server could not understand the request due to malformed syntax."},
		{name: "unauthorized", status: http.StatusUnauthorized, expectedCode: defUnauthorized.Code, expectedMessage: "Authentication is required to access this resource."},
		{name: "forbidden", status: http.StatusForbidden, expectedCode: defForbidden.Code, expectedMessage: "You do not have permission to access this resource."},
		{name: "not found", status: http.StatusNotFound, expectedCode: defNotFound.Code, expectedMessage: "The requested resource was not found."},
		{name: "conflict", status: http.StatusConflict, expectedCode: defConflict.Code, expectedMessage: "The request could not be completed due to a conflict with the current state of the resource."},
		{name: "request entity too large", status: http.StatusRequestEntityTooLarge, expectedCode: defRequestEntityTooLarge.Code, expectedMessage: "The request payload is too large."},
		{name: "rate limited", status: http.StatusTooManyRequests, expectedCode: defRateLimitExceeded.Code, expectedMessage: "Too many requests were sent in a given amount of time."},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			apiError := ValidateFallbackError(fiber.NewError(testCase.status, "ignored"))
			require.Equal(t, testCase.expectedCode, apiError.ProductCode())
			require.Equal(t, testCase.status, apiError.HTTPStatusCode())
			require.Equal(t, testCase.expectedMessage, apiError.ProductMessage())
		})
	}
}
