package http

import (
	"errors"
	stdHTTP "net/http"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	matchererrors "github.com/LerianStudio/matcher/pkg"
)

type contextVerificationConfig struct {
	notFound             *legacyContextResponse
	hiddenOwnershipAs404 *legacyContextResponse
}

type legacyContextResponse struct {
	slug    string
	message string
}

// ContextVerificationOption customizes shared context-verification error mapping.
type ContextVerificationOption func(*contextVerificationConfig)

// WithContextNotFound overrides the default not-found response for ErrContextNotFound.
func WithContextNotFound(slug, message string) ContextVerificationOption {
	return func(config *contextVerificationConfig) {
		config.notFound = &legacyContextResponse{slug: slug, message: message}
	}
}

// WithHiddenContextOwnershipAsNotFound maps ownership-denied errors to a not-found response.
func WithHiddenContextOwnershipAsNotFound(slug, message string) ContextVerificationOption {
	return func(config *contextVerificationConfig) {
		config.hiddenOwnershipAs404 = &legacyContextResponse{slug: slug, message: message}
	}
}

// ValidateContextVerificationError classifies tenant-scoped context verification failures.
func ValidateContextVerificationError(err error, options ...ContextVerificationOption) matchererrors.APIError {
	config := contextVerificationConfig{}

	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	switch {
	case errors.Is(err, libHTTP.ErrMissingContextID), errors.Is(err, libHTTP.ErrInvalidContextID):
		return NewError(defInvalidContextID, "invalid context id", err)
	case errors.Is(err, libHTTP.ErrTenantIDNotFound), errors.Is(err, libHTTP.ErrInvalidTenantID):
		return NewError(defUnauthorized, "unauthorized", err)
	case errors.Is(err, libHTTP.ErrContextNotFound):
		if config.notFound != nil {
			return newLegacyContextVerificationError(config.notFound, stdHTTP.StatusNotFound, err)
		}

		return NewError(defNotFound, "context not found", err)
	case errors.Is(err, libHTTP.ErrContextNotActive):
		return NewError(defContextNotActive, "context is not active", err)
	case errors.Is(err, libHTTP.ErrContextNotOwned):
		if config.hiddenOwnershipAs404 != nil {
			return newLegacyContextVerificationError(config.hiddenOwnershipAs404, stdHTTP.StatusNotFound, err)
		}

		return NewError(defForbidden, "access denied", err)
	case errors.Is(err, libHTTP.ErrContextAccessDenied):
		return NewError(defForbidden, "access denied", err)
	default:
		return NewError(defInternalServerError, defaultInternalErrorMessage, err)
	}
}

// ValidateFallbackError classifies uncaught transport errors into shared MTCH fallbacks.
func ValidateFallbackError(err error) matchererrors.APIError {
	if isNilError(err) {
		return NewError(defInternalServerError, defaultInternalErrorMessage, nil)
	}

	if apiError, ok := asProductError(err); ok {
		return apiError
	}

	var fiberError *fiber.Error
	if errors.As(err, &fiberError) {
		return validateFiberError(fiberError, err)
	}

	return NewError(defInternalServerError, defaultInternalErrorMessage, err)
}

func validateFiberError(fiberError *fiber.Error, err error) matchererrors.APIError {
	switch fiberError.Code {
	case fiber.StatusBadRequest:
		return NewError(defInvalidRequest, "The server could not understand the request due to malformed syntax.", err)
	case fiber.StatusUnauthorized:
		return NewError(defUnauthorized, "Authentication is required to access this resource.", err)
	case fiber.StatusForbidden:
		return NewError(defForbidden, "You do not have permission to access this resource.", err)
	case fiber.StatusNotFound:
		return NewError(defNotFound, "The requested resource was not found.", err)
	case fiber.StatusConflict:
		return NewError(defConflict, "The request could not be completed due to a conflict with the current state of the resource.", err)
	case fiber.StatusRequestEntityTooLarge:
		return NewError(defRequestEntityTooLarge, "The request payload is too large.", err)
	case fiber.StatusUnprocessableEntity:
		return NewError(defUnprocessableEntity, "The server understands the request but cannot process it in its current form.", err)
	case fiber.StatusTooManyRequests:
		return NewError(defRateLimitExceeded, "Too many requests were sent in a given amount of time.", err)
	default:
		if fiberError.Code < fiber.StatusInternalServerError {
			return newGenericClientFiberError(fiberError.Code, err)
		}

		return NewError(defInternalServerError, defaultInternalErrorMessage, err)
	}
}

func newLegacyContextVerificationError(response *legacyContextResponse, status int, err error) matchererrors.APIError {
	if response == nil {
		return NewError(definitionForStatus(status), httpStatusText(status), err)
	}

	definition, ok := definitionFromLegacySlug(response.slug)
	if !ok {
		definition = definitionForStatus(status)
	}

	return NewError(definition, response.message, err)
}

func newGenericClientFiberError(statusCode int, err error) matchererrors.APIError {
	statusText := httpStatusText(statusCode)
	if statusText == "" {
		statusText = httpStatusText(fiber.StatusBadRequest)
	}

	return matchererrors.NewError(
		matchererrors.Definition{
			Code:       defRequestFailed.Code,
			Title:      statusText,
			HTTPStatus: statusCode,
		},
		statusText,
		nil,
		err,
	)
}

func httpStatusText(status int) string {
	return stdHTTP.StatusText(status)
}
