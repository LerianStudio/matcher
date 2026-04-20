// Package matchererrors defines Matcher-owned API error primitives.
package matchererrors

import "net/http"

// Details constrains structured error details to key-value payloads.
type Details = map[string]any

// Definition describes a stable product error identity and HTTP mapping.
type Definition struct {
	Code       string
	Title      string
	HTTPStatus int
}

// APIError exposes the transport-facing Matcher error contract.
type APIError interface {
	error
	ProductCode() string
	ProductTitle() string
	ProductMessage() string
	ProductDetails() Details
	HTTPStatusCode() int
}

// BaseError stores the common data shared by all Matcher API errors.
type BaseError struct {
	Code    string
	Title   string
	Message string
	Details Details
	Status  int
	cause   error
}

func (errorValue BaseError) Error() string {
	if errorValue.Message != "" {
		return errorValue.Message
	}

	return errorValue.Title
}

func (errorValue BaseError) Unwrap() error {
	return errorValue.cause
}

// ProductCode returns the stable product error code.
func (errorValue BaseError) ProductCode() string {
	return errorValue.Code
}

// ProductTitle returns the human-readable error title.
func (errorValue BaseError) ProductTitle() string {
	return errorValue.Title
}

// ProductMessage returns the client-facing error message.
func (errorValue BaseError) ProductMessage() string {
	return errorValue.Message
}

// ProductDetails returns optional structured error details.
func (errorValue BaseError) ProductDetails() Details {
	return errorValue.Details
}

// HTTPStatusCode returns the HTTP status associated with the error.
func (errorValue BaseError) HTTPStatusCode() int {
	return errorValue.Status
}

type (
	// ValidationError represents a malformed or invalid client request.
	ValidationError struct{ BaseError }
	// UnauthorizedError represents an authentication failure.
	UnauthorizedError struct{ BaseError }
	// ForbiddenError represents an authorization failure.
	ForbiddenError struct{ BaseError }
	// NotFoundError represents a missing resource.
	NotFoundError struct{ BaseError }
	// ConflictError represents a state or concurrency conflict.
	ConflictError struct{ BaseError }
	// UnprocessableEntityError represents a semantically invalid request.
	UnprocessableEntityError struct{ BaseError }
	// RateLimitError represents a throttling condition.
	RateLimitError struct{ BaseError }
	// ClientError represents a client-visible 4xx condition without a more specific typed category.
	ClientError struct{ BaseError }
	// ServiceUnavailableError represents a temporary dependency failure.
	ServiceUnavailableError struct{ BaseError }
	// InternalError represents an unexpected server failure.
	InternalError struct{ BaseError }
)

func newBaseError(definition Definition, message string, details Details, cause error) BaseError {
	return BaseError{
		Code:    definition.Code,
		Title:   definition.Title,
		Message: message,
		Details: details,
		Status:  definition.HTTPStatus,
		cause:   cause,
	}
}

// NewValidationError builds a validation error value.
func NewValidationError(definition Definition, message string, details Details, cause error) *ValidationError {
	return &ValidationError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewUnauthorizedError builds an unauthorized error value.
func NewUnauthorizedError(definition Definition, message string, details Details, cause error) *UnauthorizedError {
	return &UnauthorizedError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewForbiddenError builds a forbidden error value.
func NewForbiddenError(definition Definition, message string, details Details, cause error) *ForbiddenError {
	return &ForbiddenError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewNotFoundError builds a not-found error value.
func NewNotFoundError(definition Definition, message string, details Details, cause error) *NotFoundError {
	return &NotFoundError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewConflictError builds a conflict error value.
func NewConflictError(definition Definition, message string, details Details, cause error) *ConflictError {
	return &ConflictError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewUnprocessableEntityError builds an unprocessable-entity error value.
func NewUnprocessableEntityError(definition Definition, message string, details Details, cause error) *UnprocessableEntityError {
	return &UnprocessableEntityError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewRateLimitError builds a rate-limit error value.
func NewRateLimitError(definition Definition, message string, details Details, cause error) *RateLimitError {
	return &RateLimitError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewClientError builds a generic client-error value for unmapped 4xx statuses.
func NewClientError(definition Definition, message string, details Details, cause error) *ClientError {
	return &ClientError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewServiceUnavailableError builds a service-unavailable error value.
func NewServiceUnavailableError(definition Definition, message string, details Details, cause error) *ServiceUnavailableError {
	return &ServiceUnavailableError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewInternalError builds an internal-server-error value.
func NewInternalError(definition Definition, message string, details Details, cause error) *InternalError {
	return &InternalError{BaseError: newBaseError(definition, message, details, cause)}
}

// NewError builds the typed Matcher API error that matches the definition status.
func NewError(definition Definition, message string, details Details, cause error) APIError {
	switch definition.HTTPStatus {
	case http.StatusBadRequest:
		return NewValidationError(definition, message, details, cause)
	case http.StatusUnauthorized:
		return NewUnauthorizedError(definition, message, details, cause)
	case http.StatusForbidden:
		return NewForbiddenError(definition, message, details, cause)
	case http.StatusNotFound:
		return NewNotFoundError(definition, message, details, cause)
	case http.StatusConflict:
		return NewConflictError(definition, message, details, cause)
	case http.StatusUnprocessableEntity:
		return NewUnprocessableEntityError(definition, message, details, cause)
	case http.StatusTooManyRequests:
		return NewRateLimitError(definition, message, details, cause)
	case http.StatusServiceUnavailable:
		return NewServiceUnavailableError(definition, message, details, cause)
	default:
		if definition.HTTPStatus >= http.StatusBadRequest && definition.HTTPStatus < http.StatusInternalServerError {
			return NewClientError(definition, message, details, cause)
		}

		return NewInternalError(definition, message, details, cause)
	}
}
