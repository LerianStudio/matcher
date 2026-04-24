// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package matchererrors defines Matcher-owned API error primitives.
package matchererrors

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

// NewError builds a Matcher API error from a definition, message, optional details, and wrapped cause.
func NewError(definition Definition, message string, details Details, cause error) APIError {
	return &BaseError{
		Code:    definition.Code,
		Title:   definition.Title,
		Message: message,
		Details: details,
		Status:  definition.HTTPStatus,
		cause:   cause,
	}
}
