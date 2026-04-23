package http

import (
	"errors"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

// Handler-level errors for the exception domain.
var (
	// ErrNilExceptionUseCase indicates the exception use case is nil.
	ErrNilExceptionUseCase = errors.New("exception use case is required")

	// ErrNilDisputeUseCase indicates the dispute use case is nil.
	ErrNilDisputeUseCase = errors.New("dispute use case is required")

	// ErrNilQueryUseCase indicates the query use case is nil.
	ErrNilQueryUseCase = errors.New("query use case is required")

	// ErrNilDispatchUseCase indicates the dispatch use case is nil.
	ErrNilDispatchUseCase = errors.New("dispatch use case is required")

	// ErrNilExceptionProvider indicates the exception provider is nil.
	ErrNilExceptionProvider = errors.New("exception provider is required")

	// ErrNilDisputeProvider indicates the dispute provider is nil.
	ErrNilDisputeProvider = errors.New("dispute provider is required")

	// ErrNilCommentUseCase indicates the comment use case is nil.
	ErrNilCommentUseCase = errors.New("comment use case is required")

	// ErrNilCommentRepository indicates the comment repository is nil.
	ErrNilCommentRepository = errors.New("comment repository is required")

	// ErrNilCallbackUseCase indicates the callback use case is nil.
	ErrNilCallbackUseCase = errors.New("callback use case is required")

	// ErrMissingExceptionID indicates the exception ID path parameter is missing.
	ErrMissingExceptionID = errors.New("exception ID is required")

	// ErrInvalidExceptionID indicates invalid exception ID parameter.
	ErrInvalidExceptionID = errors.New("invalid exception id")

	// ErrExceptionNotFound indicates the exception was not found.
	// Aliased to the domain sentinel so errors.Is matches across layers.
	ErrExceptionNotFound = entities.ErrExceptionNotFound

	// ErrExceptionAccessDenied indicates access to the exception was denied.
	ErrExceptionAccessDenied = errors.New("access to exception denied")

	// ErrMissingDisputeID indicates the dispute ID path parameter is missing.
	ErrMissingDisputeID = errors.New("dispute ID is required")

	// ErrInvalidDisputeID indicates invalid dispute ID parameter.
	ErrInvalidDisputeID = errors.New("invalid dispute id")

	// ErrDisputeNotFound indicates the dispute was not found.
	ErrDisputeNotFound = errors.New("dispute not found")

	// ErrDisputeAccessDenied indicates access to the dispute was denied.
	ErrDisputeAccessDenied = errors.New("access to dispute denied")

	// ErrMissingParameter indicates a required path parameter is missing.
	ErrMissingParameter = errors.New("missing required parameter")

	// ErrInvalidParameter indicates a path parameter has invalid format.
	ErrInvalidParameter = errors.New("invalid parameter format")

	// errForbidden indicates access denied.
	errForbidden = errors.New("forbidden")

	// ErrInvalidSortBy indicates an invalid sort_by parameter.
	ErrInvalidSortBy = errors.New(
		"invalid sort_by: must be one of id, created_at, updated_at, severity, status",
	)

	// ErrInvalidSortOrder indicates an invalid sort_order parameter.
	ErrInvalidSortOrder = errors.New("invalid sort_order: must be one of asc, desc")
)
