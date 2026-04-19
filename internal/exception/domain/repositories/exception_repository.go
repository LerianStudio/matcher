package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

// CursorFilter contains pagination and sorting parameters.
type CursorFilter struct {
	Limit     int
	Cursor    string
	SortBy    string
	SortOrder string
}

// ExceptionFilter defines optional filters for listing exceptions.
type ExceptionFilter struct {
	Status         *value_objects.ExceptionStatus
	Severity       *value_objects.ExceptionSeverity
	AssignedTo     *string
	ExternalSystem *string
	DateFrom       *time.Time
	DateTo         *time.Time
}

//go:generate mockgen -destination=mocks/exception_repository_mock.go -package=mocks . ExceptionRepository

// ExceptionRepository defines persistence operations for exceptions.
type ExceptionRepository interface {
	FindByID(ctx context.Context, id uuid.UUID) (*entities.Exception, error)
	// FindByIDs retrieves all exceptions whose ids are in the provided slice
	// in a single query. Used by bulk command paths to eliminate the
	// N round-trip pre-load that would otherwise precede N per-item
	// transactions. Ids not present in the store are silently omitted
	// from the result; callers are responsible for reconciling the
	// returned set against the requested set and reporting per-id
	// ErrExceptionNotFound where appropriate.
	FindByIDs(ctx context.Context, ids []uuid.UUID) ([]*entities.Exception, error)
	List(
		ctx context.Context,
		filter ExceptionFilter,
		cursor CursorFilter,
	) ([]*entities.Exception, libHTTP.CursorPagination, error)
	Update(ctx context.Context, exception *entities.Exception) (*entities.Exception, error)
	// UpdateWithTx updates an exception using the provided transaction.
	// This enables atomic updates across multiple repositories.
	UpdateWithTx(
		ctx context.Context,
		tx Tx,
		exception *entities.Exception,
	) (*entities.Exception, error)
}
