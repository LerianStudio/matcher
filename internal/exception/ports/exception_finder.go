package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

//go:generate mockgen -destination=mocks/exception_finder_mock.go -package=mocks . ExceptionFinder

// ExceptionFinder provides read-only access to exceptions.
type ExceptionFinder interface {
	FindByID(ctx context.Context, id uuid.UUID) (*entities.Exception, error)
}
