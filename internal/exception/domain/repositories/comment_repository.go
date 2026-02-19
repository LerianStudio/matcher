package repositories

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

//go:generate mockgen -destination=mocks/comment_repository_mock.go -package=mocks . CommentRepository

// CommentRepository defines persistence operations for exception comments.
type CommentRepository interface {
	Create(ctx context.Context, comment *entities.ExceptionComment) (*entities.ExceptionComment, error)
	FindByID(ctx context.Context, id uuid.UUID) (*entities.ExceptionComment, error)
	FindByExceptionID(ctx context.Context, exceptionID uuid.UUID) ([]*entities.ExceptionComment, error)
	Delete(ctx context.Context, id uuid.UUID) error
}
