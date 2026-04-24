package repositories

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

//go:generate mockgen -destination=mocks/comment_repository_mock.go -package=mocks . CommentRepository

// CommentRepository defines persistence operations for exception comments.
type CommentRepository interface {
	Create(ctx context.Context, comment *entities.ExceptionComment) (*entities.ExceptionComment, error)
	FindByID(ctx context.Context, id uuid.UUID) (*entities.ExceptionComment, error)
	FindByExceptionID(ctx context.Context, exceptionID uuid.UUID) ([]*entities.ExceptionComment, error)
	// DeleteByExceptionAndID deletes a comment identified by both exceptionID
	// and commentID. Both IDs must match — a comment created on exception A
	// cannot be deleted by supplying its commentID under exception B's URL.
	// Returns ErrCommentNotFound when no row matches both IDs.
	DeleteByExceptionAndID(ctx context.Context, exceptionID, commentID uuid.UUID) error
	// DeleteByExceptionAndIDWithTx performs the same scoped deletion as
	// DeleteByExceptionAndID but participates in the provided transaction so
	// comment removal can be composed atomically with other writes.
	DeleteByExceptionAndIDWithTx(
		ctx context.Context,
		tx *sql.Tx,
		exceptionID, commentID uuid.UUID,
	) error
}
