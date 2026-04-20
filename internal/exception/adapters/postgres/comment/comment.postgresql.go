package comment

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository persists exception comments in PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new comment repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new comment.
func (repo *Repository) Create(
	ctx context.Context,
	comment *entities.ExceptionComment,
) (*entities.ExceptionComment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if comment == nil {
		return nil, ErrCommentNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.comment.create")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ExceptionComment, error) {
			return repo.executeCreate(ctx, tx, comment)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create comment: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create comment", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create comment: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new comment using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	comment *entities.ExceptionComment,
) (*entities.ExceptionComment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if comment == nil {
		return nil, ErrCommentNil
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.comment.create_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ExceptionComment, error) {
			return repo.executeCreate(ctx, innerTx, comment)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create comment with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create comment", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create comment: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual comment creation within a transaction.
func (repo *Repository) executeCreate(
	ctx context.Context,
	tx *sql.Tx,
	comment *entities.ExceptionComment,
) (*entities.ExceptionComment, error) {
	_, execErr := tx.ExecContext(ctx, `
				INSERT INTO exception_comments (
					id, exception_id, author, content, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6)
			`,
		comment.ID.String(),
		comment.ExceptionID.String(),
		comment.Author,
		comment.Content,
		comment.CreatedAt,
		comment.UpdatedAt,
	)
	if execErr != nil {
		return nil, fmt.Errorf("insert comment: %w", execErr)
	}

	row := tx.QueryRowContext(ctx, `
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE id = $1
			`, comment.ID.String())

	return scanComment(row)
}

// FindByID retrieves a single comment by its ID.
func (repo *Repository) FindByID(
	ctx context.Context,
	id uuid.UUID,
) (*entities.ExceptionComment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.comment.find_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.ExceptionComment, error) {
			row := qe.QueryRowContext(ctx, `
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE id = $1
			`, id.String())

			return scanComment(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("find comment by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find comment", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to find comment: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// FindByExceptionID retrieves all comments for an exception, ordered by created_at ASC.
func (repo *Repository) FindByExceptionID(
	ctx context.Context,
	exceptionID uuid.UUID,
) ([]*entities.ExceptionComment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.comment.find_by_exception_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.ExceptionComment, error) {
			rows, queryErr := qe.QueryContext(ctx, `
				SELECT id, exception_id, author, content, created_at, updated_at
				FROM exception_comments
				WHERE exception_id = $1
				ORDER BY created_at ASC
			`, exceptionID.String())
			if queryErr != nil {
				return nil, fmt.Errorf("query comments: %w", queryErr)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil {
					logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close rows: %v", closeErr))
				}
			}()

			comments := make([]*entities.ExceptionComment, 0)

			for rows.Next() {
				c, scanErr := scanCommentRows(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				comments = append(comments, c)
			}

			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, fmt.Errorf("iterate comment rows: %w", rowsErr)
			}

			return comments, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("find comments by exception id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find comments", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to find comments: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// Delete removes a comment by ID.
func (repo *Repository) Delete(ctx context.Context, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.comment.delete")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, tx, id)
		},
	)
	if err != nil {
		if errors.Is(err, ErrCommentNotFound) {
			return ErrCommentNotFound
		}

		wrappedErr := fmt.Errorf("delete comment: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete comment", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to delete comment: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// DeleteWithTx removes a comment by ID using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) DeleteWithTx(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.comment.delete_with_tx")

	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, id)
		},
	)
	if err != nil {
		if errors.Is(err, ErrCommentNotFound) {
			return ErrCommentNotFound
		}

		wrappedErr := fmt.Errorf("delete comment with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete comment", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to delete comment: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// executeDelete performs the actual comment deletion within a transaction.
func (repo *Repository) executeDelete(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
) (bool, error) {
	result, execErr := tx.ExecContext(ctx, `
				DELETE FROM exception_comments WHERE id = $1
			`, id.String())
	if execErr != nil {
		return false, fmt.Errorf("delete comment: %w", execErr)
	}

	rowsAffected, affectedErr := result.RowsAffected()
	if affectedErr != nil {
		return false, fmt.Errorf("get rows affected: %w", affectedErr)
	}

	if rowsAffected == 0 {
		return false, ErrCommentNotFound
	}

	return true, nil
}

// DeleteByExceptionAndID deletes a comment only when both the exception ID
// and the comment ID match. This prevents cross-exception deletion where a
// caller submits exception B's URL with exception A's comment ID.
func (repo *Repository) DeleteByExceptionAndID(
	ctx context.Context,
	exceptionID, commentID uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.comment.delete_by_exception_and_id")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (bool, error) {
			result, execErr := tx.ExecContext(ctx, `
				DELETE FROM exception_comments WHERE id = $1 AND exception_id = $2
			`, commentID.String(), exceptionID.String())
			if execErr != nil {
				return false, fmt.Errorf("delete comment scoped to exception: %w", execErr)
			}

			rowsAffected, affectedErr := result.RowsAffected()
			if affectedErr != nil {
				return false, fmt.Errorf("get rows affected: %w", affectedErr)
			}

			if rowsAffected == 0 {
				return false, ErrCommentNotFound
			}

			return true, nil
		},
	)
	if err != nil {
		if errors.Is(err, ErrCommentNotFound) {
			return ErrCommentNotFound
		}

		wrappedErr := fmt.Errorf("delete comment scoped to exception: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete comment", wrappedErr)
		logger.With(
			libLog.String("exception_id", exceptionID.String()),
			libLog.String("comment_id", commentID.String()),
		).Log(ctx, libLog.LevelError, "failed to delete comment scoped to exception")

		return wrappedErr
	}

	return nil
}

var _ repositories.CommentRepository = (*Repository)(nil)
