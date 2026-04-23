package comment

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

type scanner interface {
	Scan(dest ...any) error
}

func scanCommentInto(rowScanner scanner) (*entities.ExceptionComment, error) {
	var (
		id          uuid.UUID
		exceptionID uuid.UUID
		author      string
		content     string
		createdAt   time.Time
		updatedAt   time.Time
	)

	if err := rowScanner.Scan(
		&id, &exceptionID, &author, &content, &createdAt, &updatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan comment: %w", err)
	}

	return &entities.ExceptionComment{
		ID:          id,
		ExceptionID: exceptionID,
		Author:      author,
		Content:     content,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

func scanComment(row *sql.Row) (*entities.ExceptionComment, error) {
	return scanCommentInto(row)
}

func scanCommentRows(rows *sql.Rows) (*entities.ExceptionComment, error) {
	return scanCommentInto(rows)
}
