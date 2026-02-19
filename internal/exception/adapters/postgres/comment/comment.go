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
		id          string
		exceptionID string
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

	parsedID, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("parse comment id: %w", err)
	}

	parsedExceptionID, err := uuid.Parse(exceptionID)
	if err != nil {
		return nil, fmt.Errorf("parse exception id: %w", err)
	}

	return &entities.ExceptionComment{
		ID:          parsedID,
		ExceptionID: parsedExceptionID,
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
