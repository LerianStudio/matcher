// Package exception provides PostgreSQL persistence for exception entities.
package exception

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

const columns = "id, transaction_id, severity, status, external_system, external_issue_id, assigned_to, due_at, resolution_notes, resolution_type, resolution_reason, reason, version, created_at, updated_at"

// allowedSortColumns lists columns valid for sort operations.
var allowedSortColumns = []string{"id", "created_at", "updated_at", "severity", "status"}

type scanner interface {
	Scan(dest ...any) error
}

func scanInto(rowScanner scanner) (*entities.Exception, error) {
	var (
		id               uuid.UUID
		transactionID    uuid.UUID
		severity         string
		status           string
		externalSystem   sql.NullString
		externalIssueID  sql.NullString
		assignedTo       sql.NullString
		dueAt            sql.NullTime
		resolutionNotes  sql.NullString
		resolutionType   sql.NullString
		resolutionReason sql.NullString
		reason           sql.NullString
		version          int64
		createdAt        time.Time
		updatedAt        time.Time
	)

	if err := rowScanner.Scan(
		&id, &transactionID, &severity, &status,
		&externalSystem, &externalIssueID, &assignedTo, &dueAt,
		&resolutionNotes, &resolutionType, &resolutionReason,
		&reason, &version, &createdAt, &updatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan exception row: %w", err)
	}

	parsedSeverity, err := sharedexception.ParseExceptionSeverity(severity)
	if err != nil {
		return nil, fmt.Errorf("parse severity: %w", err)
	}

	parsedStatus, err := value_objects.ParseExceptionStatus(status)
	if err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}

	return &entities.Exception{
		ID:               id,
		TransactionID:    transactionID,
		Severity:         parsedSeverity,
		Status:           parsedStatus,
		ExternalSystem:   pgcommon.NullStringToStringPtr(externalSystem),
		ExternalIssueID:  pgcommon.NullStringToStringPtr(externalIssueID),
		AssignedTo:       pgcommon.NullStringToStringPtr(assignedTo),
		DueAt:            pgcommon.NullTimeToTimePtr(dueAt),
		ResolutionNotes:  pgcommon.NullStringToStringPtr(resolutionNotes),
		ResolutionType:   pgcommon.NullStringToStringPtr(resolutionType),
		ResolutionReason: pgcommon.NullStringToStringPtr(resolutionReason),
		Reason:           pgcommon.NullStringToStringPtr(reason),
		Version:          version,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
	}, nil
}

func scanException(row *sql.Row) (*entities.Exception, error) {
	return scanInto(row)
}

func scanRows(rows *sql.Rows) (*entities.Exception, error) {
	return scanInto(rows)
}
