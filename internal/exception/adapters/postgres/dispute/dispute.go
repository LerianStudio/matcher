// Package dispute provides PostgreSQL persistence for dispute entities.
package dispute

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

type scanner interface {
	Scan(dest ...any) error
}

func scanDisputeInto(rowScanner scanner) (*dispute.Dispute, error) {
	var (
		id           uuid.UUID
		exceptionID  uuid.UUID
		category     string
		state        string
		description  string
		openedBy     string
		resolution   sql.NullString
		reopenReason sql.NullString
		evidenceJSON []byte
		createdAt    time.Time
		updatedAt    time.Time
	)

	if err := rowScanner.Scan(
		&id, &exceptionID, &category, &state, &description,
		&openedBy, &resolution, &reopenReason, &evidenceJSON,
		&createdAt, &updatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan dispute: %w", err)
	}

	parsedCategory, err := dispute.ParseDisputeCategory(category)
	if err != nil {
		return nil, fmt.Errorf("parse category: %w", err)
	}

	parsedState, err := dispute.ParseDisputeState(state)
	if err != nil {
		return nil, fmt.Errorf("parse state: %w", err)
	}

	var evidence []dispute.Evidence
	if len(evidenceJSON) > 0 && string(evidenceJSON) != "null" {
		if err := json.Unmarshal(evidenceJSON, &evidence); err != nil {
			return nil, fmt.Errorf("unmarshal evidence: %w", err)
		}
	}

	if evidence == nil {
		evidence = []dispute.Evidence{}
	}

	return &dispute.Dispute{
		ID:           id,
		ExceptionID:  exceptionID,
		Category:     parsedCategory,
		State:        parsedState,
		Description:  description,
		OpenedBy:     openedBy,
		Resolution:   pgcommon.NullStringToStringPtr(resolution),
		ReopenReason: pgcommon.NullStringToStringPtr(reopenReason),
		Evidence:     evidence,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}, nil
}

func scanDispute(row *sql.Row) (*dispute.Dispute, error) {
	return scanDisputeInto(row)
}

func scanDisputeRows(rows *sql.Rows) (*dispute.Dispute, error) {
	return scanDisputeInto(rows)
}
