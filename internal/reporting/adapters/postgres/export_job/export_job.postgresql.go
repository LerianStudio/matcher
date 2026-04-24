// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package export_job

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const exportJobsTable = "export_jobs"

const uuidSchemaRegex = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

// safeLimit converts a non-negative int to uint64 for squirrel's Limit method.
// Returns 0 for negative values (which squirrel treats as no limit).
func safeLimit(n int) uint64 {
	if n <= 0 {
		return 0
	}

	return uint64(n)
}

func trimExportJobsAndEncodeNextCursor(
	records []*entities.ExportJob,
	limit int,
	encodeCursor func(time.Time, uuid.UUID) (string, error),
) ([]*entities.ExportJob, string, error) {
	trimmedRecords, nextCursor, err := pgcommon.TrimRecordsAndEncodeTimestampNextCursor(
		records,
		limit,
		func(job *entities.ExportJob) (time.Time, uuid.UUID) {
			if job == nil {
				return time.Time{}, uuid.Nil
			}

			return job.CreatedAt, job.ID
		},
		encodeCursor,
	)
	if err != nil {
		if errors.Is(err, pgcommon.ErrCursorEncoderRequired) {
			return trimmedRecords, "", fmt.Errorf("encode next cursor: %w", ErrCursorEncoderRequired)
		}

		return trimmedRecords, "", fmt.Errorf("trim records and encode export job cursor: %w", err)
	}

	return trimmedRecords, nextCursor, nil
}

// Repository persists export jobs in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new export job repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func exportJobColumns() []string {
	return []string{
		"id", "tenant_id", "context_id", "report_type", "format", "filter",
		"status", "records_written", "bytes_written", "file_key", "file_name",
		"sha256", "error", "attempts", "next_retry_at", "created_at", "started_at", "finished_at", "expires_at", "updated_at",
	}
}

func exportJobColumnsString() string {
	return "id, tenant_id, context_id, report_type, format, filter, status, records_written, bytes_written, file_key, file_name, sha256, error, attempts, next_retry_at, created_at, started_at, finished_at, expires_at, updated_at"
}

func scanExportJob(row *sql.Row) (*entities.ExportJob, error) {
	var job entities.ExportJob

	var filterJSON []byte

	var fileKey, fileName, sha256Hash, errMsg sql.NullString

	var nextRetryAt, startedAt, finishedAt sql.NullTime

	err := row.Scan(
		&job.ID,
		&job.TenantID,
		&job.ContextID,
		&job.ReportType,
		&job.Format,
		&filterJSON,
		&job.Status,
		&job.RecordsWritten,
		&job.BytesWritten,
		&fileKey,
		&fileName,
		&sha256Hash,
		&errMsg,
		&job.Attempts,
		&nextRetryAt,
		&job.CreatedAt,
		&startedAt,
		&finishedAt,
		&job.ExpiresAt,
		&job.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, repositories.ErrExportJobNotFound
		}

		return nil, fmt.Errorf("scan export job: %w", err)
	}

	if err := json.Unmarshal(filterJSON, &job.Filter); err != nil {
		return nil, fmt.Errorf("unmarshal filter: %w", err)
	}

	job.FileKey = fileKey.String
	job.FileName = fileName.String
	job.SHA256 = sha256Hash.String
	job.Error = errMsg.String

	if nextRetryAt.Valid {
		job.NextRetryAt = &nextRetryAt.Time
	}

	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}

	if finishedAt.Valid {
		job.FinishedAt = &finishedAt.Time
	}

	return &job, nil
}

func scanExportJobs(rows *sql.Rows, err error) ([]*entities.ExportJob, error) {
	if err != nil {
		return nil, fmt.Errorf("query export jobs: %w", err)
	}

	defer rows.Close()

	var jobs []*entities.ExportJob

	for rows.Next() {
		var job entities.ExportJob

		var filterJSON []byte

		var fileKey, fileName, sha256Hash, errMsg sql.NullString

		var nextRetryAt, startedAt, finishedAt sql.NullTime

		err := rows.Scan(
			&job.ID,
			&job.TenantID,
			&job.ContextID,
			&job.ReportType,
			&job.Format,
			&filterJSON,
			&job.Status,
			&job.RecordsWritten,
			&job.BytesWritten,
			&fileKey,
			&fileName,
			&sha256Hash,
			&errMsg,
			&job.Attempts,
			&nextRetryAt,
			&job.CreatedAt,
			&startedAt,
			&finishedAt,
			&job.ExpiresAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan export job row: %w", err)
		}

		if err := json.Unmarshal(filterJSON, &job.Filter); err != nil {
			return nil, fmt.Errorf("unmarshal filter: %w", err)
		}

		job.FileKey = fileKey.String
		job.FileName = fileName.String
		job.SHA256 = sha256Hash.String
		job.Error = errMsg.String

		if nextRetryAt.Valid {
			job.NextRetryAt = pointers.Time(nextRetryAt.Time)
		}

		if startedAt.Valid {
			job.StartedAt = pointers.Time(startedAt.Time)
		}

		if finishedAt.Valid {
			job.FinishedAt = pointers.Time(finishedAt.Time)
		}

		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate export jobs: %w", err)
	}

	return jobs, nil
}
