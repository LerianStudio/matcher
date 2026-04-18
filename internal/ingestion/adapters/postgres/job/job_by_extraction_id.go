// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package job

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

// FindLatestByExtractionID returns the most recently created ingestion job
// whose JSONB metadata column carries the supplied extraction_id AND whose
// status is COMPLETED (T-005 P1 + Polish Fix 1).
//
// The status=COMPLETED filter is essential: without it, a Tick 1 that fails
// mid-process (status FAILED/PROCESSING) would still be returned, and the
// orchestrator's link write would attach the extraction to a FAILED ingestion
// job. Filtering to COMPLETED ensures Tick 2 either reuses a successful prior
// job OR creates a fresh one — never links to a failed remnant.
//
// The query is supported by the partial expression index
// idx_ingestion_jobs_metadata_extraction_id (migration 000026), whose WHERE
// predicate now also requires status='COMPLETED' so the index size matches the
// query selectivity.
//
// Returns (nil, nil) — NOT a sentinel — when no row matches. The
// trusted-stream short-circuit treats "no prior job" as the common path; an
// actual SQL error is wrapped and surfaced.
func (repo *Repository) FindLatestByExtractionID(
	ctx context.Context,
	extractionID uuid.UUID,
) (*entities.IngestionJob, error) {
	if repo == nil || repo.provider == nil {
		return nil, errRepoNotInit
	}

	if extractionID == uuid.Nil {
		// Querying the index for empty would always return zero rows, but
		// returning early avoids the round-trip and signals caller intent.
		return nil, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_ingestion_job_by_extraction_id")
	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.IngestionJob, error) {
			row := qe.QueryRowContext(
				ctx,
				`SELECT `+jobColumns+` FROM ingestion_jobs
				WHERE metadata->>'extractionId' = $1
				  AND status = 'COMPLETED'
				ORDER BY created_at DESC
				LIMIT 1`,
				extractionID.String(),
			)

			job, scanErr := scanJob(row)
			if scanErr != nil {
				if errors.Is(scanErr, sql.ErrNoRows) {
					return nil, nil
				}

				return nil, scanErr
			}

			return job, nil
		},
	)
	if err != nil {
		// Never log sql.ErrNoRows — that path is normal-fast-path.
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		wrappedErr := fmt.Errorf("find ingestion job by extraction id: %w", err)
		libOpentelemetry.HandleSpanError(span, "find by extraction id failed", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).
			Log(ctx, libLog.LevelError, "find ingestion job by extraction id failed")

		return nil, wrappedErr
	}

	return result, nil
}
