// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package extraction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// LinkIfUnlinked atomically sets ingestion_job_id on the extraction row when
// the current value is NULL. Returns sharedPorts.ErrExtractionAlreadyLinked
// when the row exists but already carries an ingestion_job_id; returns
// repositories.ErrExtractionNotFound when no row matches id.
//
// The UPDATE runs a single predicate — id = $3 AND ingestion_job_id IS NULL —
// so concurrent bridge invocations cannot both succeed. RowsAffected
// discriminates "not found" from "already linked" via a tiny follow-up SELECT
// that fires only on the zero-rows-affected path to keep the hot path
// transaction-local.
func (repo *Repository) LinkIfUnlinked(
	ctx context.Context,
	id uuid.UUID,
	ingestionJobID uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if id == uuid.Nil {
		return ports.ErrLinkExtractionIDRequired
	}

	if ingestionJobID == uuid.Nil {
		return ports.ErrLinkIngestionJobIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.link_if_unlinked")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return linkIfUnlinkedTx(ctx, tx, id, ingestionJobID)
	})
	if err != nil {
		// Wrap with %w so callers can still errors.Is on the original
		// sentinel while the error chain carries the repository context.
		wrappedErr := fmt.Errorf("link extraction if unlinked: %w", err)

		if isLinkSentinelError(err) {
			return wrappedErr
		}

		libOpentelemetry.HandleSpanError(span, "atomic link failed", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "atomic link failed")

		return wrappedErr
	}

	return nil
}

// linkIfUnlinkedTx runs the atomic UPDATE + follow-up probe inside a tenant-
// scoped transaction. Extracted from LinkIfUnlinked so the caller stays under
// the gocyclo ceiling; semantics are unchanged.
func linkIfUnlinkedTx(ctx context.Context, tx *sql.Tx, id, ingestionJobID uuid.UUID) (bool, error) {
	updatedAt := time.Now().UTC()

	result, execErr := tx.ExecContext(ctx,
		`UPDATE `+tableName+`
		SET ingestion_job_id = $1, updated_at = $2
		WHERE id = $3 AND ingestion_job_id IS NULL`,
		ingestionJobID,
		updatedAt,
		id,
	)
	if execErr != nil {
		return false, fmt.Errorf("atomic link extraction: %w", execErr)
	}

	rowsAffected, raErr := result.RowsAffected()
	if raErr != nil {
		return false, fmt.Errorf("atomic link extraction rows affected: %w", raErr)
	}

	if rowsAffected != 0 {
		return true, nil
	}

	// Zero rows affected means either the row does not exist or it is
	// already linked. Differentiate with a narrow probe so callers get a
	// precise sentinel.
	var hasIngestion sql.NullBool

	probeErr := tx.QueryRowContext(ctx,
		`SELECT ingestion_job_id IS NOT NULL FROM `+tableName+` WHERE id = $1`,
		id,
	).Scan(&hasIngestion)
	if errors.Is(probeErr, sql.ErrNoRows) {
		return false, repositories.ErrExtractionNotFound
	}

	if probeErr != nil {
		return false, fmt.Errorf("atomic link extraction probe: %w", probeErr)
	}

	if hasIngestion.Valid && hasIngestion.Bool {
		return false, ports.ErrExtractionAlreadyLinked
	}

	// Row exists and is NULL but the UPDATE still matched zero rows — this
	// should be impossible short of a tenant-schema misconfiguration. Surface
	// it as a conflict so the caller can retry in a new cycle.
	return false, repositories.ErrExtractionConflict
}

// isLinkSentinelError reports whether err matches one of the expected
// domain/port sentinels surfaced by LinkIfUnlinked. Centralised here so
// LinkIfUnlinked stays under the gocyclo ceiling and the sentinel set has a
// single authoritative list.
func isLinkSentinelError(err error) bool {
	return errors.Is(err, repositories.ErrExtractionNotFound) ||
		errors.Is(err, ports.ErrExtractionAlreadyLinked) ||
		errors.Is(err, repositories.ErrExtractionConflict) ||
		errors.Is(err, ports.ErrLinkExtractionIDRequired) ||
		errors.Is(err, ports.ErrLinkIngestionJobIDRequired)
}
