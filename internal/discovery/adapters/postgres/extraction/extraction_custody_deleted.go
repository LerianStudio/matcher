// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package extraction

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// MarkCustodyDeleted persists the terminal "custody object is gone" marker on
// the extraction row identified by id. Narrow UPDATE touches only
// custody_deleted_at — the discovery status, bridge_* columns, and ingestion
// link are left alone so this write can race safely with any other path.
//
// Used by both the bridge orchestrator's happy-path cleanupCustody hook and
// the custody retention worker's sweep. Idempotent: writing the same marker
// twice is a no-op at the row level (last-write-wins on the timestamp).
//
// The partial index idx_extraction_requests_custody_pending_cleanup (see
// migration 000027) uses custody_deleted_at IS NULL, so once this write
// completes, the row drops out of the retention sweep's candidate scan and
// sweeps converge to idle.
func (repo *Repository) MarkCustodyDeleted(
	ctx context.Context,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.mark_custody_deleted")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeMarkCustodyDeleted(ctx, tx, id, deletedAt); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("mark custody deleted: %w", err)
		libOpentelemetry.HandleSpanError(span, "mark custody deleted", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).
			Log(ctx, libLog.LevelError, "mark custody deleted")

		return wrappedErr
	}

	return nil
}

// MarkCustodyDeletedWithTx is the WithTx variant of MarkCustodyDeleted
// (repositorytx linter requirement).
func (repo *Repository) MarkCustodyDeletedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.mark_custody_deleted_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeMarkCustodyDeleted(ctx, innerTx, id, deletedAt); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("mark custody deleted with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "mark custody deleted", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).
			Log(ctx, libLog.LevelError, "mark custody deleted")

		return wrappedErr
	}

	return nil
}

// executeMarkCustodyDeleted runs the narrow UPDATE that touches ONLY
// custody_deleted_at (no other columns, no updated_at bump). Keeping the
// write this narrow avoids clobbering concurrent writes from the bridge
// worker or link path — both of which may legitimately write other columns
// on the same row at roughly the same time.
//
// Note: we intentionally do NOT bump updated_at here. The retention sweep's
// LATE-LINKED predicate reads updated_at, and bumping it would make the row
// re-enter the sweep window on the next tick. The custody_deleted_at
// convergence guard replaces that need.
func (repo *Repository) executeMarkCustodyDeleted(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			custody_deleted_at = $1
		WHERE id = $2`,
		deletedAt.UTC(),
		id,
	)
	if err != nil {
		return fmt.Errorf("mark custody deleted exec: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark custody deleted rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return repositories.ErrExtractionNotFound
	}

	return nil
}
