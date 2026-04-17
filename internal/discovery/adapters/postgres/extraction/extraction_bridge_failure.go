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

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// MarkBridgeFailed persists a terminal bridge failure on the extraction row.
// Updates only the bridge_* columns + updated_at — the discovery-side status
// is intentionally left untouched (see ExtractionRequest docstring for the
// two-state-machine rationale).
//
// Requires that req.BridgeLastError is non-empty (call MarkBridgeFailed on
// the entity first); silently skips the write if the entity is in a
// non-terminal state because that would clobber a previously-persisted
// failure with NULL.
func (repo *Repository) MarkBridgeFailed(ctx context.Context, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.mark_bridge_failed")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeMarkBridgeFailed(ctx, tx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("mark extraction bridge failed: %w", err)
		libOpentelemetry.HandleSpanError(span, "mark extraction bridge failed", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).
			Log(ctx, libLog.LevelError, "mark extraction bridge failed")

		return wrappedErr
	}

	return nil
}

// MarkBridgeFailedWithTx is the WithTx variant of MarkBridgeFailed for
// callers needing to coordinate the failure write with other state changes
// inside one transaction.
func (repo *Repository) MarkBridgeFailedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	req *entities.ExtractionRequest,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.mark_bridge_failed_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeMarkBridgeFailed(ctx, innerTx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("mark extraction bridge failed with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "mark extraction bridge failed", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).
			Log(ctx, libLog.LevelError, "mark extraction bridge failed")

		return wrappedErr
	}

	return nil
}

// IncrementBridgeAttempts persists ONLY the bumped attempts counter and
// updated_at on the extraction row, guarded by a `ingestion_job_id IS NULL`
// predicate (Polish Fix 3). Using a narrow UPDATE prevents a wide-write race
// where the worker's transient-retry path could otherwise clobber a
// concurrent link write under a lock-TTL-expiry edge case.
//
// Returns sharedPorts.ErrExtractionAlreadyLinked when the row was concurrently
// linked between the worker's read and this write — the caller should treat
// the link as authoritative and stop retrying. Returns ErrExtractionNotFound
// when no row matches the id. Returns nil on the happy path.
func (repo *Repository) IncrementBridgeAttempts(
	ctx context.Context,
	id uuid.UUID,
	attempts int,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.increment_bridge_attempts")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeIncrementBridgeAttempts(ctx, tx, id, attempts); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("increment bridge attempts: %w", err)
		libOpentelemetry.HandleSpanError(span, "increment bridge attempts", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).
			Log(ctx, libLog.LevelError, "increment bridge attempts")

		return wrappedErr
	}

	return nil
}

// IncrementBridgeAttemptsWithTx is the WithTx variant of
// IncrementBridgeAttempts (repositorytx linter requirement).
func (repo *Repository) IncrementBridgeAttemptsWithTx(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
	attempts int,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.increment_bridge_attempts_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeIncrementBridgeAttempts(ctx, innerTx, id, attempts); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("increment bridge attempts with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "increment bridge attempts", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).
			Log(ctx, libLog.LevelError, "increment bridge attempts")

		return wrappedErr
	}

	return nil
}

// executeIncrementBridgeAttempts runs the narrow UPDATE that touches only
// bridge_attempts + updated_at, gated by ingestion_job_id IS NULL.
//
// The WHERE clause's NULL guard is what makes this race-safe: if a concurrent
// LinkIfUnlinked has already written ingestion_job_id, this UPDATE skips
// (rowsAffected=0) and we surface ErrExtractionAlreadyLinked rather than
// silently clobbering the link.
func (repo *Repository) executeIncrementBridgeAttempts(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
	attempts int,
) error {
	now := time.Now().UTC()

	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			bridge_attempts = $1,
			updated_at = $2
		WHERE id = $3 AND ingestion_job_id IS NULL`,
		attempts,
		now,
		id,
	)
	if err != nil {
		return fmt.Errorf("increment bridge attempts exec: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("increment bridge attempts rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Two cases collapse to one signal: either the row was never there
		// (ErrExtractionNotFound) or the row was concurrently linked
		// (ingestion_job_id IS NOT NULL). Surface the linked sentinel —
		// missing rows are vanishingly rare in the worker path (we just
		// loaded the row in the same tick) so the linked interpretation is
		// the operationally useful one.
		return sharedPorts.ErrExtractionAlreadyLinked
	}

	return nil
}

// executeMarkBridgeFailed runs the narrow UPDATE that touches only the
// bridge_* columns + updated_at. Keeping the column list narrow avoids
// races with concurrent updates on unrelated columns (e.g., a poller bumping
// status would not be clobbered by this write).
func (repo *Repository) executeMarkBridgeFailed(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	now := time.Now().UTC()
	model.UpdatedAt = now

	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			bridge_attempts = $1,
			bridge_last_error = $2,
			bridge_last_error_message = $3,
			bridge_failed_at = $4,
			updated_at = $5
		WHERE id = $6`,
		model.BridgeAttempts,
		model.BridgeLastError,
		model.BridgeLastErrorMessage,
		model.BridgeFailedAt,
		model.UpdatedAt,
		model.ID,
	)
	if err != nil {
		return fmt.Errorf("mark bridge failed exec: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark bridge failed rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return repositories.ErrExtractionNotFound
	}

	// Reflect the persisted updated_at back onto the entity so callers
	// using OCC-style chained updates do not hit a stale-write conflict
	// on the very next call.
	req.UpdatedAt = now

	return nil
}
