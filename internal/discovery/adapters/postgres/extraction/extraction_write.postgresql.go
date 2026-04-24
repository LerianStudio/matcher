// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package extraction

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Create persists a new ExtractionRequest.
func (repo *Repository) Create(ctx context.Context, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_extraction_request")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeCreate(ctx, tx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create extraction request: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create extraction request", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create extraction request")

		return wrappedErr
	}

	return nil
}

// CreateWithTx persists a new ExtractionRequest within an existing transaction.
func (repo *Repository) CreateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
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

	ctx, span := tracer.Start(ctx, "postgres.create_extraction_request_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeCreate(ctx, innerTx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create extraction request with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create extraction request", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create extraction request")

		return wrappedErr
	}

	return nil
}

// executeCreate performs the actual insertion within a transaction.
func (repo *Repository) executeCreate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO `+tableName+` (`+allColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		model.ID,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		model.StartDate,
		model.EndDate,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.CreatedAt,
		model.UpdatedAt,
		model.BridgeAttempts,
		model.BridgeLastError,
		model.BridgeLastErrorMessage,
		model.BridgeFailedAt,
		model.CustodyDeletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert extraction request: %w", err)
	}

	return nil
}

// Update persists changes to an existing ExtractionRequest.
func (repo *Repository) Update(ctx context.Context, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpdate(ctx, tx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update extraction request")

		return wrappedErr
	}

	return nil
}

// UpdateIfUnchanged persists changes only if the row still matches the expected updated_at value.
func (repo *Repository) UpdateIfUnchanged(ctx context.Context, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request_if_unchanged")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeConditionalUpdate(ctx, tx, req, expectedUpdatedAt); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request if unchanged: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request if unchanged", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update extraction request if unchanged")

		return wrappedErr
	}

	return nil
}

// UpdateIfUnchangedWithTx persists changes only if the row still matches the
// expected updated_at value within an existing transaction.
func (repo *Repository) UpdateIfUnchangedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	req *entities.ExtractionRequest,
	expectedUpdatedAt time.Time,
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

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request_if_unchanged_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeConditionalUpdate(ctx, innerTx, req, expectedUpdatedAt); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request if unchanged with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request if unchanged", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update extraction request if unchanged")

		return wrappedErr
	}

	return nil
}

// UpdateWithTx persists changes within an existing transaction.
func (repo *Repository) UpdateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
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

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpdate(ctx, innerTx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update extraction request")

		return wrappedErr
	}

	return nil
}

// executeUpdate performs the actual update within a transaction.
func (repo *Repository) executeUpdate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			connection_id = $1,
			ingestion_job_id = $2,
			fetcher_job_id = $3,
			tables = $4,
			start_date = $5,
			end_date = $6,
			filters = $7,
			status = $8,
			result_path = $9,
			error_message = $10,
			updated_at = $11,
			bridge_attempts = $12,
			bridge_last_error = $13,
			bridge_last_error_message = $14,
			bridge_failed_at = $15,
			custody_deleted_at = $16
		WHERE id = $17`,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		model.StartDate,
		model.EndDate,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.UpdatedAt,
		model.BridgeAttempts,
		model.BridgeLastError,
		model.BridgeLastErrorMessage,
		model.BridgeFailedAt,
		model.CustodyDeletedAt,
		model.ID,
	)
	if err != nil {
		return fmt.Errorf("update extraction request: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (repo *Repository) executeConditionalUpdate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			connection_id = $1,
			ingestion_job_id = $2,
			fetcher_job_id = $3,
			tables = $4,
			start_date = $5,
			end_date = $6,
			filters = $7,
			status = $8,
			result_path = $9,
			error_message = $10,
			updated_at = $11,
			bridge_attempts = $12,
			bridge_last_error = $13,
			bridge_last_error_message = $14,
			bridge_failed_at = $15,
			custody_deleted_at = $16
		WHERE id = $17 AND updated_at = $18`,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		model.StartDate,
		model.EndDate,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.UpdatedAt,
		model.BridgeAttempts,
		model.BridgeLastError,
		model.BridgeLastErrorMessage,
		model.BridgeFailedAt,
		model.CustodyDeletedAt,
		model.ID,
		expectedUpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update extraction request conditionally: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return repositories.ErrExtractionConflict
	}

	return nil
}
