// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package context

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Create inserts a new reconciliation context into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_reconciliation_context")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create reconciliation context: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to create reconciliation context",
			wrappedErr,
		)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create reconciliation context")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new reconciliation context using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_reconciliation_context_with_tx")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create reconciliation context: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to create reconciliation context",
			wrappedErr,
		)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create reconciliation context")

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual context creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	model, err := NewContextPostgreSQLModel(entity)
	if err != nil {
		return nil, fmt.Errorf("create context model: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO reconciliation_contexts (id, tenant_id, name, type, interval, status, fee_tolerance_abs, fee_tolerance_pct, fee_normalization, auto_match_on_upload, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		model.ID,
		model.TenantID,
		model.Name,
		model.Type,
		model.Interval,
		model.Status,
		model.FeeToleranceAbs,
		model.FeeTolerancePct,
		model.FeeNormalization,
		model.AutoMatchOnUpload,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert reconciliation context: %w", err)
	}

	return model.ToEntity()
}

// Update modifies an existing reconciliation context.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_reconciliation_context")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("update reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to update reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update reconciliation context")

			return nil, wrappedErr
		}

		return nil, fmt.Errorf("update reconciliation context: %w", err)
	}

	return result, nil
}

// UpdateWithTx modifies an existing reconciliation context using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_reconciliation_context_with_tx")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("update reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to update reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update reconciliation context")

			return nil, wrappedErr
		}

		return nil, fmt.Errorf("update reconciliation context: %w", err)
	}

	return result, nil
}

// executeUpdate performs the actual context update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	entity.UpdatedAt = time.Now().UTC()

	model, err := NewContextPostgreSQLModel(entity)
	if err != nil {
		return nil, fmt.Errorf("create context model: %w", err)
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE reconciliation_contexts SET name = $1, type = $2, interval = $3, status = $4, fee_tolerance_abs = $5, fee_tolerance_pct = $6, fee_normalization = $7, auto_match_on_upload = $8, updated_at = $9
		WHERE tenant_id = $10 AND id = $11`,
		model.Name,
		model.Type,
		model.Interval,
		model.Status,
		model.FeeToleranceAbs,
		model.FeeTolerancePct,
		model.FeeNormalization,
		model.AutoMatchOnUpload,
		model.UpdatedAt,
		model.TenantID,
		model.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("update reconciliation context: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return nil, sql.ErrNoRows
	}

	return model.ToEntity()
}

// Delete removes a reconciliation context from the database.
func (repo *Repository) Delete(ctx stdctx.Context, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_reconciliation_context")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("delete reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to delete reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete reconciliation context")

			return wrappedErr
		}

		return fmt.Errorf("delete reconciliation context: %w", err)
	}

	return nil
}

// DeleteWithTx removes a reconciliation context using the provided transaction.
func (repo *Repository) DeleteWithTx(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_reconciliation_context_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, id)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("delete reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to delete reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete reconciliation context")

			return wrappedErr
		}

		return fmt.Errorf("delete reconciliation context: %w", err)
	}

	return nil
}

// executeDelete performs the actual context deletion within a transaction.
func (repo *Repository) executeDelete(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) (bool, error) {
	tenantID := auth.GetTenantID(ctx)

	result, err := tx.ExecContext(
		ctx,
		"DELETE FROM reconciliation_contexts WHERE tenant_id = $1 AND id = $2",
		tenantID,
		id.String(),
	)
	if err != nil {
		return false, fmt.Errorf("delete reconciliation context: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return false, sql.ErrNoRows
	}

	return true, nil
}
