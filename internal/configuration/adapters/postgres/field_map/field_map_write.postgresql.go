// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package field_map

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

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Create inserts a new field map into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.field_map.create")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*shared.FieldMap, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create field map: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create field map", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create field map")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new field map using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_field_map_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*shared.FieldMap, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create field map with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create field map", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create field map")

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual field map creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	model, err := NewFieldMapPostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO field_maps (id, context_id, source_id, mapping, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		model.ID,
		model.ContextID,
		model.SourceID,
		model.Mapping,
		model.Version,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// Update modifies an existing field map.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_field_map")
	defer span.End()

	entity.UpdatedAt = time.Now().UTC()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*shared.FieldMap, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update field map: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update field map", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update field map")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// UpdateWithTx modifies an existing field map using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_field_map_with_tx")
	defer span.End()

	entity.UpdatedAt = time.Now().UTC()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*shared.FieldMap, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update field map with tx: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update field map", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update field map")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// executeUpdate performs the actual field map update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	model, err := NewFieldMapPostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE field_maps SET mapping = $1, version = $2, updated_at = $3 WHERE id = $4`,
		model.Mapping,
		model.Version,
		model.UpdatedAt,
		model.ID,
	)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected == 0 {
		return nil, sql.ErrNoRows
	}

	return model.ToEntity()
}

// Delete removes a field map from the database.
func (repo *Repository) Delete(ctx stdctx.Context, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_field_map")
	defer span.End()

	_, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, id)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("delete field map: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete field map", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete field map")
		}

		return wrappedErr
	}

	return nil
}

// DeleteWithTx removes a field map using the provided transaction.
func (repo *Repository) DeleteWithTx(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_field_map_with_tx")
	defer span.End()

	_, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, id)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("delete field map with tx: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete field map", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete field map")
		}

		return wrappedErr
	}

	return nil
}

// executeDelete performs the actual field map deletion within a transaction.
func (repo *Repository) executeDelete(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) (bool, error) {
	result, err := tx.ExecContext(ctx, "DELETE FROM field_maps WHERE id = $1", id.String())
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	if rowsAffected == 0 {
		return false, sql.ErrNoRows
	}

	return true, nil
}
