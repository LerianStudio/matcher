// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package source

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
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const sourceColumns = "id, context_id, name, type, side, config, created_at, updated_at"

// Repository provides PostgreSQL operations for reconciliation sources.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new source repository.
func NewRepository(provider ports.InfrastructureProvider) (*Repository, error) {
	if provider == nil {
		return nil, ErrConnectionRequired
	}

	return &Repository{provider: provider}, nil
}

// Create inserts a new reconciliation source into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrSourceEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.create")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create reconciliation source", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to create reconciliation source")

		return nil, fmt.Errorf("failed to create reconciliation source: %w", err)
	}

	return result, nil
}

// CreateWithTx inserts a new reconciliation source using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrSourceEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.create_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create reconciliation source", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to create reconciliation source")

		return nil, fmt.Errorf("failed to create reconciliation source: %w", err)
	}

	return result, nil
}

// executeCreate performs the actual source creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	model, err := NewSourcePostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		model.ID,
		model.ContextID,
		model.Name,
		model.Type,
		model.Side,
		model.Config,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// Update modifies an existing reconciliation source.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrSourceEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.update")
	defer span.End()

	entity.UpdatedAt = time.Now().UTC()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update reconciliation source", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update reconciliation source")
		}

		return nil, fmt.Errorf("failed to update reconciliation source: %w", err)
	}

	return result, nil
}

// UpdateWithTx modifies an existing reconciliation source using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrSourceEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.update_with_tx")
	defer span.End()

	entity.UpdatedAt = time.Now().UTC()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update reconciliation source", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update reconciliation source")
		}

		return nil, fmt.Errorf("failed to update reconciliation source: %w", err)
	}

	return result, nil
}

// executeUpdate performs the actual source update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	model, err := NewSourcePostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE reconciliation_sources SET name = $1, type = $2, side = $3, config = $4, updated_at = $5
		WHERE context_id = $6 AND id = $7`,
		model.Name,
		model.Type,
		model.Side,
		model.Config,
		model.UpdatedAt,
		model.ContextID,
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

// Delete removes a reconciliation source from the database.
func (repo *Repository) Delete(ctx stdctx.Context, contextID, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.delete")
	defer span.End()

	_, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, contextID, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete reconciliation source", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to delete reconciliation source")
		}

		return fmt.Errorf("failed to delete reconciliation source: %w", err)
	}

	return nil
}

// DeleteWithTx removes a reconciliation source using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) DeleteWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID, id uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.delete_with_tx")
	defer span.End()

	_, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, contextID, id)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete reconciliation source", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to delete reconciliation source")
		}

		return fmt.Errorf("failed to delete reconciliation source: %w", err)
	}

	return nil
}

// executeDelete performs the actual source deletion within a transaction.
func (repo *Repository) executeDelete(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID, id uuid.UUID,
) (bool, error) {
	result, err := tx.ExecContext(
		ctx,
		"DELETE FROM reconciliation_sources WHERE context_id = $1 AND id = $2",
		contextID,
		id,
	)
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
