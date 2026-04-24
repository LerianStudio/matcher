// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package match_rule

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
)

// Create inserts a new match rule into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_match_rule")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create match rule", err)

		logger.With(
			libLog.Any("context.id", entity.ContextID.String()),
			libLog.Any("priority", entity.Priority),
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to create match rule")

		return nil, fmt.Errorf("failed to create match rule: %w", err)
	}

	return result, nil
}

// CreateWithTx inserts a new match rule using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_match_rule_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create match rule", err)

		logger.With(
			libLog.Any("context.id", entity.ContextID.String()),
			libLog.Any("priority", entity.Priority),
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to create match rule")

		return nil, fmt.Errorf("failed to create match rule: %w", err)
	}

	return result, nil
}

// executeCreate performs the actual match rule creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	model, err := NewMatchRulePostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO match_rules (id, context_id, priority, type, config, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		model.ID,
		model.ContextID,
		model.Priority,
		model.Type,
		model.Config,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// Update modifies an existing match rule.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_match_rule")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update match rule", err)

			logger.With(
				libLog.Any("context.id", entity.ContextID.String()),
				libLog.Any("match_rule.id", entity.ID.String()),
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to update match rule")
		}

		return nil, fmt.Errorf("failed to update match rule: %w", err)
	}

	return result, nil
}

// UpdateWithTx modifies an existing match rule using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_match_rule_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update match rule", err)

			logger.With(
				libLog.Any("context.id", entity.ContextID.String()),
				libLog.Any("match_rule.id", entity.ID.String()),
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to update match rule")
		}

		return nil, fmt.Errorf("failed to update match rule: %w", err)
	}

	return result, nil
}

// executeUpdate performs the actual match rule update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	entity.UpdatedAt = time.Now().UTC()

	model, err := NewMatchRulePostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE match_rules SET priority = $1, type = $2, config = $3, updated_at = $4
		WHERE context_id = $5 AND id = $6`,
		model.Priority,
		model.Type,
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

// Delete removes a match rule from the database.
func (repo *Repository) Delete(ctx stdctx.Context, contextID, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_match_rule")
	defer span.End()

	_, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, contextID, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete match rule", err)

			logger.With(
				libLog.Any("context.id", contextID.String()),
				libLog.Any("match_rule.id", id.String()),
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to delete match rule")
		}

		return fmt.Errorf("failed to delete match rule: %w", err)
	}

	return nil
}

// DeleteWithTx removes a match rule using the provided transaction.
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

	ctx, span := tracer.Start(ctx, "postgres.delete_match_rule_with_tx")
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
			libOpentelemetry.HandleSpanError(span, "failed to delete match rule", err)

			logger.With(
				libLog.Any("context.id", contextID.String()),
				libLog.Any("match_rule.id", id.String()),
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to delete match rule")
		}

		return fmt.Errorf("failed to delete match rule: %w", err)
	}

	return nil
}

// executeDelete performs the actual match rule deletion within a transaction.
func (repo *Repository) executeDelete(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID, id uuid.UUID,
) (bool, error) {
	result, err := tx.ExecContext(
		ctx,
		"DELETE FROM match_rules WHERE context_id = $1 AND id = $2",
		contextID.String(),
		id.String(),
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
