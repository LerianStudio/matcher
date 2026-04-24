// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee_rule

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const feeRuleColumns = "id, context_id, side, fee_schedule_id, name, priority, predicates, created_at, updated_at"

// Repository provides PostgreSQL operations for fee rules.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new fee rule repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new fee rule into the database.
func (repo *Repository) Create(ctx stdctx.Context, rule *fee.FeeRule) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if rule == nil {
		return ErrFeeRuleEntityNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_fee_rule")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (bool, error) {
			return true, repo.executeCreate(ctx, tx, rule)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create fee rule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create fee rule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create fee rule")

		return wrappedErr
	}

	return nil
}

// CreateWithTx inserts a new fee rule using the provided transaction.
func (repo *Repository) CreateWithTx(ctx stdctx.Context, tx *sql.Tx, rule *fee.FeeRule) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if rule == nil {
		return ErrFeeRuleEntityNil
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_fee_rule_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return true, repo.executeCreate(ctx, innerTx, rule)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create fee rule with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create fee rule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create fee rule")

		return wrappedErr
	}

	return nil
}

// executeCreate performs the actual fee rule creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	rule *fee.FeeRule,
) error {
	model, err := NewPostgreSQLModel(rule)
	if err != nil {
		return fmt.Errorf("create fee rule model: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO fee_rules (id, context_id, side, fee_schedule_id, name, priority, predicates, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		model.ID,
		model.ContextID,
		model.Side,
		model.FeeScheduleID,
		model.Name,
		model.Priority,
		model.Predicates,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert fee rule: %w", err)
	}

	return nil
}

// FindByID retrieves a fee rule by its ID.
func (repo *Repository) FindByID(
	ctx stdctx.Context,
	id uuid.UUID,
) (*fee.FeeRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_fee_rule_by_id")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*fee.FeeRule, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT "+feeRuleColumns+" FROM fee_rules WHERE id = $1",
				id.String(),
			)

			return scanFeeRule(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fee.ErrFeeRuleNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to find fee rule", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find fee rule by id")

		return nil, fmt.Errorf("find fee rule by id: %w", err)
	}

	return result, nil
}

// FindByContextID retrieves all fee rules for a context, ordered by priority ascending.
func (repo *Repository) FindByContextID(
	ctx stdctx.Context,
	contextID uuid.UUID,
) ([]*fee.FeeRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_fee_rules_by_context")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*fee.FeeRule, error) {
			rows, err := tx.QueryContext(
				ctx,
				"SELECT "+feeRuleColumns+" FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC",
				contextID.String(),
			)
			if err != nil {
				return nil, err
			}

			defer rows.Close()

			var rules []*fee.FeeRule

			for rows.Next() {
				rule, scanErr := scanFeeRule(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				rules = append(rules, rule)
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return rules, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list fee rules", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list fee rules by context")

		return nil, fmt.Errorf("find fee rules by context: %w", err)
	}

	return result, nil
}

// FindByContextIDWithTx retrieves all fee rules for a context using an existing
// transaction. This enables consistent snapshot reads when the caller already
// holds a transaction (e.g. clone operations).
func (repo *Repository) FindByContextIDWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
) ([]*fee.FeeRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_fee_rules_by_context_with_tx")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) ([]*fee.FeeRule, error) {
			rows, err := innerTx.QueryContext(
				ctx,
				"SELECT "+feeRuleColumns+" FROM fee_rules WHERE context_id = $1 ORDER BY priority ASC",
				contextID.String(),
			)
			if err != nil {
				return nil, err
			}

			defer rows.Close()

			var rules []*fee.FeeRule

			for rows.Next() {
				rule, scanErr := scanFeeRule(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				rules = append(rules, rule)
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return rules, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list fee rules with tx", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list fee rules by context with tx")

		return nil, fmt.Errorf("find fee rules by context with tx: %w", err)
	}

	return result, nil
}

// Update modifies an existing fee rule.
func (repo *Repository) Update(ctx stdctx.Context, rule *fee.FeeRule) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if rule == nil {
		return ErrFeeRuleEntityNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_fee_rule")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (bool, error) {
			return repo.executeUpdate(ctx, tx, rule)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fee.ErrFeeRuleNotFound
		}

		wrappedErr := fmt.Errorf("update fee rule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update fee rule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update fee rule")

		return wrappedErr
	}

	return nil
}

// UpdateWithTx modifies an existing fee rule using the provided transaction.
func (repo *Repository) UpdateWithTx(ctx stdctx.Context, tx *sql.Tx, rule *fee.FeeRule) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if rule == nil {
		return ErrFeeRuleEntityNil
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_fee_rule_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeUpdate(ctx, innerTx, rule)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("update fee rule with tx: %w", err)
			libOpentelemetry.HandleSpanError(span, "failed to update fee rule", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update fee rule")

			return wrappedErr
		}

		return fmt.Errorf("update fee rule with tx: %w", err)
	}

	return nil
}

// executeUpdate performs the actual fee rule update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	rule *fee.FeeRule,
) (bool, error) {
	model, err := NewPostgreSQLModel(rule)
	if err != nil {
		return false, fmt.Errorf("create fee rule model: %w", err)
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE fee_rules
				SET side = $1, fee_schedule_id = $2, name = $3, priority = $4, predicates = $5, updated_at = $6
				WHERE id = $7 AND context_id = $8`,
		model.Side,
		model.FeeScheduleID,
		model.Name,
		model.Priority,
		model.Predicates,
		model.UpdatedAt,
		model.ID,
		model.ContextID,
	)
	if err != nil {
		return false, fmt.Errorf("update fee rule: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return false, sql.ErrNoRows
	}

	return true, nil
}

// Delete removes a fee rule by ID, scoped to a context for defense-in-depth.
func (repo *Repository) Delete(ctx stdctx.Context, contextID, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_fee_rule")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, contextID, id)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fee.ErrFeeRuleNotFound
		}

		wrappedErr := fmt.Errorf("delete fee rule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete fee rule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete fee rule")

		return wrappedErr
	}

	return nil
}

// DeleteWithTx removes a fee rule by ID using the provided transaction, scoped to a context.
func (repo *Repository) DeleteWithTx(ctx stdctx.Context, tx *sql.Tx, contextID, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_fee_rule_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, innerTx, contextID, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("delete fee rule with tx: %w", err)
			libOpentelemetry.HandleSpanError(span, "failed to delete fee rule", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete fee rule")

			return wrappedErr
		}

		return fmt.Errorf("delete fee rule with tx: %w", err)
	}

	return nil
}

// executeDelete performs the actual fee rule deletion within a transaction.
func (repo *Repository) executeDelete(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	id uuid.UUID,
) (bool, error) {
	res, err := tx.ExecContext(
		ctx,
		"DELETE FROM fee_rules WHERE id = $1 AND context_id = $2",
		id.String(),
		contextID.String(),
	)
	if err != nil {
		return false, fmt.Errorf("delete fee rule: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return false, sql.ErrNoRows
	}

	return true, nil
}

// scanFeeRule scans a single fee rule row into a domain entity.
func scanFeeRule(
	scanner interface{ Scan(dest ...any) error },
) (*fee.FeeRule, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.Side,
		&model.FeeScheduleID,
		&model.Name,
		&model.Priority,
		&model.Predicates,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}
