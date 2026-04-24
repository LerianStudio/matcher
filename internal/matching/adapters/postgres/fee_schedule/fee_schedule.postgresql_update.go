// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee_schedule

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Update replaces a fee schedule and its items.
func (repo *Repository) Update(ctx context.Context, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	return repo.updateInternal(ctx, nil, schedule)
}

// UpdateWithTx replaces a fee schedule and its items using the provided transaction.
func (repo *Repository) UpdateWithTx(ctx context.Context, tx matchingRepos.Tx, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrInvalidTx
	}

	return repo.updateInternal(ctx, tx, schedule)
}

// updateInternal is the shared implementation for Update and UpdateWithTx.
func (repo *Repository) updateInternal(ctx context.Context, tx *sql.Tx, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_fee_schedule")

	defer span.End()

	model, items, convErr := FromEntity(schedule)
	if convErr != nil {
		return nil, fmt.Errorf("convert to model: %w", convErr)
	}

	if model == nil {
		return nil, ErrFeeScheduleModelNeeded
	}

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) (*fee.FeeSchedule, error) {
			execResult, execErr := execTx.ExecContext(ctx,
				`UPDATE fee_schedules SET name = $1, currency = $2, application_order = $3, rounding_scale = $4, rounding_mode = $5, updated_at = $6
				WHERE id = $7`,
				model.Name,
				model.Currency,
				model.ApplicationOrder,
				model.RoundingScale,
				model.RoundingMode,
				model.UpdatedAt,
				model.ID,
			)
			if execErr != nil {
				return nil, fmt.Errorf("update fee schedule: %w", execErr)
			}

			rowsAffected, execErr := execResult.RowsAffected()
			if execErr != nil {
				return nil, fmt.Errorf("get rows affected: %w", execErr)
			}

			if rowsAffected == 0 {
				return nil, fee.ErrFeeScheduleNotFound
			}

			// Replace items: delete old, insert new
			_, execErr = execTx.ExecContext(ctx,
				"DELETE FROM fee_schedule_items WHERE fee_schedule_id = $1",
				model.ID,
			)
			if execErr != nil {
				return nil, fmt.Errorf("delete old fee schedule items: %w", execErr)
			}

			for idx, item := range items {
				_, execErr = execTx.ExecContext(ctx,
					`INSERT INTO fee_schedule_items (id, fee_schedule_id, name, priority, structure_type, structure_data, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
					item.ID,
					item.FeeScheduleID,
					item.Name,
					item.Priority,
					item.StructureType,
					item.StructureData,
					item.CreatedAt,
					item.UpdatedAt,
				)
				if execErr != nil {
					return nil, fmt.Errorf("insert fee schedule item[%d]: %w", idx, execErr)
				}
			}

			return ToEntity(model, items)
		},
	)
	if err != nil {
		if errors.Is(err, fee.ErrFeeScheduleNotFound) {
			return nil, fee.ErrFeeScheduleNotFound
		}

		wrappedErr := fmt.Errorf("update fee schedule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update fee schedule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update fee schedule")

		return nil, wrappedErr
	}

	return result, nil
}
