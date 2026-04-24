// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee_schedule

import (
	"context"
	"database/sql"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Create inserts a new fee schedule with its items.
func (repo *Repository) Create(ctx context.Context, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	return repo.createInternal(ctx, nil, schedule)
}

// CreateWithTx inserts a new fee schedule using the provided transaction.
func (repo *Repository) CreateWithTx(ctx context.Context, tx matchingRepos.Tx, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrInvalidTx
	}

	return repo.createInternal(ctx, tx, schedule)
}

// createInternal is the shared implementation for Create and CreateWithTx.
func (repo *Repository) createInternal(ctx context.Context, tx *sql.Tx, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_fee_schedule")

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
			_, execErr := execTx.ExecContext(ctx,
				`INSERT INTO fee_schedules (id, tenant_id, name, currency, application_order, rounding_scale, rounding_mode, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				model.ID,
				model.TenantID,
				model.Name,
				model.Currency,
				model.ApplicationOrder,
				model.RoundingScale,
				model.RoundingMode,
				model.CreatedAt,
				model.UpdatedAt,
			)
			if execErr != nil {
				return nil, fmt.Errorf("insert fee schedule: %w", execErr)
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
		wrappedErr := fmt.Errorf("create fee schedule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create fee schedule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create fee schedule")

		return nil, wrappedErr
	}

	return result, nil
}
