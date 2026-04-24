// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee_schedule

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// GetByID retrieves a fee schedule by its ID, including items.
func (repo *Repository) GetByID(ctx context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_fee_schedule_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*fee.FeeSchedule, error) {
			model, err := scanSchedule(tx.QueryRowContext(ctx,
				"SELECT "+scheduleColumns+" FROM fee_schedules WHERE id = $1",
				id.String(),
			))
			if err != nil {
				return nil, err
			}

			items, err := queryItems(ctx, tx, model.ID.String())
			if err != nil {
				return nil, err
			}

			return ToEntity(model, items)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fee.ErrFeeScheduleNotFound
		}

		wrappedErr := fmt.Errorf("get fee schedule by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get fee schedule by id", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to get fee schedule by id")

		return nil, wrappedErr
	}

	return result, nil
}

// defaultListLimit is the default maximum number of fee schedules returned by List.
const defaultListLimit = 100

// List retrieves fee schedules for the current tenant, limited by the given limit.
// If limit is <= 0, the default limit of 100 is used.
func (repo *Repository) List(ctx context.Context, limit int) ([]*fee.FeeSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if limit <= 0 {
		limit = defaultListLimit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_fee_schedules")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*fee.FeeSchedule, error) {
			rows, queryErr := tx.QueryContext(ctx,
				"SELECT "+scheduleColumns+" FROM fee_schedules ORDER BY name LIMIT $1",
				limit,
			)
			if queryErr != nil {
				return nil, queryErr
			}

			defer func() {
				_ = rows.Close()
			}()

			models, scanErr := scanScheduleRows(rows)
			if scanErr != nil {
				return nil, scanErr
			}

			ids := make([]string, 0, len(models))
			for _, m := range models {
				ids = append(ids, m.ID.String())
			}

			groupedItems, itemsErr := queryItemsForSchedules(ctx, tx, ids)
			if itemsErr != nil {
				return nil, itemsErr
			}

			schedules := make([]*fee.FeeSchedule, 0, len(models))

			for _, model := range models {
				entity, convErr := ToEntity(model, groupedItems[model.ID.String()])
				if convErr != nil {
					return nil, convErr
				}

				schedules = append(schedules, entity)
			}

			return schedules, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list fee schedules: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list fee schedules", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to list fee schedules")

		return nil, wrappedErr
	}

	return result, nil
}

// GetByIDs retrieves fee schedules by their IDs, returning a map of ID to schedule.
func (repo *Repository) GetByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if len(ids) == 0 {
		return make(map[uuid.UUID]*fee.FeeSchedule), nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_fee_schedules_by_ids")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (map[uuid.UUID]*fee.FeeSchedule, error) {
			scheduleIDs := make([]string, 0, len(ids))
			for _, id := range ids {
				scheduleIDs = append(scheduleIDs, id.String())
			}

			query, args, queryErr := squirrel.StatementBuilder.
				PlaceholderFormat(squirrel.Dollar).
				Select(scheduleColumns).
				From("fee_schedules").
				Where(squirrel.Eq{"id": scheduleIDs}).
				ToSql()
			if queryErr != nil {
				return nil, fmt.Errorf("build fee schedules by ids query: %w", queryErr)
			}

			rows, queryErr := tx.QueryContext(ctx, query, args...)
			if queryErr != nil {
				return nil, queryErr
			}

			defer func() {
				_ = rows.Close()
			}()

			models, scanErr := scanScheduleRows(rows)
			if scanErr != nil {
				return nil, scanErr
			}

			modelIDs := make([]string, 0, len(models))
			for _, m := range models {
				modelIDs = append(modelIDs, m.ID.String())
			}

			groupedItems, itemsErr := queryItemsForSchedules(ctx, tx, modelIDs)
			if itemsErr != nil {
				return nil, itemsErr
			}

			scheduleMap := make(map[uuid.UUID]*fee.FeeSchedule, len(models))

			for _, model := range models {
				entity, convErr := ToEntity(model, groupedItems[model.ID.String()])
				if convErr != nil {
					return nil, convErr
				}

				scheduleMap[entity.ID] = entity
			}

			return scheduleMap, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get fee schedules by ids: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get fee schedules by ids", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to get fee schedules by ids")

		return nil, wrappedErr
	}

	return result, nil
}
