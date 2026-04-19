// Package fee_schedule provides PostgreSQL persistence for fee schedule entities.
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

	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	scheduleColumns = "id, tenant_id, name, currency, application_order, rounding_scale, rounding_mode, created_at, updated_at"
	itemColumns     = "id, fee_schedule_id, name, priority, structure_type, structure_data, created_at, updated_at"
)

// Repository persists fee schedules in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new fee schedule repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

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

			items, err := queryItems(ctx, tx, model.ID)
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

// Delete removes a fee schedule by ID. CASCADE handles items.
func (repo *Repository) Delete(ctx context.Context, id uuid.UUID) error {
	return repo.deleteInternal(ctx, nil, id)
}

// DeleteWithTx removes a fee schedule by ID using the provided transaction.
func (repo *Repository) DeleteWithTx(ctx context.Context, tx matchingRepos.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrInvalidTx
	}

	return repo.deleteInternal(ctx, tx, id)
}

// deleteInternal is the shared implementation for Delete and DeleteWithTx.
func (repo *Repository) deleteInternal(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.delete_fee_schedule")

	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(execTx *sql.Tx) (bool, error) {
		result, execErr := execTx.ExecContext(ctx,
			"DELETE FROM fee_schedules WHERE id = $1",
			id.String(),
		)
		if execErr != nil {
			return false, fmt.Errorf("delete fee schedule: %w", execErr)
		}

		rowsAffected, execErr := result.RowsAffected()
		if execErr != nil {
			return false, fmt.Errorf("get rows affected: %w", execErr)
		}

		if rowsAffected == 0 {
			return false, sql.ErrNoRows
		}

		return true, nil
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fee.ErrFeeScheduleNotFound
		}

		wrappedErr := fmt.Errorf("delete fee schedule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete fee schedule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete fee schedule")

		return wrappedErr
	}

	return nil
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
				ids = append(ids, m.ID)
			}

			groupedItems, itemsErr := queryItemsForSchedules(ctx, tx, ids)
			if itemsErr != nil {
				return nil, itemsErr
			}

			schedules := make([]*fee.FeeSchedule, 0, len(models))

			for _, model := range models {
				entity, convErr := ToEntity(model, groupedItems[model.ID])
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
				modelIDs = append(modelIDs, m.ID)
			}

			groupedItems, itemsErr := queryItemsForSchedules(ctx, tx, modelIDs)
			if itemsErr != nil {
				return nil, itemsErr
			}

			scheduleMap := make(map[uuid.UUID]*fee.FeeSchedule, len(models))

			for _, model := range models {
				entity, convErr := ToEntity(model, groupedItems[model.ID])
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

// scanScheduleRows iterates over sql.Rows and scans each row into a PostgreSQLModel.
func scanScheduleRows(rows *sql.Rows) ([]*PostgreSQLModel, error) {
	var models []*PostgreSQLModel

	for rows.Next() {
		model, scanErr := scanSchedule(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		models = append(models, model)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return models, nil
}

func scanSchedule(scanner interface{ Scan(dest ...any) error }) (*PostgreSQLModel, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.TenantID,
		&model.Name,
		&model.Currency,
		&model.ApplicationOrder,
		&model.RoundingScale,
		&model.RoundingMode,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return &model, nil
}

func queryItems(ctx context.Context, tx *sql.Tx, scheduleID string) ([]ItemPostgreSQLModel, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT "+itemColumns+" FROM fee_schedule_items WHERE fee_schedule_id = $1 ORDER BY priority",
		scheduleID,
	)
	if err != nil {
		return nil, fmt.Errorf("query fee schedule items: %w", err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var items []ItemPostgreSQLModel

	for rows.Next() {
		var item ItemPostgreSQLModel
		if err := rows.Scan(
			&item.ID,
			&item.FeeScheduleID,
			&item.Name,
			&item.Priority,
			&item.StructureType,
			&item.StructureData,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan fee schedule item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func queryItemsForSchedules(ctx context.Context, tx *sql.Tx, scheduleIDs []string) (map[string][]ItemPostgreSQLModel, error) {
	if len(scheduleIDs) == 0 {
		return make(map[string][]ItemPostgreSQLModel), nil
	}

	query, args, err := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Select(itemColumns).
		From("fee_schedule_items").
		Where(squirrel.Eq{"fee_schedule_id": scheduleIDs}).
		OrderBy("fee_schedule_id, priority").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build fee schedule items batch query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query fee schedule items batch: %w", err)
	}

	defer func() { _ = rows.Close() }()

	grouped := make(map[string][]ItemPostgreSQLModel)

	for rows.Next() {
		var item ItemPostgreSQLModel
		if err := rows.Scan(
			&item.ID,
			&item.FeeScheduleID,
			&item.Name,
			&item.Priority,
			&item.StructureType,
			&item.StructureData,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan fee schedule item: %w", err)
		}

		grouped[item.FeeScheduleID] = append(grouped[item.FeeScheduleID], item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return grouped, nil
}

var _ matchingRepos.FeeScheduleRepository = (*Repository)(nil)
