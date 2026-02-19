package schedule

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const scheduleColumns = "id, context_id, cron_expression, enabled, last_run_at, next_run_at, created_at, updated_at"

// Repository provides PostgreSQL operations for reconciliation schedules.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new schedule repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new reconciliation schedule into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrScheduleEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_reconciliation_schedule")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationSchedule, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create schedule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create schedule", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create schedule")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new reconciliation schedule using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrScheduleEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_reconciliation_schedule_with_tx")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationSchedule, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create schedule with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create schedule", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create schedule")

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual schedule creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	model, err := NewSchedulePostgreSQLModel(entity)
	if err != nil {
		return nil, fmt.Errorf("create schedule model: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO reconciliation_schedules (id, context_id, cron_expression, enabled, last_run_at, next_run_at, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		model.ID,
		model.ContextID,
		model.CronExpression,
		model.Enabled,
		model.LastRunAt,
		model.NextRunAt,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert schedule: %w", err)
	}

	return model.ToEntity()
}

// FindByID retrieves a schedule by its ID.
func (repo *Repository) FindByID(
	ctx stdctx.Context,
	id uuid.UUID,
) (*entities.ReconciliationSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_schedule_by_id")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationSchedule, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT "+scheduleColumns+" FROM reconciliation_schedules WHERE id = $1",
				id.String(),
			)

			return scanSchedule(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find schedule", err)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find schedule by id")
		}

		return nil, fmt.Errorf("find schedule by id: %w", err)
	}

	return result, nil
}

// FindByContextID retrieves all schedules for a context.
func (repo *Repository) FindByContextID(
	ctx stdctx.Context,
	contextID uuid.UUID,
) ([]*entities.ReconciliationSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_schedules_by_context")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ReconciliationSchedule, error) {
			rows, err := tx.QueryContext(
				ctx,
				"SELECT "+scheduleColumns+" FROM reconciliation_schedules WHERE context_id = $1 ORDER BY created_at",
				contextID.String(),
			)
			if err != nil {
				return nil, err
			}

			defer rows.Close()

			var schedules []*entities.ReconciliationSchedule

			for rows.Next() {
				s, scanErr := scanSchedule(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				schedules = append(schedules, s)
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return schedules, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list schedules", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list schedules by context")

		return nil, fmt.Errorf("find schedules by context: %w", err)
	}

	return result, nil
}

// FindDueSchedules retrieves all enabled schedules whose next_run_at <= now.
//
// This query JOINs with reconciliation_contexts to resolve the tenant_id for each schedule,
// so the scheduler worker can pass the correct tenant when triggering match runs.
//
// Tenant isolation note: this query runs within WithTenantTxProvider, which applies the
// tenant schema via auth.ApplyTenantSchema. In schema-per-tenant deployments, the
// search_path ensures only the current tenant's schedules are returned. For background
// workers without explicit tenant context, the default tenant schema is used.
func (repo *Repository) FindDueSchedules(
	ctx stdctx.Context,
	now time.Time,
) ([]*entities.ReconciliationSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_due_schedules")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ReconciliationSchedule, error) {
			rows, err := tx.QueryContext(
				ctx,
				`SELECT s.id, s.context_id, s.cron_expression, s.enabled,
						s.last_run_at, s.next_run_at, s.created_at, s.updated_at,
						c.tenant_id
				 FROM reconciliation_schedules s
				 JOIN reconciliation_contexts c ON c.id = s.context_id
				 WHERE s.enabled = TRUE AND s.next_run_at <= $1
				 ORDER BY s.next_run_at`,
				now,
			)
			if err != nil {
				return nil, err
			}

			defer rows.Close()

			var schedules []*entities.ReconciliationSchedule

			for rows.Next() {
				s, scanErr := scanDueSchedule(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				schedules = append(schedules, s)
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return schedules, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find due schedules", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find due schedules")

		return nil, fmt.Errorf("find due schedules: %w", err)
	}

	return result, nil
}

// Update modifies an existing schedule.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrScheduleEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_schedule")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationSchedule, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("update schedule: %w", err)
			libOpentelemetry.HandleSpanError(span, "failed to update schedule", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update schedule")

			return nil, wrappedErr
		}

		return nil, fmt.Errorf("update schedule: %w", err)
	}

	return result, nil
}

// UpdateWithTx modifies an existing schedule using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrScheduleEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_schedule_with_tx")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationSchedule, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("update schedule with tx: %w", err)
			libOpentelemetry.HandleSpanError(span, "failed to update schedule", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update schedule")

			return nil, wrappedErr
		}

		return nil, fmt.Errorf("update schedule with tx: %w", err)
	}

	return result, nil
}

// executeUpdate performs the actual schedule update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	model, err := NewSchedulePostgreSQLModel(entity)
	if err != nil {
		return nil, fmt.Errorf("create schedule model: %w", err)
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE reconciliation_schedules SET cron_expression = $1, enabled = $2, last_run_at = $3, next_run_at = $4, updated_at = $5
				WHERE id = $6`,
		model.CronExpression,
		model.Enabled,
		model.LastRunAt,
		model.NextRunAt,
		model.UpdatedAt,
		model.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("update schedule: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return nil, sql.ErrNoRows
	}

	return model.ToEntity()
}

// Delete removes a schedule by ID.
func (repo *Repository) Delete(ctx stdctx.Context, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_schedule")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("delete schedule: %w", err)
			libOpentelemetry.HandleSpanError(span, "failed to delete schedule", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to delete schedule")

			return wrappedErr
		}

		return fmt.Errorf("delete schedule: %w", err)
	}

	return nil
}

// DeleteWithTx removes a schedule by ID using the provided transaction.
// This enables atomic operations within the same transaction as other operations.
func (repo *Repository) DeleteWithTx(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_schedule_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, innerTx, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("delete schedule with tx: %w", err)
			libOpentelemetry.HandleSpanError(span, "failed to delete schedule", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to delete schedule")

			return wrappedErr
		}

		return fmt.Errorf("delete schedule with tx: %w", err)
	}

	return nil
}

// executeDelete performs the actual schedule deletion within a transaction.
func (repo *Repository) executeDelete(
	ctx stdctx.Context,
	tx *sql.Tx,
	id uuid.UUID,
) (bool, error) {
	res, err := tx.ExecContext(
		ctx,
		"DELETE FROM reconciliation_schedules WHERE id = $1",
		id.String(),
	)
	if err != nil {
		return false, fmt.Errorf("delete schedule: %w", err)
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

func scanSchedule(
	scanner interface{ Scan(dest ...any) error },
) (*entities.ReconciliationSchedule, error) {
	var model SchedulePostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.CronExpression,
		&model.Enabled,
		&model.LastRunAt,
		&model.NextRunAt,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// scanDueSchedule scans a row from the FindDueSchedules JOIN query, which includes
// the tenant_id column from reconciliation_contexts alongside the schedule columns.
func scanDueSchedule(
	scanner interface{ Scan(dest ...any) error },
) (*entities.ReconciliationSchedule, error) {
	var model SchedulePostgreSQLModel

	var tenantIDStr string

	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.CronExpression,
		&model.Enabled,
		&model.LastRunAt,
		&model.NextRunAt,
		&model.CreatedAt,
		&model.UpdatedAt,
		&tenantIDStr,
	); err != nil {
		return nil, err
	}

	entity, err := model.ToEntity()
	if err != nil {
		return nil, err
	}

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return nil, fmt.Errorf("parsing TenantID: %w", err)
	}

	entity.TenantID = tenantID

	return entity, nil
}
