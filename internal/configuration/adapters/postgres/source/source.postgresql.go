package source

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const sourceColumns = "id, context_id, name, type, config, fee_schedule_id, created_at, updated_at"

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

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create reconciliation source", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create reconciliation source")

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

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTxOrExisting(
		ctx,
		connection,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create reconciliation source", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create reconciliation source")

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
		`INSERT INTO reconciliation_sources (id, context_id, name, type, config, fee_schedule_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		model.ID,
		model.ContextID,
		model.Name,
		model.Type,
		model.Config,
		model.FeeScheduleID,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// FindByID retrieves a reconciliation source by context and source ID.
func (repo *Repository) FindByID(
	ctx stdctx.Context,
	contextID, id uuid.UUID,
) (*entities.ReconciliationSource, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.find_by_id")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (*entities.ReconciliationSource, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT "+sourceColumns+" FROM reconciliation_sources WHERE context_id = $1 AND id = $2",
				contextID.String(),
				id.String(),
			)

			return scanSource(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(
				span,
				"failed to find reconciliation source by id",
				err,
			)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find reconciliation source by id")
		}

		return nil, fmt.Errorf("failed to find reconciliation source by id: %w", err)
	}

	return result, nil
}

// FindByContextID retrieves all reconciliation sources for a context using cursor-based pagination.
func (repo *Repository) FindByContextID(
	ctx stdctx.Context,
	contextID uuid.UUID,
	cursor string,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.list")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("get postgres connection: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	baseQuery := squirrel.Select(strings.Split(sourceColumns, ", ")...).
		From("reconciliation_sources").
		Where(squirrel.Eq{"context_id": contextID.String()}).
		PlaceholderFormat(squirrel.Dollar)

	sources, pagination, err := executeSourceQuery(ctx, connection, baseQuery, cursor, decodedCursor, limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation sources", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list reconciliation sources")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find reconciliation sources by context: %w", err)
	}

	return sources, pagination, nil
}

// FindByContextIDAndType retrieves reconciliation sources for a context filtered by type using cursor-based pagination.
func (repo *Repository) FindByContextIDAndType(
	ctx stdctx.Context,
	contextID uuid.UUID,
	sourceType value_objects.SourceType,
	cursor string,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.list_by_type")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("get postgres connection: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	baseQuery := squirrel.Select(strings.Split(sourceColumns, ", ")...).
		From("reconciliation_sources").
		Where(squirrel.Eq{"context_id": contextID.String()}).
		Where(squirrel.Eq{"type": sourceType.String()}).
		PlaceholderFormat(squirrel.Dollar)

	sources, pagination, err := executeSourceQuery(ctx, connection, baseQuery, cursor, decodedCursor, limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation sources", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list reconciliation sources")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find reconciliation sources by context and type: %w", err)
	}

	return sources, pagination, nil
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

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update reconciliation source", err)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update reconciliation source")
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

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTxOrExisting(
		ctx,
		connection,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationSource, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update reconciliation source", err)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update reconciliation source")
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
		`UPDATE reconciliation_sources SET name = $1, type = $2, config = $3, fee_schedule_id = $4, updated_at = $5
		WHERE context_id = $6 AND id = $7`,
		model.Name,
		model.Type,
		model.Config,
		model.FeeScheduleID,
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

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return fmt.Errorf("get postgres connection: %w", err)
	}

	_, err = common.WithTenantTx(ctx, connection, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, contextID, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete reconciliation source", err)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to delete reconciliation source")
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

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return fmt.Errorf("get postgres connection: %w", err)
	}

	_, err = common.WithTenantTxOrExisting(
		ctx,
		connection,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, contextID, id)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete reconciliation source", err)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to delete reconciliation source")
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

// GetContextIDBySourceID retrieves the context_id for a given source ID.
// This is used as a fallback path when the ingestion job lookup fails during
// exception resolution context lookup (Transaction.SourceID -> context_id).
func (repo *Repository) GetContextIDBySourceID(
	ctx stdctx.Context,
	sourceID uuid.UUID,
) (uuid.UUID, error) {
	if repo == nil || repo.provider == nil {
		return uuid.Nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.find_context_id_by_source_id")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return uuid.Nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (uuid.UUID, error) {
			var contextIDStr string

			row := tx.QueryRowContext(
				ctx,
				"SELECT context_id FROM reconciliation_sources WHERE id = $1",
				sourceID.String(),
			)

			if err := row.Scan(&contextIDStr); err != nil {
				return uuid.Nil, err
			}

			parsed, err := uuid.Parse(contextIDStr)
			if err != nil {
				return uuid.Nil, fmt.Errorf("parse context id: %w", err)
			}

			return parsed, nil
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(
				span,
				"failed to find context id by source id",
				err,
			)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find context id by source id")
		}

		return uuid.Nil, fmt.Errorf("find context id by source id: %w", err)
	}

	return result, nil
}

// parseCursor decodes a cursor string into a Cursor struct for pagination.
// Returns a default cursor pointing forward if the input is empty.
func parseCursor(cursor string) (libHTTP.Cursor, error) {
	if cursor == "" {
		return libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}, nil
	}

	parsedCursor, err := libHTTP.DecodeCursor(cursor)
	if err != nil {
		return libHTTP.Cursor{}, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	return parsedCursor, nil
}

// executeSourceQuery executes a paginated source query within a tenant transaction.
func executeSourceQuery(
	ctx stdctx.Context,
	connection *libPostgres.Client,
	baseQuery squirrel.SelectBuilder,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) ([]*entities.ReconciliationSource, error) {
			return fetchPaginatedSources(ctx, tx, baseQuery, cursor, decodedCursor, limit, &pagination)
		},
	)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("execute tenant tx for source query: %w", err)
	}

	return result, pagination, nil
}

// fetchPaginatedSources applies pagination and fetches sources from the database.
func fetchPaginatedSources(
	ctx stdctx.Context,
	tx *sql.Tx,
	baseQuery squirrel.SelectBuilder,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
	pagination *libHTTP.CursorPagination,
) (sources []*entities.ReconciliationSource, err error) {
	paginatedQuery, err := buildPaginatedSourceQuery(baseQuery, decodedCursor, limit)
	if err != nil {
		return nil, err
	}

	sources, err = executeSourceRows(ctx, tx, paginatedQuery, limit)
	if err != nil {
		return nil, err
	}

	return applySourcePagination(sources, cursor, decodedCursor, limit, pagination)
}

// buildPaginatedSourceQuery applies cursor direction and limit to the base query.
func buildPaginatedSourceQuery(
	baseQuery squirrel.SelectBuilder,
	decodedCursor libHTTP.Cursor,
	limit int,
) (squirrel.SelectBuilder, error) {
	orderDirection := libHTTP.ValidateSortDirection("ASC")

	operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(orderDirection, decodedCursor.Direction)
	if dirErr != nil {
		return baseQuery, fmt.Errorf("cursor direction rules: %w", dirErr)
	}

	if decodedCursor.ID != "" {
		baseQuery = baseQuery.Where(squirrel.Expr("id "+operator+" ?", decodedCursor.ID))
	}

	return baseQuery.
		OrderBy("id " + effectiveOrder).
		Limit(sharedpg.SafeIntToUint64(limit) + 1), nil
}

// executeSourceRows runs the query and scans all source rows.
func executeSourceRows(
	ctx stdctx.Context,
	tx *sql.Tx,
	paginatedQuery squirrel.SelectBuilder,
	limit int,
) ([]*entities.ReconciliationSource, error) {
	query, args, err := paginatedQuery.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build list sources query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	sources := make([]*entities.ReconciliationSource, 0, limit)

	for rows.Next() {
		sourceEntity, scanErr := scanSource(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		sources = append(sources, sourceEntity)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return sources, nil
}

// applySourcePagination processes results to calculate cursor pagination.
func applySourcePagination(
	sources []*entities.ReconciliationSource,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
	pagination *libHTTP.CursorPagination,
) ([]*entities.ReconciliationSource, error) {
	hasPagination := len(sources) > limit
	isFirstPage := cursor == "" || (!hasPagination && decodedCursor.Direction == libHTTP.CursorDirectionPrev)

	sources = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		decodedCursor.Direction,
		sources,
		limit,
	)

	if len(sources) > 0 {
		page, calcErr := libHTTP.CalculateCursor(
			isFirstPage,
			hasPagination,
			decodedCursor.Direction,
			sources[0].ID.String(),
			sources[len(sources)-1].ID.String(),
		)
		if calcErr != nil {
			return nil, fmt.Errorf("calculate cursor: %w", calcErr)
		}

		*pagination = page
	}

	return sources, nil
}

func scanSource(
	scanner interface{ Scan(dest ...any) error },
) (*entities.ReconciliationSource, error) {
	var model SourcePostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.Name,
		&model.Type,
		&model.Config,
		&model.FeeScheduleID,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}
