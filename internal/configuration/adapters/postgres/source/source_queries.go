package source

import (
	stdctx "context"
	"database/sql"
	"fmt"
	"strings"

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
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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
	defer connection.Release()

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	baseQuery := squirrel.Select(strings.Split(sourceColumns, ", ")...).
		From("reconciliation_sources").
		Where(squirrel.Eq{"context_id": contextID.String()}).
		PlaceholderFormat(squirrel.Dollar)

	sources, pagination, err := executeSourceQuery(ctx, connection.Connection(), baseQuery, cursor, decodedCursor, limit)
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
	defer connection.Release()

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

	sources, pagination, err := executeSourceQuery(ctx, connection.Connection(), baseQuery, cursor, decodedCursor, limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation sources", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list reconciliation sources")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find reconciliation sources by context and type: %w", err)
	}

	return sources, pagination, nil
}

// FindByContextIDWithTx retrieves all reconciliation sources for a context using
// cursor-based pagination within an existing transaction. This enables consistent
// snapshot reads when the caller already holds a transaction (e.g. clone operations).
func (repo *Repository) FindByContextIDWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	cursor string,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, libHTTP.CursorPagination{}, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.source.list_with_tx")
	defer span.End()

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	baseQuery := squirrel.Select(strings.Split(sourceColumns, ", ")...).
		From("reconciliation_sources").
		Where(squirrel.Eq{"context_id": contextID.String()}).
		PlaceholderFormat(squirrel.Dollar)

	sources, pagination, err := executeSourceQueryWithTx(ctx, tx, baseQuery, cursor, decodedCursor, limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation sources with tx", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list reconciliation sources with tx")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find reconciliation sources by context with tx: %w", err)
	}

	return sources, pagination, nil
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

// executeSourceQueryWithTx executes a paginated source query using an existing transaction.
func executeSourceQueryWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	baseQuery squirrel.SelectBuilder,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	var pagination libHTTP.CursorPagination

	sources, err := fetchPaginatedSources(ctx, tx, baseQuery, cursor, decodedCursor, limit, &pagination)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("execute source query with tx: %w", err)
	}

	return sources, pagination, nil
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
