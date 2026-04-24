// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package source

import (
	stdctx "context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
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

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	baseQuery := squirrel.Select(strings.Split(sourceColumns, ", ")...).
		From("reconciliation_sources").
		Where(squirrel.Eq{"context_id": contextID.String()}).
		PlaceholderFormat(squirrel.Dollar)

	sources, pagination, err := executeSourceQuery(ctx, repo.provider, baseQuery, cursor, decodedCursor, limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation sources", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list reconciliation sources")

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

	sources, pagination, err := executeSourceQuery(ctx, repo.provider, baseQuery, cursor, decodedCursor, limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation sources", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list reconciliation sources")

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

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list reconciliation sources with tx")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find reconciliation sources by context with tx: %w", err)
	}

	return sources, pagination, nil
}

// executeSourceQuery executes a paginated source query within a tenant transaction.
func executeSourceQuery(
	ctx stdctx.Context,
	provider ports.InfrastructureProvider,
	baseQuery squirrel.SelectBuilder,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantReadQuery(
		ctx,
		provider,
		func(qe common.QueryExecutor) ([]*entities.ReconciliationSource, error) {
			return fetchPaginatedSources(ctx, qe, baseQuery, cursor, decodedCursor, limit, &pagination)
		},
	)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("execute tenant read query for source query: %w", err)
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
	qe common.QueryExecutor,
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

	sources, err = executeSourceRows(ctx, qe, paginatedQuery, limit)
	if err != nil {
		return nil, err
	}

	return applySourcePagination(sources, cursor, decodedCursor, limit, pagination)
}
