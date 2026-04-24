// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package context

import (
	stdctx "context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const contextColumns = "id, tenant_id, name, type, interval, status, fee_tolerance_abs, fee_tolerance_pct, fee_normalization, auto_match_on_upload, created_at, updated_at"

// Repository provides PostgreSQL operations for reconciliation contexts.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new context repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Count returns the total number of reconciliation contexts for a tenant.
func (repo *Repository) Count(ctx stdctx.Context) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.count_reconciliation_contexts")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	result, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (int64, error) {
		row := tx.QueryRowContext(
			ctx,
			"SELECT COUNT(1) FROM reconciliation_contexts WHERE tenant_id = $1",
			tenantID,
		)

		var count int64

		if err := row.Scan(&count); err != nil {
			return 0, err
		}

		return count, nil
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to count reconciliation contexts", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to count reconciliation contexts")

		return 0, fmt.Errorf("count reconciliation contexts: %w", err)
	}

	return result, nil
}

func scanContext(
	scanner interface{ Scan(dest ...any) error },
) (*entities.ReconciliationContext, error) {
	var model ContextPostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.TenantID,
		&model.Name,
		&model.Type,
		&model.Interval,
		&model.Status,
		&model.FeeToleranceAbs,
		&model.FeeTolerancePct,
		&model.FeeNormalization,
		&model.AutoMatchOnUpload,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// decodeCursorParam decodes a cursor string into a Cursor struct.
// Returns a default cursor pointing forward if the cursor is empty.
func decodeCursorParam(cursor string) (libHTTP.Cursor, error) {
	if cursor == "" {
		return libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}, nil
	}

	parsedCursor, err := libHTTP.DecodeCursor(cursor)
	if err != nil {
		return libHTTP.Cursor{}, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	return parsedCursor, nil
}

// buildContextQuery creates the base query for listing contexts with optional filters.
func buildContextQuery(
	tenantID string,
	contextType *shared.ContextType,
	status *value_objects.ContextStatus,
) squirrel.SelectBuilder {
	query := squirrel.Select(strings.Split(contextColumns, ", ")...).
		From("reconciliation_contexts").
		Where(squirrel.Eq{"tenant_id": tenantID}).
		PlaceholderFormat(squirrel.Dollar)

	if contextType != nil {
		query = query.Where(squirrel.Eq{"type": contextType.String()})
	}

	if status != nil {
		query = query.Where(squirrel.Eq{"status": status.String()})
	}

	return query
}

// executeContextQuery executes the query and scans results into context entities.
func executeContextQuery(
	ctx stdctx.Context,
	tx *sql.Tx,
	queryBuilder squirrel.SelectBuilder,
	limit int,
) ([]*entities.ReconciliationContext, error) {
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build list contexts query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	contexts := make([]*entities.ReconciliationContext, 0, limit)

	for rows.Next() {
		contextEntity, scanErr := scanContext(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		contexts = append(contexts, contextEntity)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return contexts, nil
}

// processPaginatedContexts applies pagination logic to the context results.
func processPaginatedContexts(
	contexts []*entities.ReconciliationContext,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	hasPagination := len(contexts) > limit
	isFirstPage := cursor == "" || (!hasPagination && decodedCursor.Direction == libHTTP.CursorDirectionPrev)

	contexts = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		decodedCursor.Direction,
		contexts,
		limit,
	)

	var pagination libHTTP.CursorPagination

	if len(contexts) > 0 {
		page, err := libHTTP.CalculateCursor(
			isFirstPage,
			hasPagination,
			decodedCursor.Direction,
			contexts[0].ID.String(),
			contexts[len(contexts)-1].ID.String(),
		)
		if err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		pagination = page
	}

	return contexts, pagination, nil
}
