// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package export_job

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// List retrieves export jobs for the tenant in context with optional status filter.
// Tenant is extracted from context using auth.GetTenantID(ctx).
// cursor is the ID of the last item from the previous page (nil for first page).
// Results are ordered by created_at DESC and cursor-based pagination uses (created_at, id) keyset.
func (repo *Repository) List(
	ctx context.Context,
	status *string,
	cursor *libHTTP.TimestampCursor,
	limit int,
) ([]*entities.ExportJob, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.list")

	defer span.End()

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		wrappedErr := fmt.Errorf("parse tenant ID from context: %w", err)
		libOpentelemetry.HandleSpanError(span, "invalid tenant ID in context", wrappedErr)

		libLog.SafeError(logger, ctx, "invalid tenant ID in context", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	var pagination libHTTP.CursorPagination

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	jobs, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ExportJob, error) {
			queryLimit := limit + 1
			builder := squirrel.
				Select(exportJobColumns()...).
				From(exportJobsTable).
				Where(squirrel.Eq{"tenant_id": tenantID}).
				OrderBy("created_at DESC", "id DESC").
				Limit(safeLimit(queryLimit)).
				PlaceholderFormat(squirrel.Dollar)

			if status != nil {
				builder = builder.Where(squirrel.Eq{"status": *status})
			}

			// Apply cursor-based pagination using keyset (created_at, id)
			if cursor != nil {
				builder = builder.Where(
					squirrel.Or{
						squirrel.Lt{"created_at": cursor.Timestamp},
						squirrel.And{
							squirrel.Eq{"created_at": cursor.Timestamp},
							squirrel.Lt{"id": cursor.ID},
						},
					},
				)
			}

			query, args, err := builder.ToSql()
			if err != nil {
				return nil, fmt.Errorf("build select query: %w", err)
			}

			records, err := scanExportJobs(tx.QueryContext(ctx, query, args...))
			if err != nil {
				return nil, err
			}

			records, nextCursor, nextCursorErr := trimExportJobsAndEncodeNextCursor(
				records,
				limit,
				libHTTP.EncodeTimestampCursor,
			)
			if nextCursorErr != nil {
				return nil, nextCursorErr
			}

			pagination.Next = nextCursor

			return records, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list export jobs: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list export jobs", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list export jobs", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return jobs, pagination, nil
}

// ListByContext retrieves export jobs for a specific context using forward-only
// cursor-based pagination. Results are ordered by created_at DESC with a stable
// (created_at, id) keyset cursor — same ordering as List — so clients can iterate
// pages without gaps or duplicates even if jobs share a created_at timestamp.
func (repo *Repository) ListByContext(
	ctx context.Context,
	contextID uuid.UUID,
	cursor *libHTTP.TimestampCursor,
	limit int,
) ([]*entities.ExportJob, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.list_by_context")

	defer span.End()

	var pagination libHTTP.CursorPagination

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	jobs, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ExportJob, error) {
			// Fetch limit+1 to detect if more pages exist without an extra COUNT query.
			queryLimit := limit + 1
			builder := squirrel.
				Select(exportJobColumns()...).
				From(exportJobsTable).
				Where(squirrel.Eq{"context_id": contextID}).
				OrderBy("created_at DESC", "id DESC").
				Limit(safeLimit(queryLimit)).
				PlaceholderFormat(squirrel.Dollar)

			// Apply cursor-based pagination using keyset (created_at, id).
			if cursor != nil {
				builder = builder.Where(
					squirrel.Or{
						squirrel.Lt{"created_at": cursor.Timestamp},
						squirrel.And{
							squirrel.Eq{"created_at": cursor.Timestamp},
							squirrel.Lt{"id": cursor.ID},
						},
					},
				)
			}

			query, args, buildErr := builder.ToSql()
			if buildErr != nil {
				return nil, fmt.Errorf("build select query: %w", buildErr)
			}

			records, scanErr := scanExportJobs(tx.QueryContext(ctx, query, args...))
			if scanErr != nil {
				return nil, scanErr
			}

			records, nextCursor, cursorErr := trimExportJobsAndEncodeNextCursor(
				records,
				limit,
				libHTTP.EncodeTimestampCursor,
			)
			if cursorErr != nil {
				return nil, cursorErr
			}

			pagination.Next = nextCursor

			return records, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list export jobs by context: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list export jobs", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list export jobs", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return jobs, pagination, nil
}

// ListExpired retrieves jobs that have passed their expiration time.
func (repo *Repository) ListExpired(
	ctx context.Context,
	limit int,
) ([]*entities.ExportJob, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.list_expired")

	defer span.End()

	jobs, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ExportJob, error) {
			query, args, err := squirrel.
				Select(exportJobColumns()...).
				From(exportJobsTable).
				Where(squirrel.Eq{"status": entities.ExportJobStatusSucceeded}).
				Where(squirrel.Lt{"expires_at": time.Now().UTC()}).
				OrderBy("expires_at ASC").
				Limit(safeLimit(limit)).
				PlaceholderFormat(squirrel.Dollar).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("build select query: %w", err)
			}

			return scanExportJobs(tx.QueryContext(ctx, query, args...))
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list expired export jobs: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list expired jobs", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list expired export jobs", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return jobs, nil
}

// ListTenants returns tenant IDs based on database schemas so background workers
// can fan out work across tenant-scoped data.
func (repo *Repository) ListTenants(ctx context.Context) ([]string, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.export_job.list_tenants")
	defer span.End()

	result, err := pgcommon.WithTenantRead(
		ctx,
		repo.provider,
		func(conn *sql.Conn) ([]string, error) {
			rows, err := conn.QueryContext(ctx, "SELECT nspname FROM pg_namespace WHERE nspname ~* $1", uuidSchemaRegex)
			if err != nil {
				return nil, fmt.Errorf("query tenant schemas: %w", err)
			}
			defer rows.Close()

			var tenants []string

			for rows.Next() {
				var tenant string
				if scanErr := rows.Scan(&tenant); scanErr != nil {
					return nil, fmt.Errorf("scan tenant schema: %w", scanErr)
				}

				tenants = append(tenants, tenant)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterate tenant schemas: %w", err)
			}

			defaultTenantID := auth.GetDefaultTenantID()
			if defaultTenantID != "" && !slices.Contains(tenants, defaultTenantID) {
				tenants = append(tenants, defaultTenantID)
			}

			return tenants, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list export job tenants: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list export job tenants", wrappedErr)
		libLog.SafeError(logger, ctx, "failed to list export job tenants", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}
