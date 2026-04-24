// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

func (repo *Repository) validateListByEntityParams(
	entityType string,
	entityID uuid.UUID,
	limit int,
) (string, error) {
	if repo == nil || repo.provider == nil {
		return "", ErrRepositoryNotInitialized
	}

	trimmedEntityType := strings.TrimSpace(entityType)
	if trimmedEntityType == "" {
		return "", entities.ErrEntityTypeRequired
	}

	if entityID == uuid.Nil {
		return "", entities.ErrEntityIDRequired
	}

	if limit <= 0 {
		return "", ErrLimitMustBePositive
	}

	return trimmedEntityType, nil
}

// GetByID retrieves a single audit log by its ID.
// Tenant is extracted from context via auth.GetTenantID(ctx).
func (repo *Repository) GetByID(ctx context.Context, id uuid.UUID) (*entities.AuditLog, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if id == uuid.Nil {
		return nil, ErrIDRequired
	}

	tenantID, err := getTenantIDFromContext(ctx)
	if err != nil {
		return nil, entities.ErrTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_audit_log_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.AuditLog, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+auditLogColumns+" FROM audit_logs WHERE id = $1 AND tenant_id = $2",
				id,
				tenantID,
			)

			return scanAuditLog(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAuditLogNotFound
		}

		wrappedErr := fmt.Errorf("get audit log by id transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get audit log by id", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get audit log by id", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// ListByEntity retrieves audit logs for a specific entity using cursor-based pagination.
// Uses timestamp+ID keyset pagination for correct ordering with (created_at DESC, id DESC).
// Tenant is extracted from context via auth.GetTenantID(ctx).
// Returns the logs, next cursor (empty if no more pages), and any error.
func (repo *Repository) ListByEntity(
	ctx context.Context,
	entityType string,
	entityID uuid.UUID,
	cursor *sharedhttp.TimestampCursor,
	limit int,
) ([]*entities.AuditLog, string, error) {
	trimmedEntityType, err := repo.validateListByEntityParams(entityType, entityID, limit)
	if err != nil {
		return nil, "", err
	}

	tenantID, err := getTenantIDFromContext(ctx)
	if err != nil {
		return nil, "", entities.ErrTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_audit_logs")

	defer span.End()

	type listResult struct {
		logs       []*entities.AuditLog
		nextCursor string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*listResult, error) {
			qb := squirrel.Select(strings.Split(auditLogColumns, ", ")...).
				From("audit_logs").
				Where(squirrel.Eq{"tenant_id": tenantID}).
				Where(squirrel.Eq{"entity_type": trimmedEntityType}).
				Where(squirrel.Eq{"entity_id": entityID}).
				OrderBy("created_at DESC", "id DESC").
				Limit(safeLimit(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			if cursor != nil {
				qb = qb.Where("(created_at, id) < (?, ?)", cursor.Timestamp, cursor.ID)
			}

			query, args, err := qb.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("querying audit logs: %w", err)
			}

			defer rows.Close()

			logs := make([]*entities.AuditLog, 0, limit)

			for rows.Next() {
				log, err := scanAuditLog(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning audit log: %w", err)
				}

				logs = append(logs, log)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating audit logs: %w", err)
			}

			logs, nextCursor, err := buildNextCursor(logs, limit)
			if err != nil {
				return nil, fmt.Errorf("build next cursor: %w", err)
			}

			return &listResult{logs: logs, nextCursor: nextCursor}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list audit logs by entity transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list audit logs by entity", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list audit logs", wrappedErr, runtime.IsProductionMode())

		return nil, "", wrappedErr
	}

	return result.logs, result.nextCursor, nil
}

func applyAuditLogFilter(
	qb squirrel.SelectBuilder,
	filter entities.AuditLogFilter,
	cursor *sharedhttp.TimestampCursor,
) squirrel.SelectBuilder {
	if filter.Actor != nil {
		qb = qb.Where(squirrel.Eq{"actor_id": *filter.Actor})
	}

	if filter.Action != nil {
		qb = qb.Where(squirrel.Eq{"action": *filter.Action})
	}

	if filter.EntityType != nil {
		qb = qb.Where(squirrel.Eq{"entity_type": *filter.EntityType})
	}

	if filter.DateFrom != nil {
		qb = qb.Where(squirrel.GtOrEq{"created_at": *filter.DateFrom})
	}

	if filter.DateTo != nil {
		qb = qb.Where(squirrel.LtOrEq{"created_at": *filter.DateTo})
	}

	if cursor != nil {
		qb = qb.Where("(created_at, id) < (?, ?)", cursor.Timestamp, cursor.ID)
	}

	return qb
}

// List retrieves audit logs with optional filters using cursor-based pagination.
// Uses timestamp+ID keyset pagination for correct ordering with (created_at DESC, id DESC).
// Tenant is extracted from context via auth.GetTenantID(ctx).
// Returns the logs, next cursor (empty if no more pages), and any error.
func (repo *Repository) List(
	ctx context.Context,
	filter entities.AuditLogFilter,
	cursor *sharedhttp.TimestampCursor,
	limit int,
) ([]*entities.AuditLog, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if limit <= 0 {
		return nil, "", ErrLimitMustBePositive
	}

	tenantID, err := getTenantIDFromContext(ctx)
	if err != nil {
		return nil, "", entities.ErrTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_audit_logs")

	defer span.End()

	type listResult struct {
		logs       []*entities.AuditLog
		nextCursor string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*listResult, error) {
			qb := squirrel.Select(strings.Split(auditLogColumns, ", ")...).
				From("audit_logs").
				Where(squirrel.Eq{"tenant_id": tenantID}).
				OrderBy("created_at DESC", "id DESC").
				Limit(safeLimit(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			qb = applyAuditLogFilter(qb, filter, cursor)

			query, args, err := qb.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("querying audit logs: %w", err)
			}

			defer rows.Close()

			logs := make([]*entities.AuditLog, 0, limit)

			for rows.Next() {
				log, err := scanAuditLog(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning audit log: %w", err)
				}

				logs = append(logs, log)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating audit logs: %w", err)
			}

			logs, nextCursor, err := buildNextCursor(logs, limit)
			if err != nil {
				return nil, fmt.Errorf("build next cursor: %w", err)
			}

			return &listResult{logs: logs, nextCursor: nextCursor}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list audit logs transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list audit logs", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list audit logs", wrappedErr, runtime.IsProductionMode())

		return nil, "", wrappedErr
	}

	return result.logs, result.nextCursor, nil
}
