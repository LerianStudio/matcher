// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package exception

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
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// FindByID retrieves an exception by its ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*entities.Exception, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.find_by_id")

	defer span.End()

	exception, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.Exception, error) {
			row := qe.QueryRowContext(ctx, `
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`, id.String())

			return scanException(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, entities.ErrExceptionNotFound
		}

		wrappedErr := fmt.Errorf("failed to find exception: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find exception", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to find exception", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return exception, nil
}

// FindByIDs retrieves all exceptions whose ids appear in the provided slice
// using a single `WHERE id IN (...)` query against the tenant replica.
// Ids not present in the store are silently omitted from the result: the
// caller reconciles the returned set against the requested set. An empty
// or nil input returns an empty slice and no error.
//
// The IN-list form (rather than ANY($1::uuid[])) mirrors the ingestion
// transaction adapter's FindByContextAndIDs and sidesteps pgx text-mode
// ARRAY encoding quirks without pgxuuid registered -- plus it keeps
// sqlmock assertions straightforward because each id is a scalar arg.
func (repo *Repository) FindByIDs(
	ctx context.Context,
	ids []uuid.UUID,
) ([]*entities.Exception, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if len(ids) == 0 {
		return []*entities.Exception{}, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.find_by_ids")

	defer span.End()

	// Convert []uuid.UUID to []string so squirrel emits IN ($1,...,$N)
	// with scalar text args, avoiding driver-specific array handling.
	idStrs := make([]string, 0, len(ids))
	for _, id := range ids {
		idStrs = append(idStrs, id.String())
	}

	query, args, buildErr := squirrel.Select(strings.Split(columns, ", ")...).
		From("exceptions").
		Where(squirrel.Eq{"id": idStrs}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if buildErr != nil {
		return nil, fmt.Errorf("build find-by-ids query: %w", buildErr)
	}

	exceptions, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.Exception, error) {
			rows, queryErr := qe.QueryContext(ctx, query, args...)
			if queryErr != nil {
				return nil, fmt.Errorf("query exceptions by ids: %w", queryErr)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil {
					logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close rows: %v", closeErr))
				}
			}()

			return scanAllRows(rows, len(ids))
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to find exceptions by ids: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find exceptions by ids", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to find exceptions by ids", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return exceptions, nil
}

// ExistsForTenant checks if an exception with the given ID exists in the current tenant's schema.
// This method uses tenant-scoped read queries for schema isolation.
func (repo *Repository) ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.exists_for_tenant")

	defer span.End()

	exists, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (bool, error) {
			var found bool

			err := qe.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)`, id.String()).
				Scan(&found)
			if err != nil {
				return false, fmt.Errorf("check exception existence: %w", err)
			}

			return found, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to check exception existence: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to check exception existence", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to check exception existence", wrappedErr, runtime.IsProductionMode())

		return false, wrappedErr
	}

	return exists, nil
}
