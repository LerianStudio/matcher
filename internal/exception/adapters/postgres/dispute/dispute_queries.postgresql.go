// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dispute

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// FindByID retrieves a dispute by its ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.find_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*dispute.Dispute, error) {
			return repo.findByIDExec(ctx, qe, id)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDisputeNotFound
		}

		wrappedErr := fmt.Errorf("find dispute by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to find dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// FindByExceptionID retrieves a dispute by its exception ID.
func (repo *Repository) FindByExceptionID(
	ctx context.Context,
	exceptionID uuid.UUID,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.find_by_exception_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*dispute.Dispute, error) {
			row := qe.QueryRowContext(ctx, `
			SELECT id, exception_id, category, state, description,
			       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
			FROM disputes
			WHERE exception_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`, exceptionID.String())

			return scanDispute(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDisputeNotFound
		}

		wrappedErr := fmt.Errorf("find dispute by exception id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find dispute by exception", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to find dispute by exception", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// ExistsForTenant checks if a dispute with the given ID exists in the current tenant's schema.
// This method uses tenant-scoped read queries for schema isolation.
func (repo *Repository) ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.exists_for_tenant")

	defer span.End()

	exists, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (bool, error) {
			var found bool

			err := qe.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM disputes WHERE id = $1)`, id.String()).
				Scan(&found)
			if err != nil {
				return false, fmt.Errorf("check dispute existence: %w", err)
			}

			return found, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to check dispute existence: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to check dispute existence", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to check dispute existence", wrappedErr, runtime.IsProductionMode())

		return false, wrappedErr
	}

	return exists, nil
}
