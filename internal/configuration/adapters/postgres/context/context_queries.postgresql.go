// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package context

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// FindByID retrieves a reconciliation context by tenant and context ID.
func (repo *Repository) FindByID(
	ctx stdctx.Context,
	id uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_reconciliation_context_by_id")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (*entities.ReconciliationContext, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+contextColumns+" FROM reconciliation_contexts WHERE tenant_id = $1 AND id = $2",
				tenantID,
				id.String(),
			)

			return scanContext(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find reconciliation context by id", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find reconciliation context by id")
		}

		return nil, fmt.Errorf("find reconciliation context by id: %w", err)
	}

	return result, nil
}

// FindByIDWithTx retrieves a reconciliation context by tenant and context ID using the provided transaction.
func (repo *Repository) FindByIDWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	id uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	tenantID := auth.GetTenantID(ctx)
	row := tx.QueryRowContext(
		ctx,
		"SELECT "+contextColumns+" FROM reconciliation_contexts WHERE tenant_id = $1 AND id = $2",
		tenantID,
		id.String(),
	)

	result, err := scanContext(row)
	if err != nil {
		return nil, fmt.Errorf("find reconciliation context by id with tx: %w", err)
	}

	return result, nil
}

// FindByName retrieves a reconciliation context by tenant and name.
func (repo *Repository) FindByName(
	ctx stdctx.Context,
	name string,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_reconciliation_context_by_name")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (*entities.ReconciliationContext, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+contextColumns+" FROM reconciliation_contexts WHERE tenant_id = $1 AND name = $2",
				tenantID,
				name,
			)

			return scanContext(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		libOpentelemetry.HandleSpanError(
			span,
			"failed to find reconciliation context by name",
			err,
		)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find reconciliation context by name")

		return nil, fmt.Errorf("find reconciliation context by name: %w", err)
	}

	return result, nil
}

// FindAll retrieves all reconciliation contexts for a tenant with optional filters using cursor-based pagination.
func (repo *Repository) FindAll(
	ctx stdctx.Context,
	cursor string,
	limit int,
	contextType *shared.ContextType,
	status *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_all_reconciliation_contexts")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, err := decodeCursorParam(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (contexts []*entities.ReconciliationContext, err error) {
			findAll := buildContextQuery(tenantID, contextType, status)

			orderDirection := libHTTP.ValidateSortDirection("ASC")

			operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(orderDirection, decodedCursor.Direction)
			if dirErr != nil {
				return nil, fmt.Errorf("cursor direction rules: %w", dirErr)
			}

			if decodedCursor.ID != "" {
				findAll = findAll.Where(squirrel.Expr("id "+operator+" ?", decodedCursor.ID))
			}

			findAll = findAll.
				OrderBy("id " + effectiveOrder).
				Limit(pgcommon.SafeIntToUint64(limit) + 1)

			contexts, err = executeContextQuery(ctx, tx, findAll, limit)
			if err != nil {
				return nil, err
			}

			contexts, pagination, err = processPaginatedContexts(
				contexts, cursor, decodedCursor, limit,
			)
			if err != nil {
				return nil, err
			}

			return contexts, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation contexts", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list reconciliation contexts")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("find all reconciliation contexts: %w", err)
	}

	return result, pagination, nil
}
