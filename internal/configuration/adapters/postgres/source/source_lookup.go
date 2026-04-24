// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package source

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

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

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (*entities.ReconciliationSource, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+sourceColumns+" FROM reconciliation_sources WHERE context_id = $1 AND id = $2",
				contextID,
				id,
			)

			return scanSource(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find reconciliation source by id", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find reconciliation source by id")
		}

		return nil, fmt.Errorf("failed to find reconciliation source by id: %w", err)
	}

	return result, nil
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

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (uuid.UUID, error) {
			var contextID uuid.UUID

			row := qe.QueryRowContext(
				ctx,
				"SELECT context_id FROM reconciliation_sources WHERE id = $1",
				sourceID,
			)

			if err := row.Scan(&contextID); err != nil {
				return uuid.Nil, err
			}

			return contextID, nil
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find context id by source id", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find context id by source id")
		}

		return uuid.Nil, fmt.Errorf("find context id by source id: %w", err)
	}

	return result, nil
}
