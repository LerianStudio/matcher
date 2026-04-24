// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package extraction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// FindByID retrieves an ExtractionRequest by its internal ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_extraction_request_by_id")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (*entities.ExtractionRequest, error) {
		row := tx.QueryRowContext(
			ctx,
			"SELECT "+allColumns+" FROM "+tableName+" WHERE id = $1",
			id.String(),
		)

		return scanExtraction(row)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, repositories.ErrExtractionNotFound
		}

		wrappedErr := fmt.Errorf("find extraction request by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find extraction request by id", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to find extraction request by id")

		return nil, wrappedErr
	}

	return result, nil
}

// FindEligibleForBridge returns up to limit COMPLETE extractions with
// ingestion_job_id IS NULL, oldest first. Ordering by updated_at keeps the
// backlog drain fair across tenants and avoids starving long-idle rows.
func (repo *Repository) FindEligibleForBridge(
	ctx context.Context,
	limit int,
) ([]*entities.ExtractionRequest, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if limit <= 0 {
		return nil, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.find_eligible_for_bridge")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) ([]*entities.ExtractionRequest, error) {
		// T-005 P2: exclude rows that have already been terminally failed
		// by the bridge worker. Without this filter, the worker would
		// re-pick failed rows on every cycle and hit the same terminal
		// error forever (livelock).
		rows, queryErr := tx.QueryContext(ctx,
			`SELECT `+allColumns+` FROM `+tableName+`
			WHERE status = $1
			  AND ingestion_job_id IS NULL
			  AND bridge_last_error IS NULL
			ORDER BY updated_at ASC
			LIMIT $2`,
			string(vo.ExtractionStatusComplete),
			limit,
		)
		if queryErr != nil {
			return nil, fmt.Errorf("query eligible extractions: %w", queryErr)
		}
		defer rows.Close()

		extractions := make([]*entities.ExtractionRequest, 0, limit)

		for rows.Next() {
			extraction, scanErr := scanExtraction(rows)
			if scanErr != nil {
				return nil, fmt.Errorf("scan eligible extraction: %w", scanErr)
			}

			extractions = append(extractions, extraction)
		}

		if iterErr := rows.Err(); iterErr != nil {
			return nil, fmt.Errorf("iterate eligible extractions: %w", iterErr)
		}

		return extractions, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("find eligible extractions: %w", err)
		libOpentelemetry.HandleSpanError(span, "find eligible extractions failed", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "find eligible extractions failed")

		return nil, wrappedErr
	}

	return result, nil
}

// FindBridgeRetentionCandidates returns extractions whose custody object
// is potentially orphaned in object storage and needs retention sweeping.
// See ExtractionRepository.FindBridgeRetentionCandidates for the full
// contract.
//
// SQL semantics:
//   - TERMINAL bucket: bridge_last_error IS NOT NULL — happy-path
//     cleanupCustody never ran for these.
//   - LATE-LINKED bucket: ingestion_job_id IS NOT NULL AND updated_at <
//     now() - gracePeriod — happy-path cleanup may have failed; sweep
//     waits gracePeriod to avoid racing the orchestrator.
//
// The two buckets are unioned via OR. Both share an `updated_at ASC`
// ordering so older orphans drain first. Rows that are still actively
// being bridged (COMPLETE + unlinked + no terminal error) are explicitly
// excluded — those belong to the bridge worker, not the retention sweep.
func (repo *Repository) FindBridgeRetentionCandidates(
	ctx context.Context,
	gracePeriod time.Duration,
	limit int,
) ([]*entities.ExtractionRequest, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if limit <= 0 {
		return nil, nil
	}

	if gracePeriod < 0 {
		gracePeriod = 0
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.find_bridge_retention_candidates")
	defer span.End()

	// gracePeriodSeconds is computed in Go (rather than using PostgreSQL
	// `INTERVAL`) so the query plan is parameter-friendly and the unit
	// math stays in one place.
	gracePeriodSeconds := int64(gracePeriod.Seconds())

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) ([]*entities.ExtractionRequest, error) {
		// custody_deleted_at IS NULL is the convergence guard (migration 000027):
		// once a row has been swept (or cleaned up on the happy path), it drops
		// out of both buckets so the sweep converges to idle instead of
		// re-scanning the same rows forever.
		rows, queryErr := tx.QueryContext(ctx,
			`SELECT `+allColumns+` FROM `+tableName+`
			WHERE custody_deleted_at IS NULL
			  AND (
			        (bridge_last_error IS NOT NULL)
			     OR (
			          ingestion_job_id IS NOT NULL
			          AND updated_at < (NOW() - ($1 || ' seconds')::INTERVAL)
			        )
			      )
			ORDER BY updated_at ASC
			LIMIT $2`,
			gracePeriodSeconds,
			limit,
		)
		if queryErr != nil {
			return nil, fmt.Errorf("query bridge retention candidates: %w", queryErr)
		}
		defer rows.Close()

		extractions := make([]*entities.ExtractionRequest, 0, limit)

		for rows.Next() {
			extraction, scanErr := scanExtraction(rows)
			if scanErr != nil {
				return nil, fmt.Errorf("scan bridge retention candidate: %w", scanErr)
			}

			extractions = append(extractions, extraction)
		}

		if iterErr := rows.Err(); iterErr != nil {
			return nil, fmt.Errorf("iterate bridge retention candidates: %w", iterErr)
		}

		return extractions, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("find bridge retention candidates: %w", err)
		libOpentelemetry.HandleSpanError(span, "find bridge retention candidates failed", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "find bridge retention candidates failed")

		return nil, wrappedErr
	}

	return result, nil
}
