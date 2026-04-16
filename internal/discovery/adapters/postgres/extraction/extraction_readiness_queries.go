// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package extraction

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Bridge readiness query bounds. The drilldown limit is clamped here so a
// caller cannot ask for an unbounded result set; the dashboard never needs
// more than a screenful per page.
const (
	maxBridgeCandidatesPerPage     = 500
	defaultBridgeCandidatesPerPage = 50
)

// CountBridgeReadiness aggregates extraction rows into the four-way readiness
// partition for the tenant resolved from ctx.
//
// The query runs as a single SELECT with FILTER clauses so PostgreSQL only
// scans extraction_requests once. The partial index
// idx_extraction_requests_eligible_for_bridge accelerates the COMPLETE +
// unlinked partitions; the FAILED bucket falls back to a sequential scan but
// stays bounded because terminal-state rows accumulate slowly.
//
// staleThreshold is evaluated against NOW() - created_at so a stuck row that
// was retried recently still counts as stale. Negative or zero values are
// clamped to a minimum (1s) at the call site to keep the partition meaningful.
func (repo *Repository) CountBridgeReadiness(
	ctx context.Context,
	staleThreshold time.Duration,
) (repositories.BridgeReadinessCounts, error) {
	if repo == nil || repo.provider == nil {
		return repositories.BridgeReadinessCounts{}, ErrRepoNotInitialized
	}

	thresholdSec := staleThreshold.Seconds()
	if thresholdSec <= 0 {
		thresholdSec = 1
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.count_bridge_readiness")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (repositories.BridgeReadinessCounts, error) {
		row := tx.QueryRowContext(ctx,
			`SELECT
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NOT NULL) AS ready_count,
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NULL
					AND EXTRACT(EPOCH FROM (NOW() - created_at)) <= $2) AS pending_count,
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NULL
					AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2) AS stale_count,
				COUNT(*) FILTER (WHERE status IN ($3, $4)) AS failed_count,
				COUNT(*) FILTER (WHERE status IN ($5, $6, $7)) AS in_flight_count
			FROM `+tableName,
			string(vo.ExtractionStatusComplete),
			thresholdSec,
			string(vo.ExtractionStatusFailed),
			string(vo.ExtractionStatusCancelled),
			string(vo.ExtractionStatusPending),
			string(vo.ExtractionStatusSubmitted),
			string(vo.ExtractionStatusExtracting),
		)

		var counts repositories.BridgeReadinessCounts
		if scanErr := row.Scan(
			&counts.Ready, &counts.Pending, &counts.Stale, &counts.Failed, &counts.InFlightCount,
		); scanErr != nil {
			return repositories.BridgeReadinessCounts{}, fmt.Errorf("scan readiness counts: %w", scanErr)
		}

		return counts, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("count bridge readiness: %w", err)
		libOpentelemetry.HandleSpanError(span, "count bridge readiness failed", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "count bridge readiness failed")

		return repositories.BridgeReadinessCounts{}, wrappedErr
	}

	return result, nil
}

// ListBridgeCandidates returns extractions in the requested readiness state,
// ordered by created_at ASC for stable cursor pagination. The (createdAfter,
// idAfter) pair is the keyset cursor: callers pass back the last row's
// CreatedAt/ID to fetch the next page.
//
// limit is clamped to [1, maxBridgeCandidatesPerPage]. Zero or negative values
// fall back to defaultBridgeCandidatesPerPage.
//
// The state argument MUST already be a validated BridgeReadinessState; the
// HTTP handler is responsible for parsing the user-supplied query string.
// Implementation accepts string to keep the repository interface small.
func (repo *Repository) ListBridgeCandidates(
	ctx context.Context,
	state string,
	staleThreshold time.Duration,
	createdAfter time.Time,
	idAfter uuid.UUID,
	limit int,
) ([]*entities.ExtractionRequest, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	parsedState, err := vo.ParseBridgeReadinessState(state)
	if err != nil {
		return nil, fmt.Errorf("invalid readiness state: %w", err)
	}

	thresholdSec := staleThreshold.Seconds()
	if thresholdSec <= 0 {
		thresholdSec = 1
	}

	pageSize := clampPageLimit(limit)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.list_bridge_candidates")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) ([]*entities.ExtractionRequest, error) {
		query, args := buildBridgeCandidateQuery(parsedState, thresholdSec, createdAfter, idAfter, pageSize)

		rows, queryErr := tx.QueryContext(ctx, query, args...)
		if queryErr != nil {
			return nil, fmt.Errorf("query bridge candidates: %w", queryErr)
		}
		defer rows.Close()

		extractions := make([]*entities.ExtractionRequest, 0, pageSize)

		for rows.Next() {
			extraction, scanErr := scanExtraction(rows)
			if scanErr != nil {
				return nil, fmt.Errorf("scan bridge candidate: %w", scanErr)
			}

			extractions = append(extractions, extraction)
		}

		if iterErr := rows.Err(); iterErr != nil {
			return nil, fmt.Errorf("iterate bridge candidates: %w", iterErr)
		}

		return extractions, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("list bridge candidates: %w", err)
		libOpentelemetry.HandleSpanError(span, "list bridge candidates failed", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "list bridge candidates failed")

		return nil, wrappedErr
	}

	return result, nil
}

// buildBridgeCandidateQuery composes the per-state SELECT with the keyset
// cursor predicate. Centralising the SQL keeps the per-state branches in one
// place so the next maintainer can audit them as a unit.
func buildBridgeCandidateQuery(
	state vo.BridgeReadinessState,
	thresholdSec float64,
	createdAfter time.Time,
	idAfter uuid.UUID,
	limit int,
) (string, []any) {
	const baseSelect = `SELECT ` + allColumns + ` FROM ` + tableName + ` WHERE `

	var (
		predicate string
		args      []any
	)

	switch state {
	case vo.BridgeReadinessReady:
		predicate = "status = $1 AND ingestion_job_id IS NOT NULL"
		args = []any{string(vo.ExtractionStatusComplete)}
	case vo.BridgeReadinessPending:
		predicate = "status = $1 AND ingestion_job_id IS NULL AND EXTRACT(EPOCH FROM (NOW() - created_at)) <= $2"
		args = []any{string(vo.ExtractionStatusComplete), thresholdSec}
	case vo.BridgeReadinessStale:
		predicate = "status = $1 AND ingestion_job_id IS NULL AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2"
		args = []any{string(vo.ExtractionStatusComplete), thresholdSec}
	case vo.BridgeReadinessFailed:
		predicate = "status IN ($1, $2)"
		args = []any{string(vo.ExtractionStatusFailed), string(vo.ExtractionStatusCancelled)}
	case vo.BridgeReadinessInFlight:
		predicate = "status IN ($1, $2, $3)"
		args = []any{
			string(vo.ExtractionStatusPending),
			string(vo.ExtractionStatusSubmitted),
			string(vo.ExtractionStatusExtracting),
		}
	}

	// Append keyset cursor when caller passed a non-zero anchor. The
	// (created_at, id) tuple is unique because id is a primary key.
	if !createdAfter.IsZero() {
		nextIdx := len(args) + 1
		predicate += fmt.Sprintf(" AND (created_at, id) > ($%d, $%d)", nextIdx, nextIdx+1)

		args = append(args, createdAfter, idAfter)
	}

	limitIdx := len(args) + 1
	query := baseSelect + predicate + fmt.Sprintf(" ORDER BY created_at ASC, id ASC LIMIT $%d", limitIdx)

	args = append(args, limit)

	return query, args
}

// clampPageLimit normalises caller-supplied page sizes into a safe range so
// no caller can request an unbounded scan.
func clampPageLimit(limit int) int {
	if limit <= 0 {
		return defaultBridgeCandidatesPerPage
	}

	if limit > maxBridgeCandidatesPerPage {
		return maxBridgeCandidatesPerPage
	}

	return limit
}
