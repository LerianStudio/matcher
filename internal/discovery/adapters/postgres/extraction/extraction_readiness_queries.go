// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package extraction

import (
	"context"
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

// Bridge readiness query bounds. The drilldown limit is clamped here so a
// caller cannot ask for an unbounded result set; the dashboard never needs
// more than a screenful per page.
const (
	maxBridgeCandidatesPerPage     = 500
	defaultBridgeCandidatesPerPage = 50
)

// bridgeCandidateColumns is the narrow SELECT list for the drilldown query.
// Projects only the columns the dashboard's BridgeCandidateResponse DTO
// exposes (plus the scan-required status and bridge_last_error columns), so
// the hot path avoids the per-row json.Unmarshal cost of the `tables` and
// `filters` JSONB columns that allColumns carries. Order MUST match
// scanBridgeCandidateRow.
const bridgeCandidateColumns = "id, connection_id, ingestion_job_id, fetcher_job_id, status, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_failed_at"

// CountBridgeReadiness aggregates extraction rows into the five-way readiness
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

	result, err := pgcommon.WithTenantReadQuery(ctx, repo.provider, func(qx pgcommon.QueryExecutor) (repositories.BridgeReadinessCounts, error) {
		// T-005 integration: bridge_last_error participates in the
		// readiness partition. A row that is COMPLETE+unlinked but has
		// bridge_last_error set is "failed" (the bridge gave up), not
		// "pending" or "stale". The pending/stale buckets explicitly
		// require bridge_last_error IS NULL so they only show retryable
		// rows the worker is expected to drain.
		//
		// Defense-in-depth (C14): ready_count also excludes rows with
		// bridge_last_error IS NOT NULL. Today unreachable via the write
		// path (FindEligibleForBridge ignores terminally-failed rows), but
		// without this guard a linked+terminally-failed row would double-
		// count — landing in ready AND failed — and break the partition's
		// mutual-exclusion invariant.
		row := qx.QueryRowContext(ctx,
			`SELECT
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NOT NULL AND bridge_last_error IS NULL) AS ready_count,
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NULL
					AND bridge_last_error IS NULL
					AND EXTRACT(EPOCH FROM (NOW() - created_at)) <= $2) AS pending_count,
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NULL
					AND bridge_last_error IS NULL
					AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2) AS stale_count,
				COUNT(*) FILTER (WHERE status IN ($3, $4) OR bridge_last_error IS NOT NULL) AS failed_count,
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
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "count bridge readiness failed")

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

	result, err := pgcommon.WithTenantReadQuery(ctx, repo.provider, func(qx pgcommon.QueryExecutor) ([]*entities.ExtractionRequest, error) {
		query, args := buildBridgeCandidateQuery(parsedState, thresholdSec, createdAfter, idAfter, pageSize)

		rows, queryErr := qx.QueryContext(ctx, query, args...)
		if queryErr != nil {
			return nil, fmt.Errorf("query bridge candidates: %w", queryErr)
		}
		defer rows.Close()

		extractions := make([]*entities.ExtractionRequest, 0, pageSize)

		for rows.Next() {
			extraction, scanErr := scanBridgeCandidateRow(rows)
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
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "list bridge candidates failed")

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
	// Narrow projection (C11): the drilldown only surfaces
	// BridgeCandidateResponse fields, so the hot-path SELECT avoids the
	// JSONB `tables` and `filters` columns that scanExtraction triggers
	// json.Unmarshal on for every row. scanBridgeCandidateRow scans exactly
	// these columns in this order.
	const baseSelect = `SELECT ` + bridgeCandidateColumns + ` FROM ` + tableName + ` WHERE `

	var (
		predicate string
		args      []any
	)

	switch state {
	case vo.BridgeReadinessReady:
		// Defense-in-depth (C14): exclude terminally-failed rows so the
		// drilldown agrees with CountBridgeReadiness. A row with
		// bridge_last_error IS NOT NULL belongs to the failed bucket; if
		// it also happens to be linked it must still surface there only.
		predicate = "status = $1 AND ingestion_job_id IS NOT NULL AND bridge_last_error IS NULL"
		args = []any{string(vo.ExtractionStatusComplete)}
	case vo.BridgeReadinessPending:
		// Pending requires the row to still be retryable: bridge_last_error
		// IS NULL filters out rows the worker has terminally failed.
		predicate = "status = $1 AND ingestion_job_id IS NULL AND bridge_last_error IS NULL AND EXTRACT(EPOCH FROM (NOW() - created_at)) <= $2"
		args = []any{string(vo.ExtractionStatusComplete), thresholdSec}
	case vo.BridgeReadinessStale:
		// Same retryable filter for stale — terminal-failed rows belong in
		// the failed bucket, not stale.
		predicate = "status = $1 AND ingestion_job_id IS NULL AND bridge_last_error IS NULL AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2"
		args = []any{string(vo.ExtractionStatusComplete), thresholdSec}
	case vo.BridgeReadinessFailed:
		// T-005: failed bucket now includes bridge-failed rows in addition
		// to discovery-side FAILED/CANCELLED rows. Operators see both
		// failure classes in one drilldown.
		predicate = "(status IN ($1, $2) OR bridge_last_error IS NOT NULL)"
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

// scanBridgeCandidateRow scans a narrow bridge-candidate row into an
// ExtractionRequest populated with only the fields the dashboard drilldown
// surfaces (via BridgeCandidateResponse). Tables, Filters, StartDate,
// EndDate, ResultPath, ErrorMessage, BridgeLastErrorMessage, and
// CustodyDeletedAt are intentionally left zero — the drilldown endpoint does
// not expose them, and projecting the JSONB columns here would cost a
// json.Unmarshal per row on a hot dashboard path. Column order MUST match
// bridgeCandidateColumns.
func scanBridgeCandidateRow(scanner interface{ Scan(dest ...any) error }) (*entities.ExtractionRequest, error) {
	var model ExtractionModel
	if err := scanner.Scan(
		&model.ID,
		&model.ConnectionID,
		&model.IngestionJobID,
		&model.FetcherJobID,
		&model.Status,
		&model.CreatedAt,
		&model.UpdatedAt,
		&model.BridgeAttempts,
		&model.BridgeLastError,
		&model.BridgeFailedAt,
	); err != nil {
		return nil, err
	}

	status, err := vo.ParseExtractionStatus(model.Status)
	if err != nil {
		return nil, fmt.Errorf("parse extraction status %q: %w", model.Status, err)
	}

	var ingestionJobID uuid.UUID
	if model.IngestionJobID.Valid {
		ingestionJobID = model.IngestionJobID.UUID
	}

	var bridgeLastError vo.BridgeErrorClass

	if model.BridgeLastError.Valid && model.BridgeLastError.String != "" {
		parsed, parseErr := vo.ParseBridgeErrorClass(model.BridgeLastError.String)
		if parseErr != nil {
			return nil, fmt.Errorf("parse bridge_last_error %q: %w", model.BridgeLastError.String, parseErr)
		}

		bridgeLastError = parsed
	}

	var bridgeFailedAt time.Time
	if model.BridgeFailedAt.Valid {
		bridgeFailedAt = model.BridgeFailedAt.Time
	}

	return &entities.ExtractionRequest{
		ID:              model.ID,
		ConnectionID:    model.ConnectionID,
		IngestionJobID:  ingestionJobID,
		FetcherJobID:    nullStringToString(model.FetcherJobID),
		Status:          status,
		CreatedAt:       model.CreatedAt,
		UpdatedAt:       model.UpdatedAt,
		BridgeAttempts:  model.BridgeAttempts,
		BridgeLastError: bridgeLastError,
		BridgeFailedAt:  bridgeFailedAt,
	}, nil
}
