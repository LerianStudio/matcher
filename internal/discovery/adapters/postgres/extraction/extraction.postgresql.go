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
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time check that Repository implements ExtractionRepository.
var _ repositories.ExtractionRepository = (*Repository)(nil)

const (
	tableName = "extraction_requests"
	// allColumns is the canonical SELECT list. Order MUST match scanExtraction.
	// Bridge* columns added in migration 000026 (T-005) live at the tail so
	// adding them did not perturb existing column ordinals; custody_deleted_at
	// from migration 000027 (T-006 polish) is appended for the same reason.
	allColumns = "id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at"
)

// Repository provides PostgreSQL operations for ExtractionRequest entities.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new extraction repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create persists a new ExtractionRequest.
func (repo *Repository) Create(ctx context.Context, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_extraction_request")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeCreate(ctx, tx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create extraction request: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create extraction request")

		return wrappedErr
	}

	return nil
}

// CreateWithTx persists a new ExtractionRequest within an existing transaction.
func (repo *Repository) CreateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_extraction_request_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeCreate(ctx, innerTx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create extraction request with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create extraction request")

		return wrappedErr
	}

	return nil
}

// executeCreate performs the actual insertion within a transaction.
func (repo *Repository) executeCreate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO `+tableName+` (`+allColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		model.ID,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		model.StartDate,
		model.EndDate,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.CreatedAt,
		model.UpdatedAt,
		model.BridgeAttempts,
		model.BridgeLastError,
		model.BridgeLastErrorMessage,
		model.BridgeFailedAt,
		model.CustodyDeletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert extraction request: %w", err)
	}

	return nil
}

// Update persists changes to an existing ExtractionRequest.
func (repo *Repository) Update(ctx context.Context, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpdate(ctx, tx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update extraction request")

		return wrappedErr
	}

	return nil
}

// UpdateIfUnchanged persists changes only if the row still matches the expected updated_at value.
func (repo *Repository) UpdateIfUnchanged(ctx context.Context, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request_if_unchanged")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeConditionalUpdate(ctx, tx, req, expectedUpdatedAt); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request if unchanged: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request if unchanged", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update extraction request if unchanged")

		return wrappedErr
	}

	return nil
}

// UpdateIfUnchangedWithTx persists changes only if the row still matches the
// expected updated_at value within an existing transaction.
func (repo *Repository) UpdateIfUnchangedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	req *entities.ExtractionRequest,
	expectedUpdatedAt time.Time,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request_if_unchanged_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeConditionalUpdate(ctx, innerTx, req, expectedUpdatedAt); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request if unchanged with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request if unchanged", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update extraction request if unchanged")

		return wrappedErr
	}

	return nil
}

// UpdateWithTx persists changes within an existing transaction.
func (repo *Repository) UpdateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpdate(ctx, innerTx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update extraction request")

		return wrappedErr
	}

	return nil
}

// executeUpdate performs the actual update within a transaction.
func (repo *Repository) executeUpdate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			connection_id = $1,
			ingestion_job_id = $2,
			fetcher_job_id = $3,
			tables = $4,
			start_date = $5,
			end_date = $6,
			filters = $7,
			status = $8,
			result_path = $9,
			error_message = $10,
			updated_at = $11,
			bridge_attempts = $12,
			bridge_last_error = $13,
			bridge_last_error_message = $14,
			bridge_failed_at = $15,
			custody_deleted_at = $16
		WHERE id = $17`,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		model.StartDate,
		model.EndDate,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.UpdatedAt,
		model.BridgeAttempts,
		model.BridgeLastError,
		model.BridgeLastErrorMessage,
		model.BridgeFailedAt,
		model.CustodyDeletedAt,
		model.ID,
	)
	if err != nil {
		return fmt.Errorf("update extraction request: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (repo *Repository) executeConditionalUpdate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			connection_id = $1,
			ingestion_job_id = $2,
			fetcher_job_id = $3,
			tables = $4,
			start_date = $5,
			end_date = $6,
			filters = $7,
			status = $8,
			result_path = $9,
			error_message = $10,
			updated_at = $11,
			bridge_attempts = $12,
			bridge_last_error = $13,
			bridge_last_error_message = $14,
			bridge_failed_at = $15,
			custody_deleted_at = $16
		WHERE id = $17 AND updated_at = $18`,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		model.StartDate,
		model.EndDate,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.UpdatedAt,
		model.BridgeAttempts,
		model.BridgeLastError,
		model.BridgeLastErrorMessage,
		model.BridgeFailedAt,
		model.CustodyDeletedAt,
		model.ID,
		expectedUpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update extraction request conditionally: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return repositories.ErrExtractionConflict
	}

	return nil
}

func nullableJSON(data []byte) any {
	if data == nil {
		return nil
	}

	return data
}

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
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find extraction request by id")

		return nil, wrappedErr
	}

	return result, nil
}

// LinkIfUnlinked atomically sets ingestion_job_id on the extraction row when
// the current value is NULL. Returns sharedPorts.ErrExtractionAlreadyLinked
// when the row exists but already carries an ingestion_job_id; returns
// repositories.ErrExtractionNotFound when no row matches id.
//
// The UPDATE runs a single predicate — id = $3 AND ingestion_job_id IS NULL —
// so concurrent bridge invocations cannot both succeed. RowsAffected
// discriminates "not found" from "already linked" via a tiny follow-up SELECT
// that fires only on the zero-rows-affected path to keep the hot path
// transaction-local.
func (repo *Repository) LinkIfUnlinked(
	ctx context.Context,
	id uuid.UUID,
	ingestionJobID uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if id == uuid.Nil {
		return ports.ErrLinkExtractionIDRequired
	}

	if ingestionJobID == uuid.Nil {
		return ports.ErrLinkIngestionJobIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.extraction.link_if_unlinked")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return linkIfUnlinkedTx(ctx, tx, id, ingestionJobID)
	})
	if err != nil {
		// Wrap with %w so callers can still errors.Is on the original
		// sentinel while the error chain carries the repository context.
		wrappedErr := fmt.Errorf("link extraction if unlinked: %w", err)

		if isLinkSentinelError(err) {
			return wrappedErr
		}

		libOpentelemetry.HandleSpanError(span, "atomic link failed", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "atomic link failed")

		return wrappedErr
	}

	return nil
}

// linkIfUnlinkedTx runs the atomic UPDATE + follow-up probe inside a tenant-
// scoped transaction. Extracted from LinkIfUnlinked so the caller stays under
// the gocyclo ceiling; semantics are unchanged.
func linkIfUnlinkedTx(ctx context.Context, tx *sql.Tx, id, ingestionJobID uuid.UUID) (bool, error) {
	updatedAt := time.Now().UTC()

	result, execErr := tx.ExecContext(ctx,
		`UPDATE `+tableName+`
		SET ingestion_job_id = $1, updated_at = $2
		WHERE id = $3 AND ingestion_job_id IS NULL`,
		ingestionJobID,
		updatedAt,
		id,
	)
	if execErr != nil {
		return false, fmt.Errorf("atomic link extraction: %w", execErr)
	}

	rowsAffected, raErr := result.RowsAffected()
	if raErr != nil {
		return false, fmt.Errorf("atomic link extraction rows affected: %w", raErr)
	}

	if rowsAffected != 0 {
		return true, nil
	}

	// Zero rows affected means either the row does not exist or it is
	// already linked. Differentiate with a narrow probe so callers get a
	// precise sentinel.
	var hasIngestion sql.NullBool

	probeErr := tx.QueryRowContext(ctx,
		`SELECT ingestion_job_id IS NOT NULL FROM `+tableName+` WHERE id = $1`,
		id,
	).Scan(&hasIngestion)
	if errors.Is(probeErr, sql.ErrNoRows) {
		return false, repositories.ErrExtractionNotFound
	}

	if probeErr != nil {
		return false, fmt.Errorf("atomic link extraction probe: %w", probeErr)
	}

	if hasIngestion.Valid && hasIngestion.Bool {
		return false, ports.ErrExtractionAlreadyLinked
	}

	// Row exists and is NULL but the UPDATE still matched zero rows — this
	// should be impossible short of a tenant-schema misconfiguration. Surface
	// it as a conflict so the caller can retry in a new cycle.
	return false, repositories.ErrExtractionConflict
}

// isLinkSentinelError reports whether err matches one of the expected
// domain/port sentinels surfaced by LinkIfUnlinked. Centralised here so
// LinkIfUnlinked stays under the gocyclo ceiling and the sentinel set has a
// single authoritative list.
func isLinkSentinelError(err error) bool {
	return errors.Is(err, repositories.ErrExtractionNotFound) ||
		errors.Is(err, ports.ErrExtractionAlreadyLinked) ||
		errors.Is(err, repositories.ErrExtractionConflict) ||
		errors.Is(err, ports.ErrLinkExtractionIDRequired) ||
		errors.Is(err, ports.ErrLinkIngestionJobIDRequired)
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
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "find eligible extractions failed")

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
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "find bridge retention candidates failed")

		return nil, wrappedErr
	}

	return result, nil
}

// scanExtraction scans a SQL row into an ExtractionRequest domain entity.
// Column order MUST match allColumns; the bridge_* columns are at the tail
// because they were added in migration 000026, and custody_deleted_at is at
// the very tail because it was added in migration 000027.
func scanExtraction(scanner interface{ Scan(dest ...any) error }) (*entities.ExtractionRequest, error) {
	var model ExtractionModel
	if err := scanner.Scan(
		&model.ID,
		&model.ConnectionID,
		&model.IngestionJobID,
		&model.FetcherJobID,
		&model.Tables,
		&model.StartDate,
		&model.EndDate,
		&model.Filters,
		&model.Status,
		&model.ResultPath,
		&model.ErrorMessage,
		&model.CreatedAt,
		&model.UpdatedAt,
		&model.BridgeAttempts,
		&model.BridgeLastError,
		&model.BridgeLastErrorMessage,
		&model.BridgeFailedAt,
		&model.CustodyDeletedAt,
	); err != nil {
		return nil, err
	}

	return model.ToDomain()
}
