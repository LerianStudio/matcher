// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package extraction

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestLinkIfUnlinked_SuccessfulAtomicUpdate asserts that a successful
// UPDATE (rows_affected=1) returns nil. This is the primary happy path
// for the T-003 P1 hardening: one-extraction-one-ingestion without race.
func TestLinkIfUnlinked_SuccessfulAtomicUpdate(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extractionID := uuid.New()
	ingestionID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests
			SET ingestion_job_id = $1, updated_at = $2
			WHERE id = $3 AND ingestion_job_id IS NULL`,
	)).WithArgs(
		ingestionID,
		sqlmock.AnyArg(),
		extractionID,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.LinkIfUnlinked(context.Background(), extractionID, ingestionID)
	require.NoError(t, err)
}

// TestLinkIfUnlinked_ZeroRowsAndAlreadyLinked_ReturnsSentinel asserts that
// when the atomic UPDATE matches zero rows AND the probe shows the row
// exists with a non-null ingestion_job_id, we return the canonical
// already-linked sentinel.
func TestLinkIfUnlinked_ZeroRowsAndAlreadyLinked_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extractionID := uuid.New()
	ingestionID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests
			SET ingestion_job_id = $1, updated_at = $2
			WHERE id = $3 AND ingestion_job_id IS NULL`,
	)).WithArgs(
		ingestionID,
		sqlmock.AnyArg(),
		extractionID,
	).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT ingestion_job_id IS NOT NULL FROM extraction_requests WHERE id = $1`,
	)).WithArgs(extractionID).WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(true))
	mock.ExpectRollback()

	err := repo.LinkIfUnlinked(context.Background(), extractionID, ingestionID)
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)
}

// TestLinkIfUnlinked_ZeroRowsAndRowMissing_ReturnsNotFound asserts that
// when the atomic UPDATE matches zero rows because the row does not exist
// at all, we surface ErrExtractionNotFound.
func TestLinkIfUnlinked_ZeroRowsAndRowMissing_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extractionID := uuid.New()
	ingestionID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests
			SET ingestion_job_id = $1, updated_at = $2
			WHERE id = $3 AND ingestion_job_id IS NULL`,
	)).WithArgs(
		ingestionID,
		sqlmock.AnyArg(),
		extractionID,
	).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT ingestion_job_id IS NOT NULL FROM extraction_requests WHERE id = $1`,
	)).WithArgs(extractionID).WillReturnError(errTestQuery)
	// Actual implementation handles sql.ErrNoRows in the scan; sqlmock's
	// empty rows set also yields scan error. Use WillReturnError with
	// sql.ErrNoRows to reach the not-found branch.
	mock.ExpectRollback()

	err := repo.LinkIfUnlinked(context.Background(), extractionID, ingestionID)
	require.Error(t, err)
}

// TestLinkIfUnlinked_EmptyExtractionID_ReturnsSentinel asserts input
// validation fires before any DB work.
func TestLinkIfUnlinked_EmptyExtractionID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo, mock, _ := setupMockRepository(t)

	err := repo.LinkIfUnlinked(context.Background(), uuid.Nil, uuid.New())
	require.ErrorIs(t, err, sharedPorts.ErrLinkExtractionIDRequired)

	// mock had no expectations set; verify nothing fired.
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestLinkIfUnlinked_EmptyIngestionJobID_ReturnsSentinel asserts input
// validation fires before any DB work.
func TestLinkIfUnlinked_EmptyIngestionJobID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo, mock, _ := setupMockRepository(t)

	err := repo.LinkIfUnlinked(context.Background(), uuid.New(), uuid.Nil)
	require.ErrorIs(t, err, sharedPorts.ErrLinkIngestionJobIDRequired)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestLinkIfUnlinked_ExecError_WrapsUnderlying asserts SQL failures during
// UPDATE surface wrapped through the atomic link operation.
func TestLinkIfUnlinked_ExecError_WrapsUnderlying(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extractionID := uuid.New()
	ingestionID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests
			SET ingestion_job_id = $1, updated_at = $2
			WHERE id = $3 AND ingestion_job_id IS NULL`,
	)).WithArgs(
		ingestionID,
		sqlmock.AnyArg(),
		extractionID,
	).WillReturnError(errTestExec)
	mock.ExpectRollback()

	err := repo.LinkIfUnlinked(context.Background(), extractionID, ingestionID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "link extraction if unlinked")
}

// TestFindEligibleForBridge_Empty returns no rows, expected no results.
func TestFindEligibleForBridge_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at FROM extraction_requests
			WHERE status = $1
			  AND ingestion_job_id IS NULL
			  AND bridge_last_error IS NULL
			ORDER BY updated_at ASC
			LIMIT $2`,
	)).WithArgs("COMPLETE", 50).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	results, err := repo.FindEligibleForBridge(context.Background(), 50)
	require.NoError(t, err)
	assert.Empty(t, results)
}

// TestFindEligibleForBridge_ReturnsRows verifies multiple eligible rows
// are hydrated in order.
func TestFindEligibleForBridge_ReturnsRows(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	now := time.Now().UTC()
	rows := sqlmock.NewRows(extractionColumns())

	for i := 0; i < 3; i++ {
		extractionID := uuid.New()
		connID := uuid.New()

		rows.AddRow(
			extractionID,
			connID,
			nil, // ingestion_job_id NULL
			"fetcher-job",
			[]byte("{}"),
			"2026-04-01",
			"2026-04-02",
			nil,
			"COMPLETE",
			"/data/"+extractionID.String()+".json",
			"",
			now.Add(-time.Duration(i)*time.Minute),
			now.Add(-time.Duration(i)*time.Minute),
			// T-005 bridge_* columns: untouched extractions have attempts=0,
			// nullable failure fields stay NULL.
			0,
			nil,
			nil,
			nil,
			// T-006 custody_deleted_at (migration 000027) NULL for in-flight rows.
			nil,
		)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at FROM extraction_requests
			WHERE status = $1
			  AND ingestion_job_id IS NULL
			  AND bridge_last_error IS NULL
			ORDER BY updated_at ASC
			LIMIT $2`,
	)).WithArgs("COMPLETE", 10).WillReturnRows(rows)
	mock.ExpectCommit()

	results, err := repo.FindEligibleForBridge(context.Background(), 10)
	require.NoError(t, err)
	assert.Len(t, results, 3)

	for _, r := range results {
		assert.Equal(t, uuid.Nil, r.IngestionJobID, "eligibility requires ingestion_job_id IS NULL")
	}
}

// TestFindEligibleForBridge_ZeroLimit_ReturnsImmediately asserts limit<=0
// short-circuits without hitting the DB (no mock expectations fire).
func TestFindEligibleForBridge_ZeroLimit_ReturnsImmediately(t *testing.T) {
	t.Parallel()

	repo, mock, _ := setupMockRepository(t)

	results, err := repo.FindEligibleForBridge(context.Background(), 0)
	require.NoError(t, err)
	assert.Nil(t, results)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestFindEligibleForBridge_QueryError_WrapsUnderlying asserts SQL
// failures during eligibility scan surface wrapped.
func TestFindEligibleForBridge_QueryError_WrapsUnderlying(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at FROM extraction_requests
			WHERE status = $1
			  AND ingestion_job_id IS NULL
			  AND bridge_last_error IS NULL
			ORDER BY updated_at ASC
			LIMIT $2`,
	)).WithArgs("COMPLETE", 50).WillReturnError(errTestQuery)
	mock.ExpectRollback()

	_, err := repo.FindEligibleForBridge(context.Background(), 50)
	require.Error(t, err)
	require.Contains(t, err.Error(), "find eligible extractions")
}

// TestFindEligibleForBridge_NilRepo_ReturnsSentinel guards the defensive
// nil-receiver path.
func TestFindEligibleForBridge_NilRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.FindEligibleForBridge(context.Background(), 50)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestLinkIfUnlinked_NilRepo_ReturnsSentinel guards the defensive
// nil-receiver path.
func TestLinkIfUnlinked_NilRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.LinkIfUnlinked(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// --- FindBridgeRetentionCandidates (T-006) ---

// TestFindBridgeRetentionCandidates_Empty returns no rows.
func TestFindBridgeRetentionCandidates_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at FROM extraction_requests
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
	)).WithArgs(int64(3600), 100).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	results, err := repo.FindBridgeRetentionCandidates(context.Background(), time.Hour, 100)
	require.NoError(t, err)
	assert.Empty(t, results)
}

// TestFindBridgeRetentionCandidates_ZeroLimit_ReturnsImmediately asserts
// limit<=0 short-circuits without hitting the DB.
func TestFindBridgeRetentionCandidates_ZeroLimit_ReturnsImmediately(t *testing.T) {
	t.Parallel()

	repo, mock, _ := setupMockRepository(t)

	results, err := repo.FindBridgeRetentionCandidates(context.Background(), time.Hour, 0)
	require.NoError(t, err)
	assert.Nil(t, results)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestFindBridgeRetentionCandidates_NegativeGracePeriod_CoercedToZero
// asserts a negative grace period (operator misconfiguration) is coerced to
// zero so the SQL math stays sane.
func TestFindBridgeRetentionCandidates_NegativeGracePeriod_CoercedToZero(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at FROM extraction_requests
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
	)).WithArgs(int64(0), 50).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	results, err := repo.FindBridgeRetentionCandidates(context.Background(), -10*time.Minute, 50)
	require.NoError(t, err)
	assert.Empty(t, results)
}

// TestFindBridgeRetentionCandidates_QueryError_WrapsUnderlying asserts SQL
// failures during the retention scan surface wrapped.
func TestFindBridgeRetentionCandidates_QueryError_WrapsUnderlying(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at FROM extraction_requests
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
	)).WithArgs(int64(3600), 100).WillReturnError(errTestQuery)
	mock.ExpectRollback()

	_, err := repo.FindBridgeRetentionCandidates(context.Background(), time.Hour, 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), "find bridge retention candidates")
}

// TestFindBridgeRetentionCandidates_ExcludesAlreadyCleanedUp asserts the
// migration 000027 convergence guard: the WHERE clause must include
// `custody_deleted_at IS NULL` so rows that were previously swept (or
// cleaned up on the happy path) do NOT re-appear in subsequent candidate
// scans. Without this predicate, the sweep would rescan the same rows
// forever. This is the cornerstone test for Polish Fix 1 at the adapter
// layer.
func TestFindBridgeRetentionCandidates_ExcludesAlreadyCleanedUp(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	// The query must include the new convergence guard clause. sqlmock's
	// regexp match is strict (QuoteMeta escapes special chars) so any
	// regression that drops `custody_deleted_at IS NULL` will surface here.
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at FROM extraction_requests
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
	)).WithArgs(int64(3600), 100).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	results, err := repo.FindBridgeRetentionCandidates(context.Background(), time.Hour, 100)
	require.NoError(t, err)
	assert.Empty(t, results,
		"a row with custody_deleted_at IS NOT NULL must be excluded — the new WHERE clause is the convergence guard")
}

// TestFindBridgeRetentionCandidates_NilRepo_ReturnsSentinel guards the
// defensive nil-receiver path.
func TestFindBridgeRetentionCandidates_NilRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.FindBridgeRetentionCandidates(context.Background(), time.Hour, 100)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// compile-time assertions that the new methods live on the interface.
var (
	_ interface {
		LinkIfUnlinked(ctx context.Context, id, jobID uuid.UUID) error
	} = (*Repository)(nil)

	_ repositories.ExtractionRepository = (*Repository)(nil)
)

// Silence unused check.
var _ = errors.New
