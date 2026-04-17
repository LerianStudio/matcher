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

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func newTerminallyFailedExtraction() *entities.ExtractionRequest {
	now := time.Now().UTC()
	return &entities.ExtractionRequest{
		ID:                     uuid.New(),
		ConnectionID:           uuid.New(),
		Status:                 vo.ExtractionStatusComplete,
		FetcherJobID:           "fetcher-job-1",
		Tables:                 map[string]any{},
		BridgeAttempts:         3,
		BridgeLastError:        vo.BridgeErrorClassArtifactNotFound,
		BridgeLastErrorMessage: "404 from fetcher",
		BridgeFailedAt:         now,
		CreatedAt:              now,
		UpdatedAt:              now,
	}
}

func TestMarkBridgeFailed_NilRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var repo *Repository
	err := repo.MarkBridgeFailed(context.Background(), newTerminallyFailedExtraction())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestMarkBridgeFailed_NilProvider_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	err := repo.MarkBridgeFailed(context.Background(), newTerminallyFailedExtraction())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestMarkBridgeFailed_NilEntity_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.MarkBridgeFailed(context.Background(), nil)
	require.ErrorIs(t, err, ErrEntityRequired)
}

func TestMarkBridgeFailed_HappyPath_PersistsBridgeColumns(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extraction := newTerminallyFailedExtraction()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE extraction_requests SET").
		WithArgs(
			extraction.BridgeAttempts,
			extraction.BridgeLastError.String(),
			extraction.BridgeLastErrorMessage,
			sqlmock.AnyArg(), // bridge_failed_at
			sqlmock.AnyArg(), // updated_at refreshed inside exec
			extraction.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.MarkBridgeFailed(context.Background(), extraction)
	require.NoError(t, err)
}

func TestMarkBridgeFailed_RowMissing_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extraction := newTerminallyFailedExtraction()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE extraction_requests SET").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// C21: existence probe disambiguates "row missing" vs "concurrent link won"
	// when the gated UPDATE matches zero rows.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT EXISTS(SELECT 1 FROM extraction_requests WHERE id = $1)`,
	)).WithArgs(extraction.ID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectRollback()

	err := repo.MarkBridgeFailed(context.Background(), extraction)
	require.Error(t, err)
	assert.True(t, errors.Is(err, repositories.ErrExtractionNotFound))
}

// TestMarkBridgeFailed_ConcurrentLinkWon_ReturnsAlreadyLinked is the C21
// regression: under a lock-TTL-expiry race, Replica A successfully linked the
// extraction while Replica B's orchestrator failed downstream and tries to
// persist a terminal bridge failure. The NULL-guard must cause the UPDATE to
// match zero rows, the existence probe must confirm the row exists, and the
// repository must surface ErrExtractionAlreadyLinked so the worker can treat
// the terminal-failure write as benignly skipped (the link is the
// authoritative outcome).
func TestMarkBridgeFailed_ConcurrentLinkWon_ReturnsAlreadyLinked(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extraction := newTerminallyFailedExtraction()

	mock.ExpectBegin()
	// Gated UPDATE: the `AND ingestion_job_id IS NULL` predicate rejects the
	// write because a concurrent LinkIfUnlinked already set it — 0 rows.
	mock.ExpectExec("UPDATE extraction_requests SET").
		WithArgs(
			extraction.BridgeAttempts,
			extraction.BridgeLastError.String(),
			extraction.BridgeLastErrorMessage,
			sqlmock.AnyArg(), // bridge_failed_at
			sqlmock.AnyArg(), // updated_at refreshed inside exec
			extraction.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Existence probe: the row IS there (concurrent link, not missing).
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT EXISTS(SELECT 1 FROM extraction_requests WHERE id = $1)`,
	)).WithArgs(extraction.ID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	err := repo.MarkBridgeFailed(context.Background(), extraction)
	require.Error(t, err)
	assert.True(t, errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked),
		"MarkBridgeFailed must surface ErrExtractionAlreadyLinked so persistTerminalFailure can treat it as benign")
}

// TestMarkBridgeFailed_ExtractionNotFound_ReturnsNotFound exercises the
// negative branch of the C21 existence probe: zero rows affected AND the row
// does not exist → ErrExtractionNotFound. Complements the concurrent-link
// variant above.
func TestMarkBridgeFailed_ExtractionNotFound_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extraction := newTerminallyFailedExtraction()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE extraction_requests SET").
		WithArgs(
			extraction.BridgeAttempts,
			extraction.BridgeLastError.String(),
			extraction.BridgeLastErrorMessage,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			extraction.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT EXISTS(SELECT 1 FROM extraction_requests WHERE id = $1)`,
	)).WithArgs(extraction.ID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectRollback()

	err := repo.MarkBridgeFailed(context.Background(), extraction)
	require.Error(t, err)
	assert.True(t, errors.Is(err, repositories.ErrExtractionNotFound))
	assert.False(t, errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked),
		"a missing row must NOT be conflated with a concurrent link")
}

func TestMarkBridgeFailed_DriverError_Wraps(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	extraction := newTerminallyFailedExtraction()
	wantErr := errors.New("driver boom")

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE extraction_requests SET").
		WillReturnError(wantErr)
	mock.ExpectRollback()

	err := repo.MarkBridgeFailed(context.Background(), extraction)
	require.Error(t, err)
	assert.True(t, errors.Is(err, wantErr))
}

func TestMarkBridgeFailedWithTx_NilTx_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.MarkBridgeFailedWithTx(context.Background(), nil, newTerminallyFailedExtraction())
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestMarkBridgeFailedWithTx_HappyPath(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)
	extraction := newTerminallyFailedExtraction()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("UPDATE extraction_requests SET").
		WithArgs(
			extraction.BridgeAttempts,
			extraction.BridgeLastError.String(),
			extraction.BridgeLastErrorMessage,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			extraction.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	err = repo.MarkBridgeFailedWithTx(context.Background(), tx, extraction)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
