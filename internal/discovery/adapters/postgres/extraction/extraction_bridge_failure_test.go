// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package extraction

import (
	"context"
	"errors"
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
	mock.ExpectRollback()

	err := repo.MarkBridgeFailed(context.Background(), extraction)
	require.Error(t, err)
	assert.True(t, errors.Is(err, repositories.ErrExtractionNotFound))
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
