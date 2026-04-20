// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package job

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindLatestByExtractionID_NilRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var repo *Repository
	got, err := repo.FindLatestByExtractionID(context.Background(), uuid.New())
	require.ErrorIs(t, err, errRepoNotInit)
	assert.Nil(t, got)
}

func TestFindLatestByExtractionID_NilProvider_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	got, err := repo.FindLatestByExtractionID(context.Background(), uuid.New())
	require.ErrorIs(t, err, errRepoNotInit)
	assert.Nil(t, got)
}

func TestFindLatestByExtractionID_ZeroUUID_ShortCircuitsToNil(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMock(t)
	defer finish()

	// No mock expectations set; the call must short-circuit before any SQL.
	got, err := repo.FindLatestByExtractionID(context.Background(), uuid.Nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestFindLatestByExtractionID_NoMatch_ReturnsNilNoError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	extractionID := uuid.New()
	query := regexp.QuoteMeta(
		`SELECT ` + jobColumns + ` FROM ingestion_jobs
				WHERE metadata->>'extractionId' = $1
				  AND status = 'COMPLETED'
				ORDER BY created_at DESC
				LIMIT 1`,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(extractionID.String()).
		WillReturnError(sql.ErrNoRows)
	// Inner closure swallows sql.ErrNoRows into (nil, nil) — so the
	// transaction commits cleanly, not rolls back.
	mock.ExpectCommit()

	got, err := repo.FindLatestByExtractionID(context.Background(), extractionID)
	require.NoError(t, err)
	assert.Nil(t, got, "no row → (nil, nil), not a sentinel")
}

func TestFindLatestByExtractionID_HappyPath_ReturnsHydratedJob(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	extractionID := uuid.New()
	jobID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()
	now := time.Now().UTC()

	query := regexp.QuoteMeta(
		`SELECT ` + jobColumns + ` FROM ingestion_jobs
				WHERE metadata->>'extractionId' = $1
				  AND status = 'COMPLETED'
				ORDER BY created_at DESC
				LIMIT 1`,
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		jobID.String(),
		contextID.String(),
		sourceID.String(),
		"COMPLETED",
		now,
		sql.NullTime{Time: now.Add(time.Minute), Valid: true},
		[]byte(`{"fileName":"fetcher-stream","totalRows":100,"extractionId":"`+extractionID.String()+`"}`),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(extractionID.String()).
		WillReturnRows(rows)
	mock.ExpectCommit()

	got, err := repo.FindLatestByExtractionID(context.Background(), extractionID)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, jobID, got.ID)
	assert.Equal(t, extractionID.String(), got.Metadata.ExtractionID)
	assert.Equal(t, 100, got.Metadata.TotalRows)
}

// TestFindLatestByExtractionID_FailedJob_NotReturned is the Polish Fix 1
// regression: a FAILED job stamped with extractionId must not be returned by
// the short-circuit lookup. Without the status='COMPLETED' filter, Tick 2
// would link the extraction to a FAILED ingestion job — silent data loss.
//
// We model this at the SQL layer by asserting the WHERE clause carries the
// status='COMPLETED' predicate. The mock returns sql.ErrNoRows because the
// real DB would not match (FAILED row excluded by the predicate).
func TestFindLatestByExtractionID_FailedJob_FilteredByQuery(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	extractionID := uuid.New()
	query := regexp.QuoteMeta(
		`SELECT ` + jobColumns + ` FROM ingestion_jobs
				WHERE metadata->>'extractionId' = $1
				  AND status = 'COMPLETED'
				ORDER BY created_at DESC
				LIMIT 1`,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(extractionID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectCommit()

	got, err := repo.FindLatestByExtractionID(context.Background(), extractionID)
	require.NoError(t, err)
	assert.Nil(t, got, "FAILED job stamped with extractionId must not match — status filter excludes it")
}

// TestFindLatestByExtractionID_CompletedJob_Returned is the happy-path
// counterpart: a COMPLETED job stamped with the extractionId IS returned by
// the short-circuit lookup. Same query shape, different mock outcome.
func TestFindLatestByExtractionID_CompletedJob_Returned(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	extractionID := uuid.New()
	jobID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()
	now := time.Now().UTC()

	query := regexp.QuoteMeta(
		`SELECT ` + jobColumns + ` FROM ingestion_jobs
				WHERE metadata->>'extractionId' = $1
				  AND status = 'COMPLETED'
				ORDER BY created_at DESC
				LIMIT 1`,
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		jobID.String(),
		contextID.String(),
		sourceID.String(),
		"COMPLETED",
		now,
		sql.NullTime{Time: now.Add(time.Minute), Valid: true},
		[]byte(`{"fileName":"fetcher-stream","totalRows":5,"extractionId":"`+extractionID.String()+`"}`),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(extractionID.String()).
		WillReturnRows(rows)
	mock.ExpectCommit()

	got, err := repo.FindLatestByExtractionID(context.Background(), extractionID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, jobID, got.ID)
}

func TestFindLatestByExtractionID_QueryError_Wraps(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	extractionID := uuid.New()
	wantErr := errors.New("connection refused")

	query := regexp.QuoteMeta(
		`SELECT ` + jobColumns + ` FROM ingestion_jobs
				WHERE metadata->>'extractionId' = $1
				  AND status = 'COMPLETED'
				ORDER BY created_at DESC
				LIMIT 1`,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).
		WithArgs(extractionID.String()).
		WillReturnError(wantErr)
	mock.ExpectRollback()

	got, err := repo.FindLatestByExtractionID(context.Background(), extractionID)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.True(t, errors.Is(err, wantErr))
	assert.Contains(t, err.Error(), "find ingestion job by extraction id")
}
