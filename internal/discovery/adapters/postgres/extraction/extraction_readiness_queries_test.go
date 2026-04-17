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

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// TestCountBridgeReadiness_HappyPath asserts the FILTER aggregate query is
// shaped correctly and the five buckets land in the right struct fields.
func TestCountBridgeReadiness_HappyPath(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NOT NULL) AS ready_count,
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NULL
					AND bridge_last_error IS NULL
					AND EXTRACT(EPOCH FROM (NOW() - created_at)) <= $2) AS pending_count,
				COUNT(*) FILTER (WHERE status = $1 AND ingestion_job_id IS NULL
					AND bridge_last_error IS NULL
					AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2) AS stale_count,
				COUNT(*) FILTER (WHERE status IN ($3, $4) OR bridge_last_error IS NOT NULL) AS failed_count,
				COUNT(*) FILTER (WHERE status IN ($5, $6, $7)) AS in_flight_count
			FROM extraction_requests`,
	)).WithArgs(
		string(vo.ExtractionStatusComplete),
		float64(3600),
		string(vo.ExtractionStatusFailed),
		string(vo.ExtractionStatusCancelled),
		string(vo.ExtractionStatusPending),
		string(vo.ExtractionStatusSubmitted),
		string(vo.ExtractionStatusExtracting),
	).WillReturnRows(sqlmock.NewRows([]string{
		"ready_count", "pending_count", "stale_count", "failed_count", "in_flight_count",
	}).AddRow(int64(7), int64(2), int64(3), int64(1), int64(4)))
	mock.ExpectCommit()

	counts, err := repo.CountBridgeReadiness(context.Background(), time.Hour)
	require.NoError(t, err)

	assert.Equal(t, int64(7), counts.Ready)
	assert.Equal(t, int64(2), counts.Pending)
	assert.Equal(t, int64(3), counts.Stale)
	assert.Equal(t, int64(1), counts.Failed)
	assert.Equal(t, int64(4), counts.InFlightCount)
	assert.Equal(t, int64(17), counts.Total())
}

// TestCountBridgeReadiness_NilReceiver asserts the nil-receiver guard fires
// before any SQL is issued.
func TestCountBridgeReadiness_NilReceiver(t *testing.T) {
	t.Parallel()

	var repo *Repository
	_, err := repo.CountBridgeReadiness(context.Background(), time.Hour)
	assert.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestCountBridgeReadiness_NilProvider asserts the missing-provider guard
// fires before any SQL is issued.
func TestCountBridgeReadiness_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	_, err := repo.CountBridgeReadiness(context.Background(), time.Hour)
	assert.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestCountBridgeReadiness_QueryError surfaces SQL errors with the wrap
// chain intact so callers can errors.Is on the underlying driver error.
func TestCountBridgeReadiness_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery("COUNT").WillReturnError(errTestQuery)
	mock.ExpectRollback()

	_, err := repo.CountBridgeReadiness(context.Background(), time.Hour)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errTestQuery))
}

// TestCountBridgeReadiness_ClampsZeroOrNegativeThreshold proves the
// staleThreshold floor at 1 second; otherwise the COMPLETE+unlinked rows
// would all collapse into the stale bucket via division-by-zero-adjacent
// edge cases.
func TestCountBridgeReadiness_ClampsZeroOrNegativeThreshold(t *testing.T) {
	t.Parallel()

	for _, threshold := range []time.Duration{0, -time.Second, -time.Hour} {
		threshold := threshold

		t.Run(threshold.String(), func(t *testing.T) {
			t.Parallel()

			repo, mock, finish := setupMockRepository(t)
			defer finish()

			mock.ExpectBegin()
			mock.ExpectQuery("COUNT").WithArgs(
				string(vo.ExtractionStatusComplete),
				float64(1),
				string(vo.ExtractionStatusFailed),
				string(vo.ExtractionStatusCancelled),
				string(vo.ExtractionStatusPending),
				string(vo.ExtractionStatusSubmitted),
				string(vo.ExtractionStatusExtracting),
			).WillReturnRows(sqlmock.NewRows([]string{
				"ready_count", "pending_count", "stale_count", "failed_count", "in_flight_count",
			}).AddRow(int64(0), int64(0), int64(0), int64(0), int64(0)))
			mock.ExpectCommit()

			_, err := repo.CountBridgeReadiness(context.Background(), threshold)
			require.NoError(t, err)
		})
	}
}

// TestListBridgeCandidates_PendingState_NoCursor asserts the pending state
// composes the right WHERE clause and uses the default page limit.
func TestListBridgeCandidates_PendingState_NoCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT `+allColumns+` FROM extraction_requests WHERE status = $1 AND ingestion_job_id IS NULL AND bridge_last_error IS NULL AND EXTRACT(EPOCH FROM (NOW() - created_at)) <= $2 ORDER BY created_at ASC, id ASC LIMIT $3`,
	)).WithArgs(
		string(vo.ExtractionStatusComplete),
		float64(3600),
		defaultBridgeCandidatesPerPage,
	).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	rows, err := repo.ListBridgeCandidates(
		context.Background(),
		"pending",
		time.Hour,
		time.Time{},
		uuid.Nil,
		0,
	)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// TestListBridgeCandidates_ReadyState_WithCursor asserts the keyset
// cursor predicate is appended when the caller passes a non-zero anchor.
func TestListBridgeCandidates_ReadyState_WithCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	cursorTime := time.Now().UTC().Add(-2 * time.Hour)
	cursorID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT `+allColumns+` FROM extraction_requests WHERE status = $1 AND ingestion_job_id IS NOT NULL AND (created_at, id) > ($2, $3) ORDER BY created_at ASC, id ASC LIMIT $4`,
	)).WithArgs(
		string(vo.ExtractionStatusComplete),
		cursorTime,
		cursorID,
		25,
	).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	_, err := repo.ListBridgeCandidates(
		context.Background(),
		"ready",
		time.Hour,
		cursorTime,
		cursorID,
		25,
	)
	require.NoError(t, err)
}

// TestListBridgeCandidates_StaleState builds the predicate that mirrors the
// stale partition in CountBridgeReadiness.
func TestListBridgeCandidates_StaleState(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT `+allColumns+` FROM extraction_requests WHERE status = $1 AND ingestion_job_id IS NULL AND bridge_last_error IS NULL AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2 ORDER BY created_at ASC, id ASC LIMIT $3`,
	)).WithArgs(
		string(vo.ExtractionStatusComplete),
		float64(60),
		10,
	).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	_, err := repo.ListBridgeCandidates(context.Background(), "stale", time.Minute, time.Time{}, uuid.Nil, 10)
	require.NoError(t, err)
}

// TestListBridgeCandidates_FailedState covers FAILED and CANCELLED inclusion.
func TestListBridgeCandidates_FailedState(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT `+allColumns+` FROM extraction_requests WHERE (status IN ($1, $2) OR bridge_last_error IS NOT NULL) ORDER BY created_at ASC, id ASC LIMIT $3`,
	)).WithArgs(
		string(vo.ExtractionStatusFailed),
		string(vo.ExtractionStatusCancelled),
		defaultBridgeCandidatesPerPage,
	).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	_, err := repo.ListBridgeCandidates(context.Background(), "failed", time.Hour, time.Time{}, uuid.Nil, 0)
	require.NoError(t, err)
}

// TestListBridgeCandidates_InFlightState covers PENDING + SUBMITTED + EXTRACTING
// inclusion (the upstream-extraction-in-progress partition).
func TestListBridgeCandidates_InFlightState(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT `+allColumns+` FROM extraction_requests WHERE status IN ($1, $2, $3) ORDER BY created_at ASC, id ASC LIMIT $4`,
	)).WithArgs(
		string(vo.ExtractionStatusPending),
		string(vo.ExtractionStatusSubmitted),
		string(vo.ExtractionStatusExtracting),
		defaultBridgeCandidatesPerPage,
	).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	_, err := repo.ListBridgeCandidates(context.Background(), "in_flight", time.Hour, time.Time{}, uuid.Nil, 0)
	require.NoError(t, err)
}

// TestListBridgeCandidates_InvalidState rejects unknown states without
// hitting the database.
func TestListBridgeCandidates_InvalidState(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	_, err := repo.ListBridgeCandidates(context.Background(), "bogus", time.Hour, time.Time{}, uuid.Nil, 50)
	require.Error(t, err)
	assert.True(t, errors.Is(err, vo.ErrInvalidBridgeReadinessState))
}

// TestListBridgeCandidates_NilReceiver short-circuits before SQL.
func TestListBridgeCandidates_NilReceiver(t *testing.T) {
	t.Parallel()

	var repo *Repository
	_, err := repo.ListBridgeCandidates(context.Background(), "ready", time.Hour, time.Time{}, uuid.Nil, 50)
	assert.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestListBridgeCandidates_ClampsLimitAboveMax prevents callers from
// requesting more than maxBridgeCandidatesPerPage rows.
func TestListBridgeCandidates_ClampsLimitAboveMax(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WithArgs(
		string(vo.ExtractionStatusComplete),
		maxBridgeCandidatesPerPage,
	).WillReturnRows(sqlmock.NewRows(extractionColumns()))
	mock.ExpectCommit()

	_, err := repo.ListBridgeCandidates(context.Background(), "ready", time.Hour, time.Time{}, uuid.Nil, 9999)
	require.NoError(t, err)
}

// TestListBridgeCandidates_QueryError surfaces driver errors via the wrap chain.
func TestListBridgeCandidates_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnError(errTestQuery)
	mock.ExpectRollback()

	_, err := repo.ListBridgeCandidates(context.Background(), "ready", time.Hour, time.Time{}, uuid.Nil, 50)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errTestQuery))
}

// TestClampPageLimit covers the three branches of the helper.
func TestClampPageLimit(t *testing.T) {
	t.Parallel()

	assert.Equal(t, defaultBridgeCandidatesPerPage, clampPageLimit(0))
	assert.Equal(t, defaultBridgeCandidatesPerPage, clampPageLimit(-5))
	assert.Equal(t, 1, clampPageLimit(1))
	assert.Equal(t, 100, clampPageLimit(100))
	assert.Equal(t, maxBridgeCandidatesPerPage, clampPageLimit(maxBridgeCandidatesPerPage))
	assert.Equal(t, maxBridgeCandidatesPerPage, clampPageLimit(maxBridgeCandidatesPerPage+1))
	assert.Equal(t, maxBridgeCandidatesPerPage, clampPageLimit(1_000_000))
}
