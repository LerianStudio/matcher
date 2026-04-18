// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package extraction

import (
	"context"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// TestMarkCustodyDeleted_SuccessfulUpdate asserts the happy-path: a single
// narrow UPDATE touches only custody_deleted_at, returns nil on rows=1.
func TestMarkCustodyDeleted_SuccessfulUpdate(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	id := uuid.New()
	deletedAt := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests SET
			custody_deleted_at = $1
		WHERE id = $2`,
	)).WithArgs(
		deletedAt,
		id,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.MarkCustodyDeleted(context.Background(), id, deletedAt)
	require.NoError(t, err)
}

// TestMarkCustodyDeleted_NotFound asserts the zero-rows case surfaces the
// ExtractionNotFound sentinel — important so callers can distinguish a
// benign "already cleaned up by another writer" from a transient error.
func TestMarkCustodyDeleted_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	id := uuid.New()
	deletedAt := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests SET
			custody_deleted_at = $1
		WHERE id = $2`,
	)).WithArgs(
		deletedAt,
		id,
	).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.MarkCustodyDeleted(context.Background(), id, deletedAt)
	require.ErrorIs(t, err, repositories.ErrExtractionNotFound)
}

// TestMarkCustodyDeleted_NilRepo_ReturnsSentinel guards the nil-receiver
// defensive path.
func TestMarkCustodyDeleted_NilRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.MarkCustodyDeleted(context.Background(), uuid.New(), time.Now())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestMarkCustodyDeletedWithTx_SuccessfulUpdate exercises the WithTx variant
// required by the repositorytx linter.
func TestMarkCustodyDeletedWithTx_SuccessfulUpdate(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	id := uuid.New()
	deletedAt := time.Now().UTC()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests SET
			custody_deleted_at = $1
		WHERE id = $2`,
	)).WithArgs(
		deletedAt,
		id,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	err = repo.MarkCustodyDeletedWithTx(context.Background(), tx, id, deletedAt)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkCustodyDeletedWithTx_NilTx_ReturnsSentinel covers the argument
// validation path required for the WithTx variant.
func TestMarkCustodyDeletedWithTx_NilTx_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.MarkCustodyDeletedWithTx(context.Background(), nil, uuid.New(), time.Now())
	require.ErrorIs(t, err, ErrTransactionRequired)
}

// TestMarkCustodyDeletedWithTx_NilRepo_ReturnsSentinel guards the nil-
// receiver defensive path in the WithTx variant.
func TestMarkCustodyDeletedWithTx_NilRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.MarkCustodyDeletedWithTx(context.Background(), nil, uuid.New(), time.Now())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestMarkCustodyDeleted_CoercesToUTC asserts the stored timestamp is
// normalised to UTC. Without this, a local-timezone time.Now() could be
// persisted as e.g. PST, creating a subtle cross-DB-row consistency bug.
//
// The assertion is load-bearing because sqlmock's WithArgs uses
// reflect.DeepEqual, and two time.Time values with identical wall clocks
// but different Locations are NOT DeepEqual. So passing `localTime` (PST)
// into MarkCustodyDeleted would fail to match the `expectedUTC` arg in
// the mock unless the repo actually coerces to UTC before the UPDATE.
func TestMarkCustodyDeleted_CoercesToUTC(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	id := uuid.New()
	// Non-UTC timezone to verify coercion. Asia/Tokyo is +09:00 which
	// straddles a calendar-day boundary from UTC — if coercion were
	// broken, a naive persist would store the wrong wall clock, not just
	// the wrong zone.
	loc, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)

	localTime := time.Date(2026, 4, 16, 2, 30, 0, 0, loc) // 02:30 JST = 17:30 previous-day UTC
	expectedUTC := localTime.UTC()

	// Sanity: the input is NOT in UTC. Without this precondition the
	// test would be trivially passing when loc somehow resolves to UTC.
	require.NotEqual(t, time.UTC, localTime.Location(),
		"test precondition: localTime must be non-UTC to exercise coercion")
	require.Equal(t, time.UTC, expectedUTC.Location(),
		"test precondition: expectedUTC must be in UTC")

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE extraction_requests SET
			custody_deleted_at = $1
		WHERE id = $2`,
	)).WithArgs(
		expectedUTC,
		id,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = repo.MarkCustodyDeleted(context.Background(), id, localTime)
	require.NoError(t, err,
		"pass non-UTC time; repo must coerce to UTC so sqlmock's WithArgs match succeeds")
}

// Compile-time assertion that the new methods live on the interface
// (defense-in-depth against a future interface change that drops them).
var (
	_ interface {
		MarkCustodyDeleted(ctx context.Context, id uuid.UUID, deletedAt time.Time) error
	} = (*Repository)(nil)
)
