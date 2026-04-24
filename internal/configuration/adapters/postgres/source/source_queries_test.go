// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package source

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// NOTE: FindByContextID and FindByContextIDAndType CRUD tests are already covered
// in source_sqlmock_test.go (success, empty, cursor, error, nil checks, invalid cursor).
// This file tests ONLY functions not covered there: FindByContextIDWithTx.

// ---- FindByContextIDWithTx ----

func TestFindByContextIDWithTx_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository
	result, _, err := repo.FindByContextIDWithTx(context.Background(), nil, uuid.New(), "", 10)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByContextIDWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	result, _, err := repo.FindByContextIDWithTx(context.Background(), nil, uuid.New(), "", 10)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByContextIDWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, _, err := repo.FindByContextIDWithTx(context.Background(), nil, uuid.New(), "", 10)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestFindByContextIDWithTx_InvalidCursor(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	// We need a real transaction for the WithTx call, but cursor parsing
	// happens before any SQL execution so the tx won't be used.
	mockDB, mockSql, err := sqlmock.New()
	require.NoError(t, err)

	defer mockDB.Close()

	mockSql.ExpectBegin()

	tx, err := mockDB.Begin()
	require.NoError(t, err)

	t.Run("invalid base64 cursor returns error", func(t *testing.T) {
		t.Parallel()

		invalidCursor := "not-valid-base64-!@#$%"
		result, pagination, err := repo.FindByContextIDWithTx(context.Background(), tx, uuid.New(), invalidCursor, 10)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Nil(t, result)
		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})
}

func TestFindByContextIDWithTx_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	contextID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{"key":"value"}`)

	// Begin the outer tx that the caller provides
	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	// The repository query happens within the provided tx
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
		}).
			AddRow(id1.String(), contextID.String(), "Source 1", "LEDGER", "LEFT", configJSON, now, now).
			AddRow(id2.String(), contextID.String(), "Source 2", "GATEWAY", "RIGHT", configJSON, now, now))

	results, _, err := repo.FindByContextIDWithTx(ctx, tx, contextID, "", 10)

	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, id1, results[0].ID)
	assert.Equal(t, id2, results[1].ID)
}

func TestFindByContextIDWithTx_EmptyResult(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
		}))

	results, _, err := repo.FindByContextIDWithTx(ctx, tx, contextID, "", 10)

	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestFindByContextIDWithTx_QueryError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnError(errors.New("database timeout"))

	results, _, err := repo.FindByContextIDWithTx(ctx, tx, contextID, "", 10)

	require.Error(t, err)
	require.Nil(t, results)
	assert.Contains(t, err.Error(), "find reconciliation sources by context with tx")
}

func TestFindByContextIDWithTx_ScanError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	// Return rows with null values to trigger a scan error (ID cannot be null)
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
		}).AddRow(nil, nil, nil, nil, nil, nil, nil, nil))

	results, _, err := repo.FindByContextIDWithTx(ctx, tx, contextID, "", 10)

	require.Error(t, err)
	require.Nil(t, results)
}

// ---- executeSourceQueryWithTx ----

func TestExecuteSourceQueryWithTx_NilTx(t *testing.T) {
	t.Parallel()

	// executeSourceQueryWithTx is called by FindByContextIDWithTx which
	// does nil tx check first. Test the direct internal function path
	// indirectly through the public API (already tested above).
	// This additional test verifies the repository's behavior when tx is nil.
	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	_, _, err = repo.FindByContextIDWithTx(context.Background(), nil, uuid.New(), "", 10)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

// ---- FindByContextID connection error ----
// (complements source_sqlmock_test.go ProviderConnectionError tests)

func TestFindByContextIDWithTx_WithCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	contextID := uuid.New()
	cursorID := uuid.New()
	id := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
		}).AddRow(id.String(), contextID.String(), "Source", "BANK", "LEFT", configJSON, now, now))

	cursorStr := encodeLibCommonsTestCursor(t, cursorID, libHTTP.CursorDirectionNext)

	results, _, err := repo.FindByContextIDWithTx(ctx, tx, contextID, cursorStr, 10)

	require.NoError(t, err)
	require.Len(t, results, 1)
}

// NOTE: GetContextIDBySourceID tests are in source_lookup_test.go.

// ---- Validate limit handling ----

func TestFindByContextIDWithTx_DefaultLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	// Empty result is fine — we're testing that a zero limit doesn't panic.
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
		}))

	// Pass limit=0 — ValidateLimit should clamp it to the default.
	results, _, err := repo.FindByContextIDWithTx(ctx, tx, contextID, "", 0)

	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestFindByContextIDWithTx_NegativeLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	contextID := uuid.New()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
		}))

	// Negative limit should be clamped by ValidateLimit.
	results, _, err := repo.FindByContextIDWithTx(ctx, tx, contextID, "", -5)

	require.NoError(t, err)
	assert.Empty(t, results)
}
