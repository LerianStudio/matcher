//go:build unit

package source

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// NOTE: FindByID tests are already covered in source_sqlmock_test.go
// (success, not-found, query error, nil repo, nil provider, connection error).
// This file tests ONLY functions not covered there: GetContextIDBySourceID.

// setupMockWithReplicaForLookup creates a repo backed by sqlmock with
// primary+replica for lookup paths that resolve tenant-scoped database access.
func setupMockWithReplicaForLookup(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	return repo, mock, func() { db.Close() }
}

// ---- GetContextIDBySourceID ----

func TestGetContextIDBySourceID_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository
	result, err := repo.GetContextIDBySourceID(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Equal(t, uuid.Nil, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestGetContextIDBySourceID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	result, err := repo.GetContextIDBySourceID(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Equal(t, uuid.Nil, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestGetContextIDBySourceID_ConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("connection refused")
	provider := &testutil.MockInfrastructureProvider{PostgresErr: connErr}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.GetContextIDBySourceID(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Equal(t, uuid.Nil, result)
	require.ErrorIs(t, err, connErr)
}

func TestGetContextIDBySourceID_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplicaForLookup(t)
	defer cleanup()

	sourceID := uuid.New()
	expectedContextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT context_id FROM reconciliation_sources WHERE id").
		WillReturnRows(sqlmock.NewRows([]string{"context_id"}).
			AddRow(expectedContextID.String()))
	mock.ExpectCommit()

	result, err := repo.GetContextIDBySourceID(ctx, sourceID)

	require.NoError(t, err)
	assert.Equal(t, expectedContextID, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetContextIDBySourceID_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplicaForLookup(t)
	defer cleanup()

	sourceID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT context_id FROM reconciliation_sources WHERE id").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.GetContextIDBySourceID(ctx, sourceID)

	require.Error(t, err)
	assert.Equal(t, uuid.Nil, result)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestGetContextIDBySourceID_QueryError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplicaForLookup(t)
	defer cleanup()

	sourceID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT context_id FROM reconciliation_sources WHERE id").
		WillReturnError(errors.New("network timeout"))
	mock.ExpectRollback()

	result, err := repo.GetContextIDBySourceID(ctx, sourceID)

	require.Error(t, err)
	assert.Equal(t, uuid.Nil, result)
	assert.Contains(t, err.Error(), "find context id by source id")
}

func TestGetContextIDBySourceID_InvalidContextIDInDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplicaForLookup(t)
	defer cleanup()

	sourceID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT context_id FROM reconciliation_sources WHERE id").
		WillReturnRows(sqlmock.NewRows([]string{"context_id"}).
			AddRow("not-a-valid-uuid"))
	mock.ExpectRollback()

	result, err := repo.GetContextIDBySourceID(ctx, sourceID)

	require.Error(t, err)
	assert.Equal(t, uuid.Nil, result)
	assert.Contains(t, err.Error(), "invalid UUID")
}

func TestGetContextIDBySourceID_NilUUIDContextID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplicaForLookup(t)
	defer cleanup()

	sourceID := uuid.New()

	// uuid.Nil.String() = "00000000-0000-0000-0000-000000000000" which IS
	// a valid UUID parse, but returns uuid.Nil. Test that the function
	// still returns it faithfully (domain validation is caller's job).
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT context_id FROM reconciliation_sources WHERE id").
		WillReturnRows(sqlmock.NewRows([]string{"context_id"}).
			AddRow(uuid.Nil.String()))
	mock.ExpectCommit()

	result, err := repo.GetContextIDBySourceID(ctx, sourceID)

	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

// ---- FindByID additional edge cases ----
// These supplement the thorough FindByID tests in source_sqlmock_test.go.

func TestFindByID_ScanError_InvalidSourceType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplicaForLookup(t)
	defer cleanup()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
		}).AddRow(
			id.String(), contextID.String(), "Test", "INVALID_TYPE", "LEFT", configJSON, now, now,
		))
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, contextID, id)

	require.Error(t, err)
	require.Nil(t, result)
	// The error wraps the scan/parse error through "find reconciliation source by id"
	assert.Contains(t, err.Error(), "find reconciliation source by id")
}
