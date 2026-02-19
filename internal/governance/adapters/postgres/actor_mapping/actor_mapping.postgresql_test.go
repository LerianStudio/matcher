//go:build unit

package actormapping

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestDatabaseError    = errors.New("database error")
	errTestConnectionFailed = errors.New("connection failed")
)

var actorMappingTestColumns = []string{
	"actor_id", "display_name", "email", "created_at", "updated_at",
}

func contextWithTenant() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
}

func setupMockRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func TestRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	err := repo.Upsert(ctx, &entities.ActorMapping{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetByActorID(ctx, "actor-1")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	err = repo.Pseudonymize(ctx, "actor-1")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	err = repo.Delete(ctx, "actor-1")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestUpsert_NilMapping(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&testutil.MockInfrastructureProvider{})
	ctx := contextWithTenant()

	err := repo.Upsert(ctx, nil)
	require.ErrorIs(t, err, ErrActorMappingRequired)
}

func TestUpsert_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	now := time.Now().UTC()
	displayName := "John Doe"
	email := "john@example.com"

	mapping := &entities.ActorMapping{
		ActorID:     "actor-123",
		DisplayName: &displayName,
		Email:       &email,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO actor_mapping (actor_id,display_name,email,created_at,updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (actor_id) DO UPDATE SET display_name = EXCLUDED.display_name, email = EXCLUDED.email, updated_at = EXCLUDED.updated_at`,
	)).
		WithArgs("actor-123", &displayName, &email, now, now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Upsert(ctx, mapping)
	require.NoError(t, err)
}

func TestUpsert_NilOptionalFields(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	now := time.Now().UTC()

	mapping := &entities.ActorMapping{
		ActorID:   "actor-456",
		CreatedAt: now,
		UpdatedAt: now,
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO actor_mapping (actor_id,display_name,email,created_at,updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (actor_id) DO UPDATE SET display_name = EXCLUDED.display_name, email = EXCLUDED.email, updated_at = EXCLUDED.updated_at`,
	)).
		WithArgs("actor-456", nil, nil, now, now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Upsert(ctx, mapping)
	require.NoError(t, err)
}

func TestUpsert_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	now := time.Now().UTC()

	mapping := &entities.ActorMapping{
		ActorID:   "actor-err",
		CreatedAt: now,
		UpdatedAt: now,
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO actor_mapping (actor_id,display_name,email,created_at,updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (actor_id) DO UPDATE SET display_name = EXCLUDED.display_name, email = EXCLUDED.email, updated_at = EXCLUDED.updated_at`,
	)).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	err := repo.Upsert(ctx, mapping)
	require.Error(t, err)
	require.Contains(t, err.Error(), "upsert actor mapping")
}

func TestUpsertWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := contextWithTenant()

	err := repo.UpsertWithTx(ctx, nil, &entities.ActorMapping{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestUpsertWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := contextWithTenant()

	err := repo.UpsertWithTx(ctx, nil, &entities.ActorMapping{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestUpsertWithTx_NilMapping(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&testutil.MockInfrastructureProvider{})
	ctx := contextWithTenant()

	err := repo.UpsertWithTx(ctx, nil, nil)
	require.ErrorIs(t, err, ErrActorMappingRequired)
}

func TestUpsertWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	now := time.Now().UTC()
	displayName := "John Doe"
	email := "john@example.com"

	mapping := &entities.ActorMapping{
		ActorID:     "actor-tx-123",
		DisplayName: &displayName,
		Email:       &email,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO actor_mapping (actor_id,display_name,email,created_at,updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (actor_id) DO UPDATE SET display_name = EXCLUDED.display_name, email = EXCLUDED.email, updated_at = EXCLUDED.updated_at`,
	)).
		WithArgs("actor-tx-123", &displayName, &email, now, now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Pass nil tx so internal method creates its own transaction.
	err := repo.UpsertWithTx(ctx, nil, mapping)
	require.NoError(t, err)
}

func TestGetByActorID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	now := time.Now().UTC()
	displayName := "Jane Doe"
	email := "jane@example.com"

	query := regexp.QuoteMeta(
		`SELECT actor_id, display_name, email, created_at, updated_at FROM actor_mapping WHERE actor_id = $1`,
	)

	rows := sqlmock.NewRows(actorMappingTestColumns).
		AddRow("actor-789", &displayName, &email, now, now)

	mock.ExpectQuery(query).WithArgs("actor-789").WillReturnRows(rows)

	result, err := repo.GetByActorID(ctx, "actor-789")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "actor-789", result.ActorID)
	require.NotNil(t, result.DisplayName)
	require.Equal(t, "Jane Doe", *result.DisplayName)
	require.NotNil(t, result.Email)
	require.Equal(t, "jane@example.com", *result.Email)
	require.Equal(t, now, result.CreatedAt)
	require.Equal(t, now, result.UpdatedAt)
}

func TestGetByActorID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	query := regexp.QuoteMeta(
		`SELECT actor_id, display_name, email, created_at, updated_at FROM actor_mapping WHERE actor_id = $1`,
	)

	mock.ExpectQuery(query).WithArgs("nonexistent").WillReturnError(sql.ErrNoRows)

	_, err := repo.GetByActorID(ctx, "nonexistent")
	require.ErrorIs(t, err, ErrActorMappingNotFound)
}

func TestGetByActorID_EmptyActorID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&testutil.MockInfrastructureProvider{})
	ctx := contextWithTenant()

	_, err := repo.GetByActorID(ctx, "")
	require.ErrorIs(t, err, ErrActorIDRequired)
}

func TestGetByActorID_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	query := regexp.QuoteMeta(
		`SELECT actor_id, display_name, email, created_at, updated_at FROM actor_mapping WHERE actor_id = $1`,
	)

	mock.ExpectQuery(query).WithArgs("actor-err").WillReturnError(errTestConnectionFailed)

	_, err := repo.GetByActorID(ctx, "actor-err")
	require.Error(t, err)
	require.Contains(t, err.Error(), "get actor mapping by id")
}

func TestPseudonymize_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE actor_mapping SET display_name = $1, email = $2, updated_at = $3 WHERE actor_id = $4`,
	)).
		WithArgs("[REDACTED]", "[REDACTED]", sqlmock.AnyArg(), "actor-pseudo").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Pseudonymize(ctx, "actor-pseudo")
	require.NoError(t, err)
}

func TestPseudonymize_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE actor_mapping SET display_name = $1, email = $2, updated_at = $3 WHERE actor_id = $4`,
	)).
		WithArgs("[REDACTED]", "[REDACTED]", sqlmock.AnyArg(), "nonexistent").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Pseudonymize(ctx, "nonexistent")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrActorMappingNotFound)
}

func TestPseudonymize_EmptyActorID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&testutil.MockInfrastructureProvider{})
	ctx := contextWithTenant()

	err := repo.Pseudonymize(ctx, "")
	require.ErrorIs(t, err, ErrActorIDRequired)
}

func TestPseudonymize_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE actor_mapping SET display_name = $1, email = $2, updated_at = $3 WHERE actor_id = $4`,
	)).
		WithArgs("[REDACTED]", "[REDACTED]", sqlmock.AnyArg(), "actor-err").
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	err := repo.Pseudonymize(ctx, "actor-err")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pseudonymize actor mapping")
}

func TestDelete_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`DELETE FROM actor_mapping WHERE actor_id = $1`,
	)).
		WithArgs("actor-del").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(ctx, "actor-del")
	require.NoError(t, err)
}

func TestDelete_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`DELETE FROM actor_mapping WHERE actor_id = $1`,
	)).
		WithArgs("nonexistent").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(ctx, "nonexistent")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrActorMappingNotFound)
}

func TestDelete_EmptyActorID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&testutil.MockInfrastructureProvider{})
	ctx := contextWithTenant()

	err := repo.Delete(ctx, "")
	require.ErrorIs(t, err, ErrActorIDRequired)
}

func TestDelete_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`DELETE FROM actor_mapping WHERE actor_id = $1`,
	)).
		WithArgs("actor-err").
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	err := repo.Delete(ctx, "actor-err")
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete actor mapping")
}

func TestDeleteWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := contextWithTenant()

	err := repo.DeleteWithTx(ctx, nil, "actor-1")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestDeleteWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := contextWithTenant()

	err := repo.DeleteWithTx(ctx, nil, "actor-1")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestDeleteWithTx_EmptyActorID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&testutil.MockInfrastructureProvider{})
	ctx := contextWithTenant()

	err := repo.DeleteWithTx(ctx, nil, "")
	require.ErrorIs(t, err, ErrActorIDRequired)
}

func TestDeleteWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`DELETE FROM actor_mapping WHERE actor_id = $1`,
	)).
		WithArgs("actor-tx-del").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Pass nil tx so internal method creates its own transaction.
	err := repo.DeleteWithTx(ctx, nil, "actor-tx-del")
	require.NoError(t, err)
}

func TestScanActorMapping_NilScanner(t *testing.T) {
	t.Parallel()

	_, err := scanActorMapping(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilScanner)
}

func TestScanActorMapping_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	displayName := "Test User"
	email := "test@example.com"

	mapping, err := scanActorMapping(fakeScanner{scan: func(dest ...any) error {
		if ptr, ok := dest[0].(*string); ok {
			*ptr = "actor-scan"
		}

		if ptr, ok := dest[1].(**string); ok {
			*ptr = &displayName
		}

		if ptr, ok := dest[2].(**string); ok {
			*ptr = &email
		}

		if ptr, ok := dest[3].(*time.Time); ok {
			*ptr = now
		}

		if ptr, ok := dest[4].(*time.Time); ok {
			*ptr = now
		}

		return nil
	}})

	require.NoError(t, err)
	require.NotNil(t, mapping)
	require.Equal(t, "actor-scan", mapping.ActorID)
	require.NotNil(t, mapping.DisplayName)
	require.Equal(t, "Test User", *mapping.DisplayName)
	require.NotNil(t, mapping.Email)
	require.Equal(t, "test@example.com", *mapping.Email)
	require.Equal(t, now, mapping.CreatedAt)
	require.Equal(t, now, mapping.UpdatedAt)
}

func TestScanActorMapping_Error(t *testing.T) {
	t.Parallel()

	_, err := scanActorMapping(fakeScanner{scan: func(_ ...any) error {
		return errTestDatabaseError
	}})

	require.Error(t, err)
	require.Contains(t, err.Error(), "scanning actor mapping")
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrActorMappingRequired", ErrActorMappingRequired},
		{"ErrActorIDRequired", ErrActorIDRequired},
		{"ErrActorMappingNotFound", ErrActorMappingNotFound},
		{"ErrNilScanner", ErrNilScanner},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

type fakeScanner struct {
	scan func(dest ...any) error
}

func (f fakeScanner) Scan(dest ...any) error {
	return f.scan(dest...)
}
