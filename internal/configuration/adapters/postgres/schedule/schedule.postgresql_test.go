//go:build unit

package schedule

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

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestRepository_Create_NilRepo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.Create(ctx, createValidScheduleEntity())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.Create(ctx, createValidScheduleEntity())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_Create_NilEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.Create(ctx, nil)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrScheduleEntityRequired)
}

func TestRepository_FindByID_NilRepo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByID(ctx, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindByID(ctx, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindByContextID_NilRepo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByContextID(ctx, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindByContextID(ctx, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindDueSchedules_NilRepo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindDueSchedules(ctx, now)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindDueSchedules(ctx, now)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_Update_NilRepo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.Update(ctx, createValidScheduleEntity())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.Update(ctx, createValidScheduleEntity())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_Update_NilEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.Update(ctx, nil)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrScheduleEntityRequired)
}

func TestRepository_Delete_NilRepo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.Delete(ctx, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.Delete(ctx, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_ProviderConnectionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connectionErr := errors.New("connection failed")

	t.Run("Create returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.Create(ctx, createValidScheduleEntity())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindByID returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.FindByID(ctx, uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindByContextID returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.FindByContextID(ctx, uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindDueSchedules returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.FindDueSchedules(ctx, time.Now().UTC())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("Update returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.Update(ctx, createValidScheduleEntity())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("Delete returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		err := repo.Delete(ctx, uuid.New())

		require.Error(t, err)
		require.ErrorIs(t, err, connectionErr)
	})
}

// setupMockWithReplica creates a test repository with sqlmock including replica for full coverage.
func setupMockWithReplica(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo := NewRepository(provider)

	return repo, mock, func() { db.Close() }
}

func createValidScheduleEntity() *entities.ReconciliationSchedule {
	now := time.Now().UTC()

	return &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func TestRepository_Create_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	entity := createValidScheduleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_schedules").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := repo.Create(ctx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.Equal(t, entity.ContextID, result.ContextID)
	require.Equal(t, entity.CronExpression, result.CronExpression)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Create_InsertErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	entity := createValidScheduleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_schedules").
		WillReturnError(errors.New("duplicate key"))
	mock.ExpectRollback()

	result, err := repo.Create(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "create schedule")
}

func TestRepository_FindByID_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at",
	}).AddRow(
		id.String(), contextID.String(), "0 6 * * *", true, nil, nil, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_schedules WHERE id").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindByID(ctx, id)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)
	require.Equal(t, contextID, result.ContextID)
	require.Equal(t, "0 6 * * *", result.CronExpression)
	require.True(t, result.Enabled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByID_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_schedules WHERE id").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, id)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_FindByContextID_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	contextID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()
	now := time.Now().UTC()
	lastRun := now.Add(-1 * time.Hour)
	nextRun := now.Add(1 * time.Hour)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at",
	}).AddRow(
		id1.String(), contextID.String(), "0 0 * * *", true, &lastRun, &nextRun, now, now,
	).AddRow(
		id2.String(), contextID.String(), "0 12 * * *", false, nil, nil, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_schedules WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, id1, result[0].ID)
	assert.Equal(t, id2, result[1].ID)
	assert.True(t, result[0].Enabled)
	assert.False(t, result[1].Enabled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextID_EmptyWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at",
	})

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_schedules WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindByContextID(ctx, uuid.New())

	require.NoError(t, err)
	require.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextID_QueryErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_schedules WHERE context_id").
		WillReturnError(errors.New("query failed"))
	mock.ExpectRollback()

	result, err := repo.FindByContextID(ctx, uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "find schedules by context")
}

func TestRepository_FindDueSchedules_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	tenantID := uuid.New()
	lastRun := now.Add(-1 * time.Hour)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at", "tenant_id",
	}).AddRow(
		id.String(), contextID.String(), "0 0 * * *", true, &lastRun, &now, now, now, tenantID.String(),
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_schedules s JOIN reconciliation_contexts c").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindDueSchedules(ctx, now)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, id, result[0].ID)
	assert.Equal(t, tenantID, result[0].TenantID)
	assert.True(t, result[0].Enabled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindDueSchedules_EmptyWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at", "tenant_id",
	})

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_schedules s JOIN reconciliation_contexts c").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindDueSchedules(ctx, time.Now().UTC())

	require.NoError(t, err)
	require.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Update_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	entity := createValidScheduleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_schedules").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := repo.Update(ctx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Update_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	entity := createValidScheduleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_schedules").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Update_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	entity := createValidScheduleEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_schedules").
		WillReturnError(errors.New("update failed"))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "update schedule")
}

func TestRepository_Delete_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_schedules").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(ctx, id)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Delete_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_schedules").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(ctx, id)

	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Delete_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)

	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_schedules").
		WillReturnError(errors.New("foreign key constraint"))
	mock.ExpectRollback()

	err := repo.Delete(ctx, id)

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete schedule")
}

func TestScanSchedule_ValidRow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()
	lastRun := now.Add(-1 * time.Hour)
	nextRun := now.Add(1 * time.Hour)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at",
	}).AddRow(
		id.String(), contextID.String(), "0 0 * * *", true, &lastRun, &nextRun, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSchedule(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, "0 0 * * *", result.CronExpression)
	assert.True(t, result.Enabled)
	require.NotNil(t, result.LastRunAt)
	require.NotNil(t, result.NextRunAt)
}

func TestScanSchedule_NilOptionalFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at",
	}).AddRow(
		id.String(), contextID.String(), "*/30 * * * *", false, nil, nil, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSchedule(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Nil(t, result.LastRunAt)
	assert.Nil(t, result.NextRunAt)
	assert.False(t, result.Enabled)
}

func TestScanSchedule_InvalidID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid", uuid.New().String(), "0 0 * * *", true, nil, nil, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSchedule(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

func TestScanSchedule_InvalidContextID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(), "not-a-uuid", "0 0 * * *", true, nil, nil, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSchedule(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

func TestScanDueSchedule_ValidRow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	contextID := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()
	lastRun := now.Add(-1 * time.Hour)
	nextRun := now.Add(1 * time.Hour)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at", "tenant_id",
	}).AddRow(
		id.String(), contextID.String(), "0 0 * * *", true, &lastRun, &nextRun, now, now, tenantID.String(),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, scanErr := scanDueSchedule(sqlRows)
	require.NoError(t, scanErr)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Equal(t, "0 0 * * *", result.CronExpression)
	assert.True(t, result.Enabled)
	require.NotNil(t, result.LastRunAt)
	require.NotNil(t, result.NextRunAt)
}

func TestScanDueSchedule_InvalidTenantID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "cron_expression", "enabled", "last_run_at", "next_run_at", "created_at", "updated_at", "tenant_id",
	}).AddRow(
		uuid.New().String(), uuid.New().String(), "0 0 * * *", true, nil, nil, now, now, "not-a-valid-uuid",
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, scanErr := scanDueSchedule(sqlRows)
	require.Error(t, scanErr)
	require.Nil(t, result)
	require.Contains(t, scanErr.Error(), "invalid UUID")
}
