//go:build unit

package fee_schedule

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var errTestDatabase = errors.New("database error")

func setupMockRepo(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

func schedCols() []string {
	return []string{
		"id", "tenant_id", "name", "currency", "application_order",
		"rounding_scale", "rounding_mode", "created_at", "updated_at",
	}
}

func itemCols() []string {
	return []string{
		"id", "fee_schedule_id", "name", "priority",
		"structure_type", "structure_data", "created_at", "updated_at",
	}
}

func newTestSchedule() *fee.FeeSchedule {
	now := testutil.FixedTime()

	return &fee.FeeSchedule{
		ID:               testutil.DeterministicUUID("fee-schedule"),
		TenantID:         testutil.DeterministicUUID("fee-tenant"),
		Name:             "Test Schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        testutil.DeterministicUUID("fee-item"),
				Name:      "Flat Fee",
				Priority:  0,
				Structure: fee.FlatFee{Amount: decimal.RequireFromString("10.50")},
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// --- Create with sqlmock ---

func TestRepository_Create_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	schedule := newTestSchedule()
	model, items, err := FromEntity(schedule)
	require.NoError(t, err)

	insertScheduleQuery := regexp.QuoteMeta(
		`INSERT INTO fee_schedules (id, tenant_id, name, currency, application_order, rounding_scale, rounding_mode, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)

	insertItemQuery := regexp.QuoteMeta(
		`INSERT INTO fee_schedule_items (id, fee_schedule_id, name, priority, structure_type, structure_data, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)

	mock.ExpectBegin()
	mock.ExpectExec(insertScheduleQuery).
		WithArgs(
			model.ID, model.TenantID, model.Name, model.Currency,
			model.ApplicationOrder, model.RoundingScale, model.RoundingMode,
			model.CreatedAt, model.UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(insertItemQuery).
		WithArgs(
			items[0].ID, items[0].FeeScheduleID, items[0].Name, items[0].Priority,
			items[0].StructureType, items[0].StructureData,
			items[0].CreatedAt, items[0].UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := repo.Create(ctx, schedule)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schedule.ID, result.ID)
	assert.Equal(t, schedule.Name, result.Name)
	assert.Len(t, result.Items, 1)
}

func TestRepository_CreateWithTx_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)
	ctx := context.Background()
	schedule := newTestSchedule()
	model, items, err := FromEntity(schedule)
	require.NoError(t, err)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	mock.ExpectExec("INSERT INTO fee_schedules").
		WithArgs(
			model.ID, model.TenantID, model.Name, model.Currency,
			model.ApplicationOrder, model.RoundingScale, model.RoundingMode,
			model.CreatedAt, model.UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO fee_schedule_items").
		WithArgs(
			items[0].ID, items[0].FeeScheduleID, items[0].Name, items[0].Priority,
			items[0].StructureType, items[0].StructureData,
			items[0].CreatedAt, items[0].UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := repo.CreateWithTx(ctx, tx, schedule)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schedule.ID, result.ID)
}

func TestRepository_Create_InsertScheduleError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	schedule := newTestSchedule()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO fee_schedules").
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.Create(ctx, schedule)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "create fee schedule")
}

func TestRepository_Create_InsertItemError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	schedule := newTestSchedule()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO fee_schedules").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO fee_schedule_items").
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.Create(ctx, schedule)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "create fee schedule")
}

// --- GetByID with sqlmock ---

func TestRepository_GetByID_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	now := testutil.FixedTime()
	scheduleID := testutil.DeterministicUUID("getbyid-schedule")
	tenantID := testutil.DeterministicUUID("getbyid-tenant")
	itemID := testutil.DeterministicUUID("getbyid-item")

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules WHERE id").
		WithArgs(scheduleID.String()).
		WillReturnRows(sqlmock.NewRows(schedCols()).
			AddRow(scheduleID.String(), tenantID.String(), "Test", "USD", "PARALLEL", 2, "HALF_UP", now, now))
	mock.ExpectQuery("SELECT .+ FROM fee_schedule_items WHERE fee_schedule_id").
		WithArgs(scheduleID.String()).
		WillReturnRows(sqlmock.NewRows(itemCols()).
			AddRow(itemID.String(), scheduleID.String(), "Flat", 0, "FLAT", []byte(`{"amount":"5.00"}`), now, now))
	mock.ExpectCommit()

	result, err := repo.GetByID(ctx, scheduleID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, scheduleID, result.ID)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Len(t, result.Items, 1)
}

func TestRepository_GetByID_NotFound_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	scheduleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules WHERE id").
		WithArgs(scheduleID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.GetByID(ctx, scheduleID)

	require.ErrorIs(t, err, fee.ErrFeeScheduleNotFound)
	require.Nil(t, result)
}

func TestRepository_GetByID_QueryError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	scheduleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules WHERE id").
		WithArgs(scheduleID.String()).
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.GetByID(ctx, scheduleID)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "get fee schedule by id")
}

func TestRepository_GetByID_ItemQueryError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	now := testutil.FixedTime()
	scheduleID := testutil.DeterministicUUID("getbyid-item-err-schedule")
	tenantID := testutil.DeterministicUUID("getbyid-item-err-tenant")

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules WHERE id").
		WithArgs(scheduleID.String()).
		WillReturnRows(sqlmock.NewRows(schedCols()).
			AddRow(scheduleID.String(), tenantID.String(), "Test", "USD", "PARALLEL", 2, "HALF_UP", now, now))
	mock.ExpectQuery("SELECT .+ FROM fee_schedule_items WHERE fee_schedule_id").
		WithArgs(scheduleID.String()).
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.GetByID(ctx, scheduleID)

	require.Error(t, err)
	require.Nil(t, result)
}

// --- Delete with sqlmock ---

func TestRepository_Delete_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	scheduleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM fee_schedules WHERE id").
		WithArgs(scheduleID.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(ctx, scheduleID)

	require.NoError(t, err)
}

func TestRepository_Delete_NotFound_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	scheduleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM fee_schedules WHERE id").
		WithArgs(scheduleID.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(ctx, scheduleID)

	require.ErrorIs(t, err, fee.ErrFeeScheduleNotFound)
}

func TestRepository_Delete_ExecError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	scheduleID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM fee_schedules WHERE id").
		WithArgs(scheduleID.String()).
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	err := repo.Delete(ctx, scheduleID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete fee schedule")
}

// --- List with sqlmock ---

func TestRepository_List_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	now := testutil.FixedTime()
	scheduleID := testutil.DeterministicUUID("list-schedule")
	tenantID := testutil.DeterministicUUID("list-tenant")
	itemID := testutil.DeterministicUUID("list-item")

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules ORDER BY name LIMIT").
		WithArgs(10).
		WillReturnRows(sqlmock.NewRows(schedCols()).
			AddRow(scheduleID.String(), tenantID.String(), "Alpha", "USD", "PARALLEL", 2, "HALF_UP", now, now))
	mock.ExpectQuery("SELECT .+ FROM fee_schedule_items WHERE fee_schedule_id IN").
		WithArgs(scheduleID.String()).
		WillReturnRows(sqlmock.NewRows(itemCols()).
			AddRow(itemID.String(), scheduleID.String(), "Flat", 0, "FLAT", []byte(`{"amount":"1.00"}`), now, now))
	mock.ExpectCommit()

	result, err := repo.List(ctx, 10)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, scheduleID, result[0].ID)
	assert.Len(t, result[0].Items, 1)
}

func TestRepository_List_Empty_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules ORDER BY name LIMIT").
		WithArgs(100).
		WillReturnRows(sqlmock.NewRows(schedCols()))
	mock.ExpectCommit()

	result, err := repo.List(ctx, 0) // 0 defaults to 100

	require.NoError(t, err)
	require.Empty(t, result)
}

func TestRepository_List_QueryError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules ORDER BY name LIMIT").
		WithArgs(10).
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.List(ctx, 10)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "list fee schedules")
}

// --- GetByIDs with sqlmock ---

func TestRepository_GetByIDs_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	now := testutil.FixedTime()
	id1 := testutil.DeterministicUUID("getbyids-schedule-1")
	id2 := testutil.DeterministicUUID("getbyids-schedule-2")
	tenantID := testutil.DeterministicUUID("getbyids-tenant")

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules WHERE id IN").
		WithArgs(id1.String(), id2.String()).
		WillReturnRows(sqlmock.NewRows(schedCols()).
			AddRow(id1.String(), tenantID.String(), "Schedule A", "USD", "PARALLEL", 2, "HALF_UP", now, now).
			AddRow(id2.String(), tenantID.String(), "Schedule B", "EUR", "CASCADING", 4, "BANKERS", now, now))
	mock.ExpectQuery("SELECT .+ FROM fee_schedule_items WHERE fee_schedule_id IN").
		WithArgs(id1.String(), id2.String()).
		WillReturnRows(sqlmock.NewRows(itemCols()))
	mock.ExpectCommit()

	result, err := repo.GetByIDs(ctx, []uuid.UUID{id1, id2})

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.NotNil(t, result[id1])
	assert.NotNil(t, result[id2])
	assert.Equal(t, "Schedule A", result[id1].Name)
	assert.Equal(t, "Schedule B", result[id2].Name)
}

func TestRepository_GetByIDs_QueryError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	id1 := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM fee_schedules WHERE id IN").
		WithArgs(id1.String()).
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.GetByIDs(ctx, []uuid.UUID{id1})

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "get fee schedules by ids")
}

// --- Update with sqlmock ---

func TestRepository_Update_Success_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	schedule := newTestSchedule()
	model, items, err := FromEntity(schedule)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_schedules SET").
		WithArgs(
			model.Name, model.Currency, model.ApplicationOrder,
			model.RoundingScale, model.RoundingMode, model.UpdatedAt, model.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM fee_schedule_items WHERE fee_schedule_id").
		WithArgs(model.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO fee_schedule_items").
		WithArgs(
			items[0].ID, items[0].FeeScheduleID, items[0].Name, items[0].Priority,
			items[0].StructureType, items[0].StructureData,
			items[0].CreatedAt, items[0].UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := repo.Update(ctx, schedule)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schedule.ID, result.ID)
}

func TestRepository_Update_NotFound_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	schedule := newTestSchedule()
	model, _, err := FromEntity(schedule)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_schedules SET").
		WithArgs(
			model.Name, model.Currency, model.ApplicationOrder,
			model.RoundingScale, model.RoundingMode, model.UpdatedAt, model.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, schedule)

	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, fee.ErrFeeScheduleNotFound)
}

func TestRepository_Update_ExecError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	schedule := newTestSchedule()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_schedules SET").
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.Update(ctx, schedule)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "update fee schedule")
}

func TestRepository_Update_DeleteItemsError_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepo(t)
	defer finish()

	ctx := context.Background()
	schedule := newTestSchedule()
	model, _, err := FromEntity(schedule)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE fee_schedules SET").
		WithArgs(
			model.Name, model.Currency, model.ApplicationOrder,
			model.RoundingScale, model.RoundingMode, model.UpdatedAt, model.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM fee_schedule_items WHERE fee_schedule_id").
		WithArgs(model.ID).
		WillReturnError(errTestDatabase)
	mock.ExpectRollback()

	result, err := repo.Update(ctx, schedule)

	require.Error(t, err)
	require.Nil(t, result)
}
