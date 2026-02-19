//go:build unit

package rate

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestScan_FlatFee_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "currency", "structure_type", "structure", "created_at", "updated_at"}).
		AddRow(id.String(), "USD", string(fee.FeeStructureFlat), []byte(`{"amount":"10.50"}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scan(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, "USD", result.Currency)
	assert.Equal(t, fee.FeeStructureFlat, result.Structure.Type())
}

func TestScan_PercentageFee_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "currency", "structure_type", "structure", "created_at", "updated_at"}).
		AddRow(id.String(), "EUR", string(fee.FeeStructurePercentage), []byte(`{"rate":"0.025"}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scan(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, "EUR", result.Currency)
	assert.Equal(t, fee.FeeStructurePercentage, result.Structure.Type())
}

func TestScan_TieredFee_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "currency", "structure_type", "structure", "created_at", "updated_at"}).
		AddRow(id.String(), "BRL", string(fee.FeeStructureTiered), []byte(`{"tiers":[{"up_to":"100","rate":"0.01"},{"rate":"0.02"}]}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scan(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, "BRL", result.Currency)
	assert.Equal(t, fee.FeeStructureTiered, result.Structure.Type())
}

func TestScan_ScanError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	// Only provide 1 column to force scan error
	rows := sqlmock.NewRows([]string{"id"}).AddRow("partial-data")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scan(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestScan_InvalidID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "currency", "structure_type", "structure", "created_at", "updated_at"}).
		AddRow("not-a-uuid", "USD", string(fee.FeeStructureFlat), []byte(`{"amount":"10"}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scan(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse id")
}

func TestScan_UnknownStructureType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "currency", "structure_type", "structure", "created_at", "updated_at"}).
		AddRow(id.String(), "USD", "UNKNOWN", []byte(`{}`), now, now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scan(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "unknown fee structure type")
}

func TestRepository_GetByID_ConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("connection failed")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: connErr,
	}
	repo := NewRepository(provider)

	result, err := repo.GetByID(context.Background(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_GetByID_NotFound(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	// Default tenant: ApplyTenantSchema skips SET LOCAL search_path.
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT (.+) FROM rates").
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	result, err := repo.GetByID(context.Background(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRateNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}
