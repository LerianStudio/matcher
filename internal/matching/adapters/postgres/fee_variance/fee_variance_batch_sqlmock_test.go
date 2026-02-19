//go:build unit

package fee_variance

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestCreateBatch_ProviderError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("connection failed")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: connErr,
	}
	repo := NewRepository(provider)

	entities := []*matchingEntities.FeeVariance{
		createValidFeeVarianceEntity(),
	}

	result, err := repo.createBatch(context.Background(), nil, entities)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "create fee variance batch transaction")
}

func TestCreateBatch_WithExistingTx_PrepareError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	// Default tenant: ApplyTenantSchema skips SET LOCAL search_path.
	// The tx is caller-managed, so no commit/rollback expectations needed.
	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO match_fee_variances").
		WillReturnError(errors.New("prepare error"))

	tx, err := db.Begin()
	require.NoError(t, err)

	entities := []*matchingEntities.FeeVariance{
		createValidFeeVarianceEntity(),
	}

	result, err := repo.createBatch(context.Background(), tx, entities)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "create fee variance batch transaction")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateBatch_WithExistingTx_ExecError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO match_fee_variances").ExpectExec().
		WillReturnError(errors.New("insert error"))

	tx, err := db.Begin()
	require.NoError(t, err)

	entities := []*matchingEntities.FeeVariance{
		createValidFeeVarianceEntity(),
	}

	result, err := repo.createBatch(context.Background(), tx, entities)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "create fee variance batch transaction")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateBatch_WithExistingTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	entity := createValidFeeVarianceEntity()

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO match_fee_variances").
		ExpectExec().
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.createBatch(context.Background(), tx, []*matchingEntities.FeeVariance{entity})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateBatch_OnlyNilEntities(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO match_fee_variances")

	tx, err := db.Begin()
	require.NoError(t, err)

	entities := []*matchingEntities.FeeVariance{nil, nil, nil}

	result, err := repo.createBatch(context.Background(), tx, entities)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 3)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateBatchWithTx_NilTxConverts(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	// nil tx with valid entities should attempt provider tx
	entities := []*matchingEntities.FeeVariance{
		createValidFeeVarianceEntity(),
	}

	// This will fail because MockInfrastructureProvider doesn't have a real DB
	// but it tests the code path for nil tx conversion
	result, err := repo.CreateBatchWithTx(context.Background(), nil, entities)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestCreateBatchWithTx_ValidSqlTx(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	// createBatch with empty entities should return nil, nil
	result, err := repo.CreateBatchWithTx(context.Background(), tx, nil)

	require.NoError(t, err)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}
