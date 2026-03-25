//go:build unit

package fee_variance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider *testutil.MockInfrastructureProvider
	}{
		{
			name:     "creates repository with provider",
			provider: &testutil.MockInfrastructureProvider{},
		},
		{
			name:     "creates repository with nil provider",
			provider: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := NewRepository(tt.provider)

			require.NotNil(t, repo)
		})
	}
}

func TestCreateBatchWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.createBatch(context.Background(), nil, nil)

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

func TestCreateBatchWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.createBatch(context.Background(), nil, nil)

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

func TestCreateBatchWithTx_EmptyRows(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.CreateBatchWithTx(context.Background(), nil, nil)

	require.ErrorIs(t, err, ErrInvalidTx)
	assert.Nil(t, result)

	result, err = repo.CreateBatchWithTx(
		context.Background(),
		nil,
		[]*matchingEntities.FeeVariance{},
	)

	require.ErrorIs(t, err, ErrInvalidTx)
	assert.Nil(t, result)
}

func TestCreateBatchWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.CreateBatchWithTx(context.Background(), nil, nil)

	require.ErrorIs(t, err, ErrInvalidTx)
	assert.Nil(t, result)
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	var _ matchingRepos.FeeVarianceRepository = repo

	assert.NotNil(t, repo)
}

func TestNewRepository_WithValidProvider(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.NotNil(t, repo.provider)
}

func TestCreateBatchWithTx_WithValidEntities(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	entities := []*matchingEntities.FeeVariance{
		createValidFeeVarianceEntity(),
		createValidFeeVarianceEntity(),
	}

	result, err := repo.CreateBatchWithTx(context.Background(), nil, entities)

	assert.Nil(t, result)
	require.Error(t, err)
}

func TestCreateBatchWithTx_WithNilEntitiesInSlice(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	entities := []*matchingEntities.FeeVariance{
		nil,
		createValidFeeVarianceEntity(),
		nil,
	}

	result, err := repo.CreateBatchWithTx(context.Background(), nil, entities)

	assert.Nil(t, result)
	require.Error(t, err)
}

func TestCreateBatch_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.createBatch(context.Background(), nil, []*matchingEntities.FeeVariance{
		createValidFeeVarianceEntity(),
	})

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}
