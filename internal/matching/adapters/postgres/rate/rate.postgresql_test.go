//go:build unit

package rate

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewRepository(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.Equal(t, provider, repo.provider)
}

func TestNewRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	require.NotNil(t, repo)
	assert.Nil(t, repo.provider)
}

func TestRepository_GetByID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()
	id := uuid.New()

	result, err := repo.GetByID(ctx, id)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_GetByID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	id := uuid.New()

	result, err := repo.GetByID(ctx, id)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_GetByID_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		repo        *Repository
		wantErr     bool
		errIs       error
		errContains string
	}{
		{
			name:    "nil repository returns ErrRepoNotInitialized",
			repo:    nil,
			wantErr: true,
			errIs:   ErrRepoNotInitialized,
		},
		{
			name:    "nil provider returns ErrRepoNotInitialized",
			repo:    &Repository{provider: nil},
			wantErr: true,
			errIs:   ErrRepoNotInitialized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			id := uuid.New()

			result, err := tt.repo.GetByID(ctx, id)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}

				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

func TestColumns_Constant(t *testing.T) {
	t.Parallel()

	expected := "id, currency, structure_type, structure, created_at, updated_at"
	assert.Equal(t, expected, columns)
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	assert.NotNil(t, repo)
}
