//go:build unit

package source

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func TestNewRepository_NilConnection(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(nil)
	require.ErrorIs(t, err, ErrConnectionRequired)
}

func TestRepository_NilConnection(t *testing.T) {
	t.Parallel()

	repo := &Repository{}
	ctx := context.Background()

	_, err := repo.Create(ctx, &entities.ReconciliationSource{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.FindByID(ctx, uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, _, err = repo.FindByContextID(ctx, uuid.New(), "", 10)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, _, err = repo.FindByContextIDAndType(ctx, uuid.New(), value_objects.SourceTypeLedger, "", 10)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.Update(ctx, &entities.ReconciliationSource{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	err = repo.Delete(ctx, uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_NilEntity(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before entity check
	repo := &Repository{}
	_, err := repo.Create(context.Background(), nil)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.Update(context.Background(), nil)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepositorySentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrSourceEntityRequired", ErrSourceEntityRequired},
		{"ErrSourceEntityIDRequired", ErrSourceEntityIDRequired},
		{"ErrSourceModelRequired", ErrSourceModelRequired},
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
		{"ErrConnectionRequired", ErrConnectionRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}
