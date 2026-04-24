// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package context

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

func TestRepository_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.Create(ctx, &entities.ReconciliationContext{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.FindByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.FindByName(ctx, "test")
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, _, err = repo.FindAll(ctx, "", 10, nil, nil)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.Update(ctx, &entities.ReconciliationContext{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	err = repo.Delete(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.Count(ctx)
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
		{"ErrContextEntityRequired", ErrContextEntityRequired},
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}
