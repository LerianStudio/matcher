// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestCreateContext_DuplicateName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		findByNameFn func(context.Context, string) (*entities.ReconciliationContext, error)
		wantErr      error
		wantContains string
	}{
		{
			name: "duplicate name returns existing entity",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return &entities.ReconciliationContext{
					ID:   uuid.New(),
					Name: "Ledger vs Bank",
				}, nil
			},
			wantErr: ErrContextNameAlreadyExists,
		},
		{
			name: "FindByName transient error propagates",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, errors.New("connection refused")
			},
			wantContains: "checking context name uniqueness",
		},
		{
			name: "FindByName returns sql.ErrNoRows allows creation",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, sql.ErrNoRows
			},
		},
		{
			name: "FindByName returns nil nil allows creation",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &contextRepoStub{
				findByNameFn: tt.findByNameFn,
				createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
					return entity, nil
				},
			}
			useCase, err := NewUseCase(
				repo,
				&sourceRepoStub{},
				&fieldMapRepoStub{},
				&matchRuleRepoStub{},
			)
			require.NoError(t, err)

			result, err := useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
				Name:     "Ledger vs Bank",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			})

			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			if tt.wantContains != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantContains)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "Ledger vs Bank", result.Name)
		})
	}
}

func TestUpdateContext_RenameToDuplicateName(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	otherEntity := &entities.ReconciliationContext{
		ID:   uuid.New(), // Different ID → name collision
		Name: "Taken Name",
	}

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return otherEntity, nil
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	newName := "Taken Name"
	_, err = useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextNameAlreadyExists)
}

func TestUpdateContext_RenameToSameName(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	// FindByName returns the same entity (same ID) — not a collision.
	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return existing, nil // Same ID → allowed
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	// Different name string but FindByName returns the same entity → no error.
	newName := "Renamed"
	updated, err := useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, updated.Name)
}

func TestUpdateContext_NameUnchanged(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	findByNameCalled := false

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			findByNameCalled = true

			return nil, nil
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	// input.Name equals entity.Name → uniqueness check should be skipped.
	sameName := "Original"
	updated, err := useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &sameName},
	)
	require.NoError(t, err)
	assert.Equal(t, "Original", updated.Name)
	assert.False(t, findByNameCalled, "FindByName should NOT be called when name is unchanged")
}

func TestUpdateContext_FindByNameTransientError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	transientErr := errors.New("connection refused")

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, transientErr
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	newName := "New Name"
	_, err = useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context name uniqueness")
	require.ErrorIs(t, err, transientErr)
}
