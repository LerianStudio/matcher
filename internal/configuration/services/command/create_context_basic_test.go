// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestCreateContext_Command(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tenantID  uuid.UUID
		input     entities.CreateReconciliationContextInput
		createErr error
		wantErr   error
	}{
		{
			name:     "valid input",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "Ledger vs Bank",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		},
		{
			name:     "empty name",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
			wantErr: entities.ErrContextNameRequired,
		},
		{
			name:     "invalid type",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "Invalid",
				Type:     shared.ContextType("INVALID"),
				Interval: "0 0 * * *",
			},
			wantErr: entities.ErrContextTypeInvalid,
		},
		{
			name:     "empty interval",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "No Interval",
				Type:     shared.ContextTypeOneToOne,
				Interval: "",
			},
			wantErr: entities.ErrContextIntervalRequired,
		},
		{
			name:     "nil tenant",
			tenantID: uuid.Nil,
			input: entities.CreateReconciliationContextInput{
				Name:     "Tenant Missing",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
			wantErr: entities.ErrContextTenantRequired,
		},
		{
			name:     "repository error",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "Repo Error",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
			createErr: errCreateFailed,
			wantErr:   errCreateFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &contextRepoStub{
				findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
					return nil, nil // No duplicate
				},
				createFn: func(ctx context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
					if tt.createErr != nil {
						return nil, tt.createErr
					}

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

			contextEntity, err := useCase.CreateContext(context.Background(), tt.tenantID, tt.input)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr, "expected error %v, got %v", tt.wantErr, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.tenantID, contextEntity.TenantID)
			assert.Equal(t, tt.input.Name, contextEntity.Name)
		})
	}
}

func TestCreateContext_InlineCreateWithoutInfrastructureProvider(t *testing.T) {
	t.Parallel()

	createCalled := false
	repo := &contextRepoStub{
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			createCalled = true
			return entity, nil
		},
	}

	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Inline Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{Name: "Bank", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft},
		},
	})
	require.ErrorIs(t, err, ErrInlineCreateRequiresInfrastructure)
	assert.False(t, createCalled, "contextRepo.Create must not run when inline create is invalid")
}

func TestCreateContext_WithInfrastructureProviderAndNoInlineResources_UsesRegularCreate(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	createCalled := 0

	repo := &contextRepoStub{
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			createCalled++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		repo,
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	created, err := useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Regular Create",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, 1, createCalled)
}
