// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/ports/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestReconciliationContextInfo_Instantiation(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	info := ports.ReconciliationContextInfo{
		ID:     id,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}

	require.Equal(t, id, info.ID)
	require.Equal(t, shared.ContextTypeOneToOne, info.Type)
	require.True(t, info.Active)
}

func TestReconciliationContextInfo_AllContextTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		contextType shared.ContextType
	}{
		{"OneToOne", shared.ContextTypeOneToOne},
		{"OneToMany", shared.ContextTypeOneToMany},
		{"ManyToMany", shared.ContextTypeManyToMany},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			info := ports.ReconciliationContextInfo{
				ID:     uuid.New(),
				Type:   tc.contextType,
				Active: true,
			}
			require.NotEqual(t, uuid.Nil, info.ID)
			require.True(t, info.Active)
			require.Equal(t, tc.contextType, info.Type)
		})
	}
}

func TestReconciliationContextInfo_InactiveContext(t *testing.T) {
	t.Parallel()

	info := ports.ReconciliationContextInfo{
		ID:     uuid.New(),
		Type:   shared.ContextTypeOneToOne,
		Active: false,
	}

	require.NotEqual(t, uuid.Nil, info.ID)
	require.Equal(t, shared.ContextTypeOneToOne, info.Type)
	require.False(t, info.Active)
}

func TestContextProvider_MockCreation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockContextProvider(ctrl)

	require.NotNil(t, mock)
	require.NotNil(t, mock.EXPECT())
}

func TestContextProvider_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockContextProvider(ctrl)

	var _ ports.ContextProvider = mock
}

func TestContextProvider_FindByID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockContextProvider(ctrl)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	expectedInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToMany,
		Active: true,
	}

	mock.EXPECT().
		FindByID(ctx, tenantID, contextID).
		Return(expectedInfo, nil)

	info, err := mock.FindByID(ctx, tenantID, contextID)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, contextID, info.ID)
	require.Equal(t, shared.ContextTypeOneToMany, info.Type)
	require.True(t, info.Active)
}

func TestContextProvider_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockContextProvider(ctrl)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mock.EXPECT().
		FindByID(ctx, tenantID, contextID).
		Return(nil, nil)

	info, err := mock.FindByID(ctx, tenantID, contextID)
	require.NoError(t, err)
	require.Nil(t, info)
}

func TestContextProvider_FindByID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockContextProvider(ctrl)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mock.EXPECT().
		FindByID(ctx, tenantID, contextID).
		Return(nil, context.DeadlineExceeded)

	info, err := mock.FindByID(ctx, tenantID, contextID)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, info)
}
