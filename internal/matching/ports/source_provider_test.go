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
)

func TestSourceType_Constants(t *testing.T) {
	t.Parallel()

	require.Equal(t, ports.SourceTypeLedger, ports.SourceType("LEDGER"))
	require.Equal(t, ports.SourceTypeFile, ports.SourceType("FILE"))
	require.Equal(t, ports.SourceTypeAPI, ports.SourceType("API"))
	require.Equal(t, ports.SourceTypeWebhook, ports.SourceType("WEBHOOK"))
}

func TestSourceType_StringValues(t *testing.T) {
	t.Parallel()

	require.Equal(t, "LEDGER", string(ports.SourceTypeLedger))
	require.Equal(t, "FILE", string(ports.SourceTypeFile))
	require.Equal(t, "API", string(ports.SourceTypeAPI))
	require.Equal(t, "WEBHOOK", string(ports.SourceTypeWebhook))
}

func TestSourceInfo_Instantiation(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	sourceInfo := ports.SourceInfo{
		ID:   id,
		Type: ports.SourceTypeLedger,
	}

	require.Equal(t, id, sourceInfo.ID)
	require.Equal(t, ports.SourceTypeLedger, sourceInfo.Type)
}

func TestSourceInfo_AllTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		sourceType ports.SourceType
	}{
		{"Ledger", ports.SourceTypeLedger},
		{"File", ports.SourceTypeFile},
		{"API", ports.SourceTypeAPI},
		{"Webhook", ports.SourceTypeWebhook},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sourceInfo := ports.SourceInfo{
				ID:   uuid.New(),
				Type: tc.sourceType,
			}
			require.Equal(t, tc.sourceType, sourceInfo.Type)
			require.NotEqual(t, uuid.Nil, sourceInfo.ID)
		})
	}
}

func TestSourceProvider_MockCreation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockSourceProvider(ctrl)

	require.NotNil(t, mock)
	require.NotNil(t, mock.EXPECT())
}

func TestSourceProvider_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockSourceProvider(ctrl)

	var _ ports.SourceProvider = mock
}

func TestSourceProvider_FindByContextID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockSourceProvider(ctrl)

	ctx := context.Background()
	contextID := uuid.New()

	expectedSources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger},
		{ID: uuid.New(), Type: ports.SourceTypeFile},
	}

	mock.EXPECT().
		FindByContextID(ctx, contextID).
		Return(expectedSources, nil)

	sources, err := mock.FindByContextID(ctx, contextID)
	require.NoError(t, err)
	require.Len(t, sources, 2)
	require.Equal(t, ports.SourceTypeLedger, sources[0].Type)
	require.Equal(t, ports.SourceTypeFile, sources[1].Type)
}

func TestSourceProvider_FindByContextID_Empty(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockSourceProvider(ctrl)

	ctx := context.Background()
	contextID := uuid.New()

	mock.EXPECT().
		FindByContextID(ctx, contextID).
		Return([]*ports.SourceInfo{}, nil)

	sources, err := mock.FindByContextID(ctx, contextID)
	require.NoError(t, err)
	require.Empty(t, sources)
}

func TestSourceProvider_FindByContextID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockSourceProvider(ctrl)

	ctx := context.Background()
	contextID := uuid.New()

	mock.EXPECT().
		FindByContextID(ctx, contextID).
		Return(nil, context.DeadlineExceeded)

	sources, err := mock.FindByContextID(ctx, contextID)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, sources)
}
