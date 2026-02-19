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

func TestMatchRuleProvider_MockCreation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockMatchRuleProvider(ctrl)

	require.NotNil(t, mock)
	require.NotNil(t, mock.EXPECT())
}

func TestMatchRuleProvider_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockMatchRuleProvider(ctrl)

	var _ ports.MatchRuleProvider = mock
}

func TestMatchRuleProvider_ListByContextID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockMatchRuleProvider(ctrl)

	ctx := context.Background()
	contextID := uuid.New()

	expectedRules := shared.MatchRules{
		{ID: uuid.New(), ContextID: contextID, Priority: 1, Type: shared.RuleTypeExact},
		{ID: uuid.New(), ContextID: contextID, Priority: 2, Type: shared.RuleTypeTolerance},
	}

	mock.EXPECT().
		ListByContextID(ctx, contextID).
		Return(expectedRules, nil)

	rules, err := mock.ListByContextID(ctx, contextID)
	require.NoError(t, err)
	require.Len(t, rules, 2)
	require.Equal(t, expectedRules[0].ID, rules[0].ID)
	require.Equal(t, expectedRules[1].ID, rules[1].ID)
}

func TestMatchRuleProvider_ListByContextID_EmptyResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockMatchRuleProvider(ctrl)

	ctx := context.Background()
	contextID := uuid.New()

	mock.EXPECT().
		ListByContextID(ctx, contextID).
		Return(shared.MatchRules{}, nil)

	rules, err := mock.ListByContextID(ctx, contextID)
	require.NoError(t, err)
	require.Empty(t, rules)
}

func TestMatchRuleProvider_ListByContextID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockMatchRuleProvider(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	expectedErr := context.DeadlineExceeded

	mock.EXPECT().
		ListByContextID(ctx, contextID).
		Return(nil, expectedErr)

	rules, err := mock.ListByContextID(ctx, contextID)
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, rules)
}
