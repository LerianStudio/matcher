//go:build unit

package entities_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func createValidMatchGroup(
	ctx context.Context,
	t *testing.T,
	confidenceValue int,
) *entities.MatchGroup {
	t.Helper()

	contextID := uuid.New()
	runID := uuid.New()
	ruleID := uuid.New()
	confidence, err := value_objects.ParseConfidenceScore(confidenceValue)
	require.NoError(t, err)

	amount := decimal.RequireFromString("10")
	itemA, err := entities.NewMatchItem(ctx, uuid.New(), amount, "USD", amount)
	require.NoError(t, err)
	itemB, err := entities.NewMatchItem(ctx, uuid.New(), amount, "USD", amount)
	require.NoError(t, err)

	group, err := entities.NewMatchGroup(
		ctx,
		contextID,
		runID,
		ruleID,
		confidence,
		[]*entities.MatchItem{itemA, itemB},
	)
	require.NoError(t, err)

	return group
}

func TestMatchGroup_New_SetsMatchGroupIDOnItems(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 95)

	require.NotEqual(t, uuid.Nil, group.ID)

	for _, it := range group.Items {
		require.Equal(t, group.ID, it.MatchGroupID)
	}
}

func TestMatchGroup_Confirm_TransitionsStatusAndSetsConfirmedAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 95)

	require.Equal(t, value_objects.MatchGroupStatusProposed, group.Status)
	require.Nil(t, group.ConfirmedAt)

	err := group.Confirm(ctx)
	require.NoError(t, err)

	require.Equal(t, value_objects.MatchGroupStatusConfirmed, group.Status)
	require.NotNil(t, group.ConfirmedAt)
}

func TestMatchGroup_Confirm_ErrorOnAlreadyConfirmed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 95)

	require.NoError(t, group.Confirm(ctx))

	err := group.Confirm(ctx)
	require.ErrorIs(t, err, entities.ErrMatchGroupMustBeProposedToConfirm)
}

func TestMatchGroup_Confirm_ErrorOnRejectedGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 70)

	require.NoError(t, group.Reject(ctx, "duplicate match"))

	err := group.Confirm(ctx)
	require.ErrorIs(t, err, entities.ErrMatchGroupMustBeProposedToConfirm)
}

func TestMatchGroup_Reject_TransitionsStatusAndSetsReason(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 70)

	require.Equal(t, value_objects.MatchGroupStatusProposed, group.Status)
	require.Nil(t, group.RejectedReason)

	reason := "duplicate match detected"
	err := group.Reject(ctx, reason)
	require.NoError(t, err)

	require.Equal(t, value_objects.MatchGroupStatusRejected, group.Status)
	require.NotNil(t, group.RejectedReason)
	require.Equal(t, reason, *group.RejectedReason)
}

func TestMatchGroup_Reject_ErrorOnAlreadyRejected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 70)

	require.NoError(t, group.Reject(ctx, "first reason"))

	err := group.Reject(ctx, "second reason")
	require.ErrorIs(t, err, entities.ErrMatchGroupMustBeProposedToReject)
}

func TestMatchGroup_Reject_ErrorOnConfirmedGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 95)

	require.NoError(t, group.Confirm(ctx))

	err := group.Reject(ctx, "incorrect match discovered after confirmation")
	require.ErrorIs(t, err, entities.ErrMatchGroupMustBeProposedToReject)
}

func TestMatchGroup_Revoke_TransitionsStatusAndSetsReason(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 95)

	require.NoError(t, group.Confirm(ctx))
	require.Equal(t, value_objects.MatchGroupStatusConfirmed, group.Status)
	require.NotNil(t, group.ConfirmedAt)

	reason := "incorrect match discovered after confirmation"
	err := group.Revoke(ctx, reason)
	require.NoError(t, err)

	require.Equal(t, value_objects.MatchGroupStatusRevoked, group.Status)
	require.NotNil(t, group.ConfirmedAt, "ConfirmedAt must be preserved after revocation for audit trail")
	require.NotNil(t, group.RejectedReason)
	require.Equal(t, reason, *group.RejectedReason)
}

func TestMatchGroup_Revoke_ErrorOnProposedGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 70)

	require.Equal(t, value_objects.MatchGroupStatusProposed, group.Status)

	err := group.Revoke(ctx, "should not work on proposed")
	require.ErrorIs(t, err, entities.ErrMatchGroupMustBeConfirmedToRevoke)
}

func TestMatchGroup_Revoke_ErrorOnRejectedGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 70)

	require.NoError(t, group.Reject(ctx, "first rejection"))

	err := group.Revoke(ctx, "should not work on rejected")
	require.ErrorIs(t, err, entities.ErrMatchGroupMustBeConfirmedToRevoke)
}

func TestMatchGroup_Revoke_ErrorOnEmptyReason(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 95)

	require.NoError(t, group.Confirm(ctx))

	err := group.Revoke(ctx, "")
	require.ErrorIs(t, err, entities.ErrMatchGroupRevocationReasonRequired)
}

func TestMatchGroup_Revoke_NilReceiver(t *testing.T) {
	t.Parallel()

	group := (*entities.MatchGroup)(nil)

	err := group.Revoke(context.Background(), "reason")
	require.Error(t, err)
}

func TestMatchGroup_Reject_ErrorOnEmptyReason(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	group := createValidMatchGroup(ctx, t, 70)

	err := group.Reject(ctx, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rejection reason is required")
}

func TestMatchGroup_CanAutoConfirm_TrueFor90AndAbove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		confidence int
		expected   bool
	}{
		{60, false},
		{70, false},
		{89, false},
		{90, true},
		{95, true},
	}

	for _, tc := range tests {
		group := createValidMatchGroup(ctx, t, tc.confidence)
		require.Equal(t, tc.expected, group.CanAutoConfirm(), "confidence %d", tc.confidence)
	}
}

func TestMatchGroup_CanAutoConfirm_FalseForNilGroup(t *testing.T) {
	t.Parallel()

	var group *entities.MatchGroup
	require.False(t, group.CanAutoConfirm())
}
