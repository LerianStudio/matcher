//go:build unit

package entities_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestMatchGroupConstraints(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	runID := uuid.New()
	ruleID := uuid.New()

	itemOne, itemTwo := newMatchItemPair(t, decimal.NewFromFloat(12.5))

	confidence, err := value_objects.ParseConfidenceScore(88)
	require.NoError(t, err)

	group, err := entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		ruleID,
		confidence,
		[]*entities.MatchItem{itemOne, itemTwo},
	)
	require.NoError(t, err)
	require.Equal(t, value_objects.MatchGroupStatusProposed, group.Status)
	require.Len(t, group.Items, 2)
	require.Equal(t, group.ID, group.Items[0].MatchGroupID)
	require.Equal(t, group.ID, group.Items[1].MatchGroupID)
	require.False(t, group.CanAutoConfirm())

	require.NoError(t, group.Confirm(context.Background()))
	require.Equal(t, value_objects.MatchGroupStatusConfirmed, group.Status)
	require.Error(t, group.Confirm(context.Background()))
	require.ErrorIs(t, group.Reject(context.Background(), "manual review"), entities.ErrMatchGroupMustBeProposedToReject)
	require.NoError(t, group.Revoke(context.Background(), "manual review"))

	lowItemOne, lowItemTwo := newMatchItemPair(t, decimal.NewFromFloat(12.5))
	lowConfidence, err := value_objects.ParseConfidenceScore(59)
	require.NoError(t, err)
	_, err = entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		ruleID,
		lowConfidence,
		[]*entities.MatchItem{lowItemOne, lowItemTwo},
	)
	require.Error(t, err)

	minItemOne, minItemTwo := newMatchItemPair(t, decimal.NewFromFloat(12.5))
	minConfidence, err := value_objects.ParseConfidenceScore(60)
	require.NoError(t, err)
	minGroup, err := entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		ruleID,
		minConfidence,
		[]*entities.MatchItem{minItemOne, minItemTwo},
	)
	require.NoError(t, err)
	require.Equal(t, value_objects.MatchGroupStatusProposed, minGroup.Status)
}

func TestMatchGroupValidation(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	runID := uuid.New()
	ruleID := uuid.New()

	confidence, err := value_objects.ParseConfidenceScore(60)
	require.NoError(t, err)

	_, err = entities.NewMatchGroup(context.Background(), contextID, runID, ruleID, confidence, nil)
	require.Error(t, err)

	singleItem := newMatchItem(t, decimal.NewFromInt(1))
	_, err = entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		ruleID,
		confidence,
		[]*entities.MatchItem{singleItem},
	)
	require.Error(t, err)

	invalidContextItemOne, invalidContextItemTwo := newMatchItemPair(t, decimal.NewFromInt(1))
	_, err = entities.NewMatchGroup(
		context.Background(),
		uuid.Nil,
		runID,
		ruleID,
		confidence,
		[]*entities.MatchItem{invalidContextItemOne, invalidContextItemTwo},
	)
	require.Error(t, err)

	invalidRunItemOne, invalidRunItemTwo := newMatchItemPair(t, decimal.NewFromInt(1))
	_, err = entities.NewMatchGroup(
		context.Background(),
		contextID,
		uuid.Nil,
		ruleID,
		confidence,
		[]*entities.MatchItem{invalidRunItemOne, invalidRunItemTwo},
	)
	require.Error(t, err)

	// uuid.Nil is valid for rule_id (manual matches have no rule).
	nilRuleItemOne, nilRuleItemTwo := newMatchItemPair(t, decimal.NewFromInt(1))
	nilRuleGroup, err := entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		uuid.Nil,
		confidence,
		[]*entities.MatchItem{nilRuleItemOne, nilRuleItemTwo},
	)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, nilRuleGroup.RuleID)

	invalidItemsItemOne, _ := newMatchItemPair(t, decimal.NewFromInt(1))
	_, err = entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		ruleID,
		confidence,
		[]*entities.MatchItem{invalidItemsItemOne, nil},
	)
	require.Error(t, err)
}

func TestMatchGroupConfirmSuccess(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	runID := uuid.New()
	ruleID := uuid.New()

	itemOne, itemTwo := newMatchItemPair(t, decimal.NewFromInt(10))

	confidence, err := value_objects.ParseConfidenceScore(95)
	require.NoError(t, err)

	group, err := entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		ruleID,
		confidence,
		[]*entities.MatchItem{itemOne, itemTwo},
	)
	require.NoError(t, err)

	require.NoError(t, group.Confirm(context.Background()))
	require.Equal(t, value_objects.MatchGroupStatusConfirmed, group.Status)
	require.NotNil(t, group.ConfirmedAt)

	if group.ConfirmedAt != nil {
		require.WithinDuration(t, group.UpdatedAt, *group.ConfirmedAt, 2*time.Second)
	}

	require.ErrorIs(t, group.Reject(context.Background(), "unmatch after confirm"), entities.ErrMatchGroupMustBeProposedToReject)
	require.NoError(t, group.Revoke(context.Background(), "unmatch after confirm"))
	require.Equal(t, value_objects.MatchGroupStatusRevoked, group.Status)
	require.NotNil(t, group.ConfirmedAt, "ConfirmedAt must be preserved after revocation for audit trail")
}

func TestMatchGroupRejectGuards(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	runID := uuid.New()
	ruleID := uuid.New()

	itemOne, itemTwo := newMatchItemPair(t, decimal.NewFromInt(10))

	confidence, err := value_objects.ParseConfidenceScore(100)
	require.NoError(t, err)

	group, err := entities.NewMatchGroup(
		context.Background(),
		contextID,
		runID,
		ruleID,
		confidence,
		[]*entities.MatchItem{itemOne, itemTwo},
	)
	require.NoError(t, err)

	require.Error(t, group.Reject(context.Background(), ""))

	require.NoError(t, group.Reject(context.Background(), "insufficient data"))
	require.Equal(t, value_objects.MatchGroupStatusRejected, group.Status)
	require.NotNil(t, group.RejectedReason)

	if group.RejectedReason != nil {
		require.Equal(t, "insufficient data", *group.RejectedReason)
	}

	require.Error(t, group.Confirm(context.Background()))
	require.Error(t, group.Reject(context.Background(), "already rejected"))
}

func TestMatchGroupNilReceiver(t *testing.T) {
	t.Parallel()

	group := (*entities.MatchGroup)(nil)

	require.False(t, group.CanAutoConfirm())
	require.Error(t, group.Confirm(context.Background()))
	require.Error(t, group.Reject(context.Background(), "reason"))
	require.Error(t, group.Revoke(context.Background(), "reason"))
}

func newMatchItem(t *testing.T, amount decimal.Decimal) *entities.MatchItem {
	t.Helper()

	item, err := entities.NewMatchItem(context.Background(), uuid.New(), amount, "USD", amount)
	require.NoError(t, err)

	return item
}

func newMatchItemPair(
	t *testing.T,
	amount decimal.Decimal,
) (*entities.MatchItem, *entities.MatchItem) {
	t.Helper()

	return newMatchItem(t, amount), newMatchItem(t, amount)
}
