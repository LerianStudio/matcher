//go:build unit

package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	outboxmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

func TestEnqueueMatchConfirmedEvents_NilOutboxRepoTx(t *testing.T) {
	t.Parallel()

	uc := &UseCase{outboxRepoTx: nil}
	err := uc.enqueueMatchConfirmedEvents(context.Background(), new(sql.Tx), nil)
	require.ErrorIs(t, err, ErrOutboxRepoNotConfigured)
}

func TestEnqueueMatchConfirmedEvents_NilTx_Returns_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	err := uc.enqueueMatchConfirmedEvents(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrOutboxRequiresSQLTx)
}

func TestEnqueueMatchConfirmedEvents_EmptyGroups(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc := &UseCase{outboxRepoTx: outboxRepo}

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000220001")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	err := uc.enqueueMatchConfirmedEvents(ctx, new(sql.Tx), []*matchingEntities.MatchGroup{})
	require.NoError(t, err)
}

func TestEnqueueMatchConfirmedEvents_InvalidTenantID_Returns_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	confidence, _ := matchingVO.ParseConfidenceScore(100)
	now := time.Now().UTC()
	groups := []*matchingEntities.MatchGroup{
		{
			ID:          uuid.New(),
			ContextID:   uuid.New(),
			RunID:       uuid.New(),
			RuleID:      uuid.New(),
			Status:      matchingVO.MatchGroupStatusConfirmed,
			Confidence:  confidence,
			ConfirmedAt: &now,
			Items:       []*matchingEntities.MatchItem{{TransactionID: uuid.New()}},
		},
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")
	err := uc.enqueueMatchConfirmedEvents(ctx, new(sql.Tx), groups)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tenant id")
}

func TestEnqueueGroupEvent_NilGroup_Returns_Nil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}
	err := uc.enqueueGroupEvent(context.Background(), new(sql.Tx), nil, uuid.New(), "slug")
	require.NoError(t, err)
}

func TestEnqueueGroupEvent_NonConfirmedStatus_NoOp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	confidence, _ := matchingVO.ParseConfidenceScore(50)
	group := &matchingEntities.MatchGroup{
		ID:         uuid.New(),
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
		Items:      []*matchingEntities.MatchItem{{TransactionID: uuid.New()}},
	}

	err := uc.enqueueGroupEvent(context.Background(), new(sql.Tx), group, uuid.New(), "slug")
	require.NoError(t, err)
}

func TestEnqueueGroupEvent_ConfirmedWithoutTimestamp_Returns_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	confidence, _ := matchingVO.ParseConfidenceScore(90)
	group := &matchingEntities.MatchGroup{
		ID:          uuid.New(),
		ContextID:   uuid.New(),
		RunID:       uuid.New(),
		RuleID:      uuid.New(),
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: nil,
		Items:       []*matchingEntities.MatchItem{{TransactionID: uuid.New()}},
	}

	err := uc.enqueueGroupEvent(context.Background(), new(sql.Tx), group, uuid.New(), "slug")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build match confirmed event")
}

func TestEnqueueMatchConfirmedEvents_SkipsNonConfirmed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)
	// No CreateWithTx expectations means it should NOT be called.

	uc := &UseCase{outboxRepoTx: outboxRepo}

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000220010")
	confidence, _ := matchingVO.ParseConfidenceScore(50)
	groups := []*matchingEntities.MatchGroup{
		{
			ID:         uuid.New(),
			Status:     matchingVO.MatchGroupStatusProposed,
			Confidence: confidence,
		},
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	err := uc.enqueueMatchConfirmedEvents(ctx, new(sql.Tx), groups)
	require.NoError(t, err)
}

func TestEnqueueGroupEvent_HugeIDListTruncated(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000220030")
	confidence, _ := matchingVO.ParseConfidenceScore(100)
	now := time.Now().UTC()

	// Build a group with ~30k transaction IDs. Each UUID serializes to
	// 38 bytes + 1 separator; 30k * 39 = 1.17 MiB, comfortably over the
	// 1 MiB broker cap even before envelope overhead.
	items := make([]*matchingEntities.MatchItem, 30000)
	for i := range items {
		items[i] = &matchingEntities.MatchItem{TransactionID: uuid.New()}
	}

	group := &matchingEntities.MatchGroup{
		ID:          uuid.New(),
		ContextID:   uuid.New(),
		RunID:       uuid.New(),
		RuleID:      uuid.New(),
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		Items:       items,
	}

	outboxRepo.EXPECT().CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *sql.Tx, event *shared.OutboxEvent) (*shared.OutboxEvent, error) {
			require.LessOrEqual(t, len(event.Payload), shared.DefaultOutboxMaxPayloadBytes,
				"truncated payload must fit under broker cap")

			var payload shared.MatchConfirmedEvent
			require.NoError(t, json.Unmarshal(event.Payload, &payload))
			assert.Equal(t, 30000, payload.TruncatedIDCount,
				"TruncatedIDCount preserves the original list length")
			assert.Less(t, len(payload.TransactionIDs), 30000,
				"TransactionIDs must be trimmed below the original count")
			assert.Positive(t, len(payload.TransactionIDs),
				"at least one id should fit under the cap")

			return event, nil
		},
	)

	uc := &UseCase{outboxRepoTx: outboxRepo}
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	err := uc.enqueueGroupEvent(ctx, new(sql.Tx), group, tenantID, "tenant-slug")
	require.NoError(t, err)
}

func TestEnqueueMatchConfirmedEvents_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000220020")
	confidence, _ := matchingVO.ParseConfidenceScore(100)
	now := time.Now().UTC()
	txID := uuid.New()
	group := &matchingEntities.MatchGroup{
		ID:          uuid.New(),
		ContextID:   uuid.New(),
		RunID:       uuid.New(),
		RuleID:      uuid.New(),
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		Items:       []*matchingEntities.MatchItem{{TransactionID: txID}},
	}

	outboxRepo.EXPECT().CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *sql.Tx, event *shared.OutboxEvent) (*shared.OutboxEvent, error) {
			require.Equal(t, shared.EventTypeMatchConfirmed, event.EventType)

			var payload shared.MatchConfirmedEvent
			require.NoError(t, json.Unmarshal(event.Payload, &payload))
			require.Equal(t, tenantID, payload.TenantID)
			require.Equal(t, group.ID, payload.MatchID)
			require.Equal(t, []uuid.UUID{txID}, payload.TransactionIDs)

			return event, nil
		},
	)

	uc := &UseCase{outboxRepoTx: outboxRepo}
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	err := uc.enqueueMatchConfirmedEvents(ctx, new(sql.Tx), []*matchingEntities.MatchGroup{group})
	require.NoError(t, err)
}
