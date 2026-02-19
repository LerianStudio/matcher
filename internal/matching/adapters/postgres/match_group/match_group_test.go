//go:build unit

package match_group

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func ptrStr(s string) *string { return &s }

func TestPostgreSQLModel_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	newGroup := func(t *testing.T) *matchingEntities.MatchGroup {
		t.Helper()

		confidence, err := matchingVO.ParseConfidenceScore(80)
		require.NoError(t, err)

		itemA, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(10),
			"USD",
			decimal.NewFromInt(10),
		)
		require.NoError(t, err)
		itemB, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(10),
			"USD",
			decimal.NewFromInt(10),
		)
		require.NoError(t, err)

		group, err := matchingEntities.NewMatchGroup(
			ctx,
			uuid.New(),
			uuid.New(),
			uuid.New(),
			confidence,
			[]*matchingEntities.MatchItem{itemA, itemB},
		)
		require.NoError(t, err)

		return group
	}

	t.Run("rejected", func(t *testing.T) {
		t.Parallel()

		group := newGroup(t)
		reason := "insufficient data"
		require.NoError(t, group.Reject(ctx, reason))

		model, err := NewPostgreSQLModel(group)
		require.NoError(t, err)

		again, err := model.ToEntity()
		require.NoError(t, err)

		require.Equal(t, group.ID, again.ID)
		require.Equal(t, group.Status, again.Status)
		require.NotNil(t, again.RejectedReason)
		require.Equal(t, reason, *again.RejectedReason)
		require.Nil(t, again.ConfirmedAt)
	})

	t.Run("confirmed", func(t *testing.T) {
		t.Parallel()

		group := newGroup(t)
		require.NoError(t, group.Confirm(ctx))

		model, err := NewPostgreSQLModel(group)
		require.NoError(t, err)

		again, err := model.ToEntity()
		require.NoError(t, err)

		require.Equal(t, group.ID, again.ID)
		require.Equal(t, group.Status, again.Status)
		require.Nil(t, again.RejectedReason)
		require.NotNil(t, again.ConfirmedAt)
		require.True(t, again.ConfirmedAt.Equal(*group.ConfirmedAt))
	})
}

func TestNewPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewPostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchGroupEntityNeeded)
}

func TestNewPostgreSQLModel_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()

	confidence, err := matchingVO.ParseConfidenceScore(85)
	require.NoError(t, err)

	itemA, err := matchingEntities.NewMatchItem(
		ctx,
		uuid.New(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
	)
	require.NoError(t, err)

	itemB, err := matchingEntities.NewMatchItem(
		ctx,
		uuid.New(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
	)
	require.NoError(t, err)

	group, err := matchingEntities.NewMatchGroup(
		ctx,
		uuid.New(),
		uuid.New(),
		uuid.New(),
		confidence,
		[]*matchingEntities.MatchItem{itemA, itemB},
	)
	require.NoError(t, err)

	group.CreatedAt = now
	group.UpdatedAt = now

	model, err := NewPostgreSQLModel(group)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, group.ID.String(), model.ID)
	assert.Equal(t, group.ContextID.String(), model.ContextID)
	assert.Equal(t, group.RunID.String(), model.RunID)
	require.NotNil(t, model.RuleID)
	assert.Equal(t, group.RuleID.String(), *model.RuleID)
	assert.Equal(t, 85, model.Confidence)
	assert.Equal(t, "PROPOSED", model.Status)
	assert.Equal(t, now, model.CreatedAt)
	assert.Equal(t, now, model.UpdatedAt)
	assert.Nil(t, model.RejectedReason)
	assert.Nil(t, model.ConfirmedAt)
}

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *PostgreSQLModel
	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrMatchGroupModelNeeded)
}

func TestToEntity_InvalidID(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:        "not-a-uuid",
		ContextID: uuid.New().String(),
		RunID:     uuid.New().String(),
		RuleID:    ptrStr(uuid.New().String()),
		Status:    "PROPOSED",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse id")
}

func TestToEntity_InvalidContextID(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: "invalid",
		RunID:     uuid.New().String(),
		RuleID:    ptrStr(uuid.New().String()),
		Status:    "PROPOSED",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse context id")
}

func TestToEntity_InvalidRunID(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		RunID:     "invalid",
		RuleID:    ptrStr(uuid.New().String()),
		Status:    "PROPOSED",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse run id")
}

func TestToEntity_InvalidRuleID(t *testing.T) {
	t.Parallel()

	invalidRule := "invalid"
	model := &PostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		RunID:     uuid.New().String(),
		RuleID:    &invalidRule,
		Status:    "PROPOSED",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse rule id")
}

func TestToEntity_NilRuleID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &PostgreSQLModel{
		ID:         uuid.New().String(),
		ContextID:  uuid.New().String(),
		RunID:      uuid.New().String(),
		RuleID:     nil,
		Confidence: 80,
		Status:     "CONFIRMED",
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, uuid.Nil, entity.RuleID)
}

func TestToEntity_InvalidConfidence(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:         uuid.New().String(),
		ContextID:  uuid.New().String(),
		RunID:      uuid.New().String(),
		RuleID:     ptrStr(uuid.New().String()),
		Confidence: 150,
		Status:     "PENDING",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse confidence")
}

func TestToEntity_InvalidStatus(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:         uuid.New().String(),
		ContextID:  uuid.New().String(),
		RunID:      uuid.New().String(),
		RuleID:     ptrStr(uuid.New().String()),
		Confidence: 80,
		Status:     "INVALID_STATUS",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse status")
}

func TestToEntity_SuccessWithOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	reason := "not enough data"

	model := &PostgreSQLModel{
		ID:             uuid.New().String(),
		ContextID:      uuid.New().String(),
		RunID:          uuid.New().String(),
		RuleID:         ptrStr(uuid.New().String()),
		Confidence:     75,
		Status:         "REJECTED",
		RejectedReason: &reason,
		ConfirmedAt:    nil,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, 75, entity.Confidence.Value())
	assert.Equal(t, matchingVO.MatchGroupStatusRejected, entity.Status)
	require.NotNil(t, entity.RejectedReason)
	assert.Equal(t, reason, *entity.RejectedReason)
	assert.Nil(t, entity.ConfirmedAt)
}

func TestToEntity_SuccessWithConfirmedAt(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	confirmedAt := now.Add(time.Hour)

	model := &PostgreSQLModel{
		ID:          uuid.New().String(),
		ContextID:   uuid.New().String(),
		RunID:       uuid.New().String(),
		RuleID:      ptrStr(uuid.New().String()),
		Confidence:  90,
		Status:      "CONFIRMED",
		ConfirmedAt: &confirmedAt,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, matchingVO.MatchGroupStatusConfirmed, entity.Status)
	require.NotNil(t, entity.ConfirmedAt)
	assert.Equal(t, confirmedAt, *entity.ConfirmedAt)
}
