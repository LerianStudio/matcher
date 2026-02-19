//go:build unit

package entities

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAuditLog(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	entityID := uuid.New()
	changes := []byte(`{"key":"original"}`)
	actorID := " user-1 "

	log, err := NewAuditLog(
		context.Background(),
		tenantID,
		" match_run ",
		entityID,
		" CREATED ",
		&actorID,
		changes,
	)
	require.NoError(t, err)
	require.NotNil(t, log)

	assert.NotEqual(t, uuid.Nil, log.ID)
	assert.Equal(t, tenantID, log.TenantID)
	assert.Equal(t, "match_run", log.EntityType)
	assert.Equal(t, entityID, log.EntityID)
	assert.Equal(t, "CREATED", log.Action)
	require.NotNil(t, log.ActorID)
	assert.Equal(t, "user-1", *log.ActorID)
	assert.False(t, log.CreatedAt.IsZero())

	changes[0] = 'X'

	assert.JSONEq(t, `{"key":"original"}`, string(log.Changes))
}

func TestNewAuditLogValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validJSON := []byte(`{}`)

	_, err := NewAuditLog(ctx, uuid.Nil, "type", uuid.New(), "action", nil, validJSON)
	require.ErrorIs(t, err, ErrTenantIDRequired)

	_, err = NewAuditLog(ctx, uuid.New(), " ", uuid.New(), "action", nil, validJSON)
	require.ErrorIs(t, err, ErrEntityTypeRequired)

	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.Nil, "action", nil, validJSON)
	require.ErrorIs(t, err, ErrEntityIDRequired)

	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), " ", nil, validJSON)
	require.ErrorIs(t, err, ErrActionRequired)

	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), "action", nil, nil)
	require.ErrorIs(t, err, ErrChangesRequired)

	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), "action", nil, []byte{})
	require.ErrorIs(t, err, ErrChangesRequired)

	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), "action", nil, []byte("not-json"))
	require.ErrorIs(t, err, ErrChangesInvalidJSON)

	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), "action", nil, []byte("{invalid}"))
	require.ErrorIs(t, err, ErrChangesInvalidJSON)
}

func TestNewAuditLogMaxLengthValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validChanges := []byte(`{}`)

	longEntityType := string(make([]byte, MaxEntityTypeLength+1))
	_, err := NewAuditLog(ctx, uuid.New(), longEntityType, uuid.New(), "action", nil, validChanges)
	require.ErrorIs(t, err, ErrEntityTypeTooLong)

	longAction := string(make([]byte, MaxActionLength+1))
	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), longAction, nil, validChanges)
	require.ErrorIs(t, err, ErrActionTooLong)

	longActorID := string(make([]byte, MaxActorIDLength+1))
	_, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), "action", &longActorID, validChanges)
	require.ErrorIs(t, err, ErrActorIDTooLong)
}

func TestNewAuditLogMaxLengthBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validChanges := []byte(`{}`)

	exactEntityType := strings.Repeat("e", MaxEntityTypeLength)
	log, err := NewAuditLog(ctx, uuid.New(), exactEntityType, uuid.New(), "action", nil, validChanges)
	require.NoError(t, err, "entityType at exactly MaxEntityTypeLength must be accepted")
	assert.Equal(t, exactEntityType, log.EntityType)

	exactAction := strings.Repeat("a", MaxActionLength)
	log, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), exactAction, nil, validChanges)
	require.NoError(t, err, "action at exactly MaxActionLength must be accepted")
	assert.Equal(t, exactAction, log.Action)

	exactActorID := strings.Repeat("u", MaxActorIDLength)
	log, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), "action", &exactActorID, validChanges)
	require.NoError(t, err, "actorID at exactly MaxActorIDLength must be accepted")
	require.NotNil(t, log.ActorID)
	assert.Equal(t, exactActorID, *log.ActorID)
}

func TestNewAuditLogActorIDNormalization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validChanges := []byte(`{}`)

	emptyActorID := ""
	log, err := NewAuditLog(
		ctx,
		uuid.New(),
		"type",
		uuid.New(),
		"action",
		&emptyActorID,
		validChanges,
	)
	require.NoError(t, err)
	require.Nil(t, log.ActorID, "empty string actorID should become nil")

	whitespaceActorID := "   "
	log, err = NewAuditLog(
		ctx,
		uuid.New(),
		"type",
		uuid.New(),
		"action",
		&whitespaceActorID,
		validChanges,
	)
	require.NoError(t, err)
	require.Nil(t, log.ActorID, "whitespace-only actorID should become nil")

	log, err = NewAuditLog(ctx, uuid.New(), "type", uuid.New(), "action", nil, validChanges)
	require.NoError(t, err)
	require.Nil(t, log.ActorID, "nil actorID should remain nil")
}
