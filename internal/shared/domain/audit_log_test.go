//go:build unit

package shared

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAuditLog_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	entityID := uuid.New()
	actor := "user-123"
	changes := []byte(`{"name":"test"}`)

	log, err := NewAuditLog(ctx, tenantID, "context", entityID, "create", &actor, changes)

	require.NoError(t, err)
	require.NotNil(t, log)
	assert.NotEqual(t, uuid.Nil, log.ID)
	assert.Equal(t, tenantID, log.TenantID)
	assert.Equal(t, "context", log.EntityType)
	assert.Equal(t, entityID, log.EntityID)
	assert.Equal(t, "create", log.Action)
	require.NotNil(t, log.ActorID)
	assert.Equal(t, "user-123", *log.ActorID)
	assert.Equal(t, changes, log.Changes)
	assert.False(t, log.CreatedAt.IsZero())
}

func TestNewAuditLog_NilActorID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "source", uuid.New(), "update", nil, []byte(`{}`))

	require.NoError(t, err)
	require.NotNil(t, log)
	assert.Nil(t, log.ActorID)
}

func TestNewAuditLog_EmptyActorIDString(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	emptyActor := "   "

	log, err := NewAuditLog(ctx, uuid.New(), "source", uuid.New(), "update", &emptyActor, []byte(`{}`))

	require.NoError(t, err)
	require.NotNil(t, log)
	assert.Nil(t, log.ActorID, "whitespace-only actor should be normalized to nil")
}

func TestNewAuditLog_ChangesCopied(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	changes := []byte(`{"original":true}`)

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), "create", nil, changes)
	require.NoError(t, err)

	// Mutate original — should not affect audit log.
	changes[0] = 'X'

	assert.NotEqual(t, changes, log.Changes, "changes should be a defensive copy")
}

func TestNewAuditLog_ErrTenantIDRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.Nil, "context", uuid.New(), "create", nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditTenantIDRequired)
}

func TestNewAuditLog_ErrEntityTypeRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "", uuid.New(), "create", nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditEntityTypeRequired)
}

func TestNewAuditLog_ErrEntityTypeRequired_Whitespace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "   ", uuid.New(), "create", nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditEntityTypeRequired)
}

func TestNewAuditLog_ErrEntityTypeTooLong(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	longType := strings.Repeat("x", MaxEntityTypeLength+1)

	log, err := NewAuditLog(ctx, uuid.New(), longType, uuid.New(), "create", nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditEntityTypeTooLong)
}

func TestNewAuditLog_ErrEntityIDRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.Nil, "create", nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditEntityIDRequired)
}

func TestNewAuditLog_ErrActionRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), "", nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditActionRequired)
}

func TestNewAuditLog_ErrActionRequired_Whitespace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), "   ", nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditActionRequired)
}

func TestNewAuditLog_ErrActionTooLong(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	longAction := strings.Repeat("a", MaxActionLength+1)

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), longAction, nil, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditActionTooLong)
}

func TestNewAuditLog_ErrActorIDTooLong(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	longActor := strings.Repeat("u", MaxActorIDLength+1)

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), "create", &longActor, []byte(`{}`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditActorIDTooLong)
}

func TestNewAuditLog_ErrChangesRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), "create", nil, []byte{})

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditChangesRequired)
}

func TestNewAuditLog_ErrChangesRequired_Nil(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), "create", nil, nil)

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditChangesRequired)
}

func TestNewAuditLog_ErrChangesInvalidJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "context", uuid.New(), "create", nil, []byte(`not json`))

	assert.Nil(t, log)
	assert.ErrorIs(t, err, ErrAuditChangesInvalidJSON)
}

func TestNewAuditLog_TrimsEntityTypeAndAction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "  context  ", uuid.New(), "  create  ", nil, []byte(`{}`))

	require.NoError(t, err)
	assert.Equal(t, "context", log.EntityType)
	assert.Equal(t, "create", log.Action)
}

func TestAuditLogFieldLengthConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 50, MaxEntityTypeLength)
	assert.Equal(t, 50, MaxActionLength)
	assert.Equal(t, 255, MaxActorIDLength)
}
