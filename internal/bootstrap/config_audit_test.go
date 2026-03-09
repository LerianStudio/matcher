//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// --- Mock OutboxRepository ---

// mockOutboxRepo is a minimal mock for sharedPorts.OutboxRepository that
// captures Create calls. Only Create is used by ConfigAuditPublisher; all
// other methods return zero values.
type mockOutboxRepo struct {
	createdEvents []*sharedDomain.OutboxEvent
	createErr     error
}

func (m *mockOutboxRepo) Create(_ context.Context, event *sharedDomain.OutboxEvent) (*sharedDomain.OutboxEvent, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}

	m.createdEvents = append(m.createdEvents, event)

	return event, nil
}

func (m *mockOutboxRepo) CreateWithTx(_ context.Context, _ *sql.Tx, _ *sharedDomain.OutboxEvent) (*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepo) ListPending(context.Context, int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepo) ListPendingByType(context.Context, string, int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepo) ListTenants(context.Context) ([]string, error) { return nil, nil }

func (m *mockOutboxRepo) GetByID(context.Context, uuid.UUID) (*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (m *mockOutboxRepo) MarkFailed(context.Context, uuid.UUID, string, int) error { return nil }

func (m *mockOutboxRepo) ListFailedForRetry(_ context.Context, _ int, _ time.Time, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepo) ResetForRetry(_ context.Context, _ int, _ time.Time, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepo) ResetStuckProcessing(_ context.Context, _ int, _ time.Time, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepo) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error { return nil }

// --- Tests ---

func TestNewConfigAuditPublisher_NilOutboxRepo(t *testing.T) {
	t.Parallel()

	pub, err := NewConfigAuditPublisher(nil, &libLog.NopLogger{})

	assert.Nil(t, pub)
	assert.ErrorIs(t, err, ErrNilOutboxRepoForConfigAudit)
}

func TestNewConfigAuditPublisher_NilLogger(t *testing.T) {
	t.Parallel()

	pub, err := NewConfigAuditPublisher(&mockOutboxRepo{}, nil)

	require.NoError(t, err)
	assert.NotNil(t, pub)
}

func TestNewConfigAuditPublisher_Success(t *testing.T) {
	t.Parallel()

	pub, err := NewConfigAuditPublisher(&mockOutboxRepo{}, &libLog.NopLogger{})

	require.NoError(t, err)
	assert.NotNil(t, pub)
}

func TestPublishConfigChange_CreatesCorrectAuditEvent(t *testing.T) {
	t.Parallel()

	repo := &mockOutboxRepo{}
	pub, err := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	changes := []ConfigChange{
		{Key: "rate_limit.max", OldValue: 100, NewValue: 200},
		{Key: "app.log_level", OldValue: "info", NewValue: "debug"},
	}

	err = pub.PublishConfigChange(ctx, "user-42", "updated", changes)
	require.NoError(t, err)

	// Verify one outbox event was created.
	require.Len(t, repo.createdEvents, 1)

	outboxEvent := repo.createdEvents[0]
	assert.Equal(t, sharedDomain.EventTypeAuditLogCreated, outboxEvent.EventType)
	assert.Equal(t, systemConfigEntityID, outboxEvent.AggregateID)
	assert.Equal(t, sharedDomain.OutboxStatusPending, outboxEvent.Status)

	// Deserialize the payload and verify audit event fields.
	var auditEvent sharedDomain.AuditLogCreatedEvent
	require.NoError(t, json.Unmarshal(outboxEvent.Payload, &auditEvent))

	assert.Equal(t, sharedDomain.EventTypeAuditLogCreated, auditEvent.EventType)
	assert.Equal(t, tenantID, auditEvent.TenantID)
	assert.Equal(t, systemConfigEntityType, auditEvent.EntityType)
	assert.Equal(t, systemConfigEntityID, auditEvent.EntityID)
	assert.Equal(t, "updated", auditEvent.Action)
	assert.NotNil(t, auditEvent.Actor)
	assert.Equal(t, "user-42", *auditEvent.Actor)
	assert.NotNil(t, auditEvent.Changes)

	// Verify the changes payload contains our config changes.
	configChanges, ok := auditEvent.Changes["config_changes"]
	assert.True(t, ok, "expected config_changes key in changes map")
	assert.NotNil(t, configChanges)
}

func TestPublishConfigChange_SystemActor(t *testing.T) {
	t.Parallel()

	repo := &mockOutboxRepo{}
	pub, err := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	changes := []ConfigChange{
		{Key: "version", OldValue: uint64(1), NewValue: uint64(2)},
	}

	err = pub.PublishConfigChange(ctx, "system", "reloaded", changes)
	require.NoError(t, err)
	require.Len(t, repo.createdEvents, 1)

	var auditEvent sharedDomain.AuditLogCreatedEvent
	require.NoError(t, json.Unmarshal(repo.createdEvents[0].Payload, &auditEvent))

	assert.Equal(t, "reloaded", auditEvent.Action)
	assert.NotNil(t, auditEvent.Actor)
	assert.Equal(t, "system", *auditEvent.Actor)
}

func TestPublishConfigChange_EmptyActor(t *testing.T) {
	t.Parallel()

	repo := &mockOutboxRepo{}
	pub, err := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	err = pub.PublishConfigChange(ctx, "", "updated", []ConfigChange{
		{Key: "app.log_level", OldValue: "info", NewValue: "debug"},
	})
	require.NoError(t, err)

	var auditEvent sharedDomain.AuditLogCreatedEvent
	require.NoError(t, json.Unmarshal(repo.createdEvents[0].Payload, &auditEvent))

	// Empty actor should produce nil actor pointer.
	assert.Nil(t, auditEvent.Actor)
}

func TestPublishConfigChange_InvalidTenantID(t *testing.T) {
	t.Parallel()

	repo := &mockOutboxRepo{}
	pub, err := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")

	err = pub.PublishConfigChange(ctx, "user-1", "updated", []ConfigChange{
		{Key: "app.log_level", OldValue: "info", NewValue: "debug"},
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse tenant id")
	assert.Empty(t, repo.createdEvents)
}

func TestPublishConfigChange_OutboxCreateFails(t *testing.T) {
	t.Parallel()

	repo := &mockOutboxRepo{createErr: assert.AnError}
	pub, err := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	err = pub.PublishConfigChange(ctx, "user-1", "updated", []ConfigChange{
		{Key: "app.log_level", OldValue: "info", NewValue: "debug"},
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "persist outbox event")
}

func TestPublishConfigChange_NilPublisher(t *testing.T) {
	t.Parallel()

	var pub *ConfigAuditPublisher

	err := pub.PublishConfigChange(context.Background(), "user-1", "updated", nil)
	assert.ErrorIs(t, err, ErrNilOutboxRepoForConfigAudit)
}

func TestBuildConfigChangesMap_EmptyChanges(t *testing.T) {
	t.Parallel()

	result := buildConfigChangesMap(nil)
	assert.Nil(t, result)

	result = buildConfigChangesMap([]ConfigChange{})
	assert.Nil(t, result)
}

func TestBuildConfigChangesMap_WithChanges(t *testing.T) {
	t.Parallel()

	changes := []ConfigChange{
		{Key: "rate_limit.max", OldValue: 100, NewValue: 200},
	}

	result := buildConfigChangesMap(changes)
	require.NotNil(t, result)

	items, ok := result["config_changes"]
	assert.True(t, ok)

	itemsList, ok := items.([]map[string]any)
	require.True(t, ok)
	require.Len(t, itemsList, 1)

	assert.Equal(t, "rate_limit.max", itemsList[0]["key"])
	assert.Equal(t, 100, itemsList[0]["old_value"])
	assert.Equal(t, 200, itemsList[0]["new_value"])
}

func TestSystemConfigEntityID_IsDeterministic(t *testing.T) {
	t.Parallel()

	// Recompute to verify determinism.
	expected := uuid.NewSHA1(systemConfigNamespace, []byte(systemConfigEntityType))
	assert.Equal(t, expected, systemConfigEntityID)
	assert.NotEqual(t, uuid.Nil, systemConfigEntityID)
}

func TestAppliedToConfigChanges(t *testing.T) {
	t.Parallel()

	applied := []ConfigChangeResult{
		{Key: "rate_limit.max", OldValue: 100, NewValue: 200, HotReloaded: true},
		{Key: "app.log_level", OldValue: "info", NewValue: "debug", HotReloaded: true},
	}

	changes := appliedToConfigChanges(applied)

	require.Len(t, changes, 2)
	assert.Equal(t, "rate_limit.max", changes[0].Key)
	assert.Equal(t, 100, changes[0].OldValue)
	assert.Equal(t, 200, changes[0].NewValue)
	assert.Equal(t, "app.log_level", changes[1].Key)
}

func TestAppliedToConfigChanges_Empty(t *testing.T) {
	t.Parallel()

	changes := appliedToConfigChanges(nil)
	assert.Empty(t, changes)

	changes = appliedToConfigChanges([]ConfigChangeResult{})
	assert.Empty(t, changes)
}

func TestSetAuditCallback_NilArgs(t *testing.T) {
	t.Parallel()

	// Should not panic with nil arguments.
	SetAuditCallback(nil, nil, nil)
	SetAuditCallback(nil, &ConfigAuditPublisher{}, nil)

	cm, err := NewConfigManager(defaultConfig(), "", &libLog.NopLogger{})
	require.NoError(t, err)

	SetAuditCallback(cm, nil, nil)
}

func TestSetAuditCallback_SubscriberRegistered(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, "", &libLog.NopLogger{})
	require.NoError(t, err)

	repo := &mockOutboxRepo{}
	pub, err := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, err)

	// Record subscriber count before.
	cm.mu.Lock()
	beforeCount := len(cm.subscribers)
	cm.mu.Unlock()

	SetAuditCallback(cm, pub, &libLog.NopLogger{})

	cm.mu.Lock()
	afterCount := len(cm.subscribers)
	cm.mu.Unlock()

	assert.Equal(t, beforeCount+1, afterCount, "SetAuditCallback should register one subscriber")
}

func TestConfigAPIHandler_SetAuditPublisher(t *testing.T) {
	t.Parallel()

	cm, err := NewConfigManager(defaultConfig(), "", &libLog.NopLogger{})
	require.NoError(t, err)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	assert.Nil(t, handler.auditPublisher)

	repo := &mockOutboxRepo{}
	pub, pubErr := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, pubErr)

	handler.SetAuditPublisher(pub)
	assert.NotNil(t, handler.auditPublisher)
}

func TestConfigAPIHandler_SetAuditPublisher_NilHandler(t *testing.T) {
	t.Parallel()

	var handler *ConfigAPIHandler

	// Should not panic.
	handler.SetAuditPublisher(&ConfigAuditPublisher{})
}

func TestSetAuditCallback_SkipsAPISourceUpdates(t *testing.T) {
	t.Parallel()

	cm, err := NewConfigManager(defaultConfig(), "", &libLog.NopLogger{})
	require.NoError(t, err)

	repo := &mockOutboxRepo{}
	pub, err := NewConfigAuditPublisher(repo, &libLog.NopLogger{})
	require.NoError(t, err)

	SetAuditCallback(cm, pub, &libLog.NopLogger{})

	_, err = cm.Update(map[string]any{"rate_limit.max": 111})
	require.NoError(t, err)

	_, err = cm.Update(map[string]any{"rate_limit.max": 222})
	require.NoError(t, err)

	assert.Empty(t, repo.createdEvents, "API updates should not be double-audited by subscriber callback")
}
