// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"fmt"
	"testing"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore is a minimal Store implementation for seed tests. It records all
// Put calls so tests can inspect what was written.
type mockStore struct {
	putCalls []mockPutCall
	putErr   error
	getErr   error
	results  map[string]ports.ReadResult
}

type mockPutCall struct {
	Target   domain.Target
	Ops      []ports.WriteOp
	Expected domain.Revision
	Actor    domain.Actor
	Source   string
}

func (m *mockStore) Get(_ context.Context, target domain.Target) (ports.ReadResult, error) {
	if m.getErr != nil {
		return ports.ReadResult{}, m.getErr
	}

	if m.results == nil {
		return ports.ReadResult{}, nil
	}

	result, ok := m.results[m.targetKey(target)]
	if !ok {
		return ports.ReadResult{}, nil
	}

	return result, nil
}

func (m *mockStore) Put(_ context.Context, target domain.Target, ops []ports.WriteOp,
	expected domain.Revision, actor domain.Actor, source string,
) (domain.Revision, error) {
	m.putCalls = append(m.putCalls, mockPutCall{
		Target:   target,
		Ops:      ops,
		Expected: expected,
		Actor:    actor,
		Source:   source,
	})

	if m.putErr != nil {
		return domain.RevisionZero, m.putErr
	}

	return expected.Next(), nil
}

func (m *mockStore) targetKey(target domain.Target) string {
	return fmt.Sprintf("%s|%s|%s", target.Kind, target.Scope, target.SubjectID)
}

func TestConfigManager_SeedMode_DefaultOff(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	assert.False(t, cm.InSeedMode(), "new ConfigManager should not be in seed mode")
}

func TestConfigManager_EnterSeedMode(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	assert.True(t, cm.InSeedMode(), "after enterSeedMode(), InSeedMode() should return true")
}

func TestConfigManager_EnterSeedMode_Idempotent(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()
	cm.enterSeedMode() // Should not panic or misbehave

	assert.True(t, cm.InSeedMode())
}

func TestConfigManager_Get_StillWorksInSeedMode(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "debug"

	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	got := cm.Get()
	require.NotNil(t, got, "Get() should still work in seed mode")
	assert.Equal(t, "debug", got.App.LogLevel)
}

func TestExtractConfigValue_AppLogLevel(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "warn"

	val := extractConfigValue(cfg, "app.log_level")
	assert.Equal(t, "warn", val)
}

func TestExtractConfigValue_ServerBodyLimit(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Server.BodyLimitBytes = 42

	val := extractConfigValue(cfg, "server.body_limit_bytes")
	assert.Equal(t, 42, val)
}

func TestExtractConfigValue_PostgresHost(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Postgres.PrimaryHost = "db.prod.example.com"

	val := extractConfigValue(cfg, "postgres.primary_host")
	assert.Equal(t, "db.prod.example.com", val)
}

func TestExtractConfigValue_RenamedKeys(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Server.CORSAllowedOrigins = "https://app.example.com"
	cfg.Postgres.MaxOpenConnections = 41
	cfg.Postgres.MaxIdleConnections = 9
	cfg.Redis.MinIdleConn = 4
	cfg.RabbitMQ.URI = "amqps"

	tests := []struct {
		name string
		key  string
		want any
	}{
		{name: "cors origins", key: "cors.allowed_origins", want: "https://app.example.com"},
		{name: "postgres max open conns", key: "postgres.max_open_conns", want: 41},
		{name: "postgres max idle conns", key: "postgres.max_idle_conns", want: 9},
		{name: "redis min idle conns", key: "redis.min_idle_conns", want: 4},
		{name: "rabbitmq url", key: "rabbitmq.url", want: "amqps"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, extractConfigValue(cfg, tt.key))
		})
	}
}

func TestExtractConfigValue_BoolValue(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Enabled = true

	val := extractConfigValue(cfg, "rate_limit.enabled")
	assert.Equal(t, true, val)
}

func TestExtractConfigValue_UnknownKey(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	val := extractConfigValue(cfg, "nonexistent.key")
	assert.Nil(t, val)
}

func TestExtractConfigValue_NilConfig(t *testing.T) {
	t.Parallel()

	val := extractConfigValue(nil, "app.log_level")
	assert.Nil(t, val)
}

func TestExtractConfigValue_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	val := extractConfigValue(cfg, "")
	assert.Nil(t, val)
}

func TestIsEqualValue_SameInt(t *testing.T) {
	t.Parallel()

	assert.True(t, isEqualValue(100, 100))
}

func TestIsEqualValue_DifferentInt(t *testing.T) {
	t.Parallel()

	assert.False(t, isEqualValue(100, 200))
}

func TestIsEqualValue_SameString(t *testing.T) {
	t.Parallel()

	assert.True(t, isEqualValue("hello", "hello"))
}

func TestIsEqualValue_DifferentString(t *testing.T) {
	t.Parallel()

	assert.False(t, isEqualValue("hello", "world"))
}

func TestIsEqualValue_SameBool(t *testing.T) {
	t.Parallel()

	assert.True(t, isEqualValue(true, true))
}

func TestIsEqualValue_DifferentBool(t *testing.T) {
	t.Parallel()

	assert.False(t, isEqualValue(true, false))
}

func TestIsEqualValue_IntVsDefault(t *testing.T) {
	t.Parallel()

	assert.False(t, isEqualValue(200, 100))
	assert.True(t, isEqualValue(100, 100))
}

func TestIsEqualValue_NilVsNil(t *testing.T) {
	t.Parallel()

	assert.True(t, isEqualValue(nil, nil))
}

func TestIsEqualValue_NilVsValue(t *testing.T) {
	t.Parallel()

	assert.False(t, isEqualValue(nil, 100))
	assert.False(t, isEqualValue(100, nil))
}

func TestIsEqualValue_CrossTypeNumeric(t *testing.T) {
	t.Parallel()

	// int vs int64
	assert.True(t, isEqualValue(42, int64(42)))
	// int vs float64 (whole number)
	assert.True(t, isEqualValue(42, float64(42)))
}

func TestBuildSeedOps_AllDefaults(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// Create defs that match the defaults.
	defs := []domain.KeyDef{
		{
			Key:              "app.log_level",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     cfg.App.LogLevel,
			ValueType:        domain.ValueTypeString,
			MutableAtRuntime: true,
		},
		{
			Key:              "rate_limit.max",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     cfg.RateLimit.Max,
			ValueType:        domain.ValueTypeInt,
			MutableAtRuntime: true,
		},
	}

	ops := buildSeedOps(cfg, defs)
	assert.Empty(t, ops, "no ops should be generated when all values match defaults")
}

func TestBuildSeedOps_NonDefaultValues(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "debug" // Changed from default "info"
	cfg.RateLimit.Max = 500    // Changed from default 100

	defs := []domain.KeyDef{
		{
			Key:              "app.log_level",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "info",
			ValueType:        domain.ValueTypeString,
			MutableAtRuntime: true,
		},
		{
			Key:              "rate_limit.max",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     100,
			ValueType:        domain.ValueTypeInt,
			MutableAtRuntime: true,
		},
	}

	ops := buildSeedOps(cfg, defs)
	require.Len(t, ops, 2)

	// Ops should be in def iteration order.
	assert.Equal(t, "app.log_level", ops[0].Key)
	assert.Equal(t, "debug", ops[0].Value)

	assert.Equal(t, "rate_limit.max", ops[1].Key)
	assert.Equal(t, 500, ops[1].Value)
}

func TestBuildSeedOps_RenamedKeys(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Server.CORSAllowedOrigins = "https://app.example.com"
	cfg.Postgres.MaxOpenConnections = 41
	cfg.Redis.MinIdleConn = 4
	cfg.RabbitMQ.URI = "amqps"

	defs := []domain.KeyDef{
		{
			Key:              "cors.allowed_origins",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCORSAllowedOrigins,
			ValueType:        domain.ValueTypeString,
			MutableAtRuntime: true,
		},
		{
			Key:              "postgres.max_open_conns",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGMaxOpenConns,
			ValueType:        domain.ValueTypeInt,
			MutableAtRuntime: true,
		},
		{
			Key:              "redis.min_idle_conns",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisMinIdleConn,
			ValueType:        domain.ValueTypeInt,
			MutableAtRuntime: true,
		},
		{
			Key:              "rabbitmq.url",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitURI,
			ValueType:        domain.ValueTypeString,
			MutableAtRuntime: true,
		},
	}

	ops := buildSeedOps(cfg, defs)
	require.Len(t, ops, 4)
	assert.Equal(t, "cors.allowed_origins", ops[0].Key)
	assert.Equal(t, "https://app.example.com", ops[0].Value)
	assert.Equal(t, "postgres.max_open_conns", ops[1].Key)
	assert.Equal(t, 41, ops[1].Value)
	assert.Equal(t, "redis.min_idle_conns", ops[2].Key)
	assert.Equal(t, 4, ops[2].Value)
	assert.Equal(t, "rabbitmq.url", ops[3].Key)
	assert.Equal(t, "amqps", ops[3].Value)
}

func TestBuildSeedOps_IncludesBootstrapOnlyWhenNonDefault(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "staging" // Changed but bootstrap-only

	defs := []domain.KeyDef{
		{
			Key:              "app.env_name",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "development",
			ValueType:        domain.ValueTypeString,
			MutableAtRuntime: false, // Bootstrap-only
		},
	}

	ops := buildSeedOps(cfg, defs)
	require.Len(t, ops, 1)
	assert.Equal(t, "app.env_name", ops[0].Key)
	assert.Equal(t, "staging", ops[0].Value)
}

func TestBuildSeedOps_SkipsBootstrapOnlySecrets(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Idempotency.HMACSecret = "bootstrap-secret"

	defs := []domain.KeyDef{
		{
			Key:              "idempotency.hmac_secret",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			MutableAtRuntime: false,
			Secret:           true,
		},
	}

	ops := buildSeedOps(cfg, defs)
	assert.Empty(t, ops)
}

func TestSeedStore_NoNonDefaultValues(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)

	store := &mockStore{}
	reg := registry.New()

	// Register a single mutable key with matching default value.
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "app.log_level",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		DefaultValue:     "info", // Matches cfg.App.LogLevel
		ValueType:        domain.ValueTypeString,
		ApplyBehavior:    domain.ApplyBundleRebuild,
		MutableAtRuntime: true,
	}))

	err = cm.SeedStore(context.Background(), store, reg)
	require.NoError(t, err)

	assert.Empty(t, store.putCalls, "no Put should be called when all values are default")
	assert.False(t, cm.InSeedMode(), "SeedStore should not enter seed mode before initial reload succeeds")
}

func TestSeedStore_WithNonDefaultValues(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "debug"    // Non-default
	cfg.RateLimit.Max = 500       // Non-default
	cfg.RateLimit.Enabled = false // Non-default (default is true)

	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)

	store := &mockStore{}
	reg := registry.New()

	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "app.log_level",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		DefaultValue:     "info",
		ValueType:        domain.ValueTypeString,
		ApplyBehavior:    domain.ApplyBundleRebuild,
		MutableAtRuntime: true,
	}))
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "rate_limit.max",
		Kind:             domain.KindSetting,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant},
		DefaultValue:     100,
		ValueType:        domain.ValueTypeInt,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
	}))
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "rate_limit.enabled",
		Kind:             domain.KindSetting,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant},
		DefaultValue:     true,
		ValueType:        domain.ValueTypeBool,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
	}))

	err = cm.SeedStore(context.Background(), store, reg)
	require.NoError(t, err)

	require.Len(t, store.putCalls, 2, "should seed configs and settings separately")

	configCall := store.putCalls[0]
	settingCall := store.putCalls[1]

	assert.Equal(t, domain.KindConfig, configCall.Target.Kind)
	assert.Equal(t, domain.ScopeGlobal, configCall.Target.Scope)
	assert.Equal(t, domain.RevisionZero, configCall.Expected)
	assert.Equal(t, domain.Actor{ID: "seed-migration"}, configCall.Actor)
	assert.Equal(t, "seed", configCall.Source)
	assert.Len(t, configCall.Ops, 1)

	assert.Equal(t, domain.KindSetting, settingCall.Target.Kind)
	assert.Equal(t, domain.ScopeGlobal, settingCall.Target.Scope)
	assert.Equal(t, domain.RevisionZero, settingCall.Expected)
	assert.Equal(t, domain.Actor{ID: "seed-migration"}, settingCall.Actor)
	assert.Equal(t, "seed", settingCall.Source)
	assert.Len(t, settingCall.Ops, 2)

	assert.False(t, cm.InSeedMode(), "SeedStore should not enter seed mode before initial reload succeeds")
}

func TestSeedStore_StoreError(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "debug" // Non-default

	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)

	store := &mockStore{putErr: fmt.Errorf("connection refused")}
	reg := registry.New()

	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "app.log_level",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		DefaultValue:     "info",
		ValueType:        domain.ValueTypeString,
		ApplyBehavior:    domain.ApplyBundleRebuild,
		MutableAtRuntime: true,
	}))

	err = cm.SeedStore(context.Background(), store, reg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")

	// Should NOT enter seed mode on failure.
	assert.False(t, cm.InSeedMode(), "should not enter seed mode when store.Put fails")
}

func TestSeedStore_StoreAlreadyHasData(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "debug" // Non-default, will generate a seed op

	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)

	// Simulate pre-existing data: the store returns a revision mismatch error
	// when Put is called with RevisionZero (expects empty store).
	store := &mockStore{putErr: domain.ErrRevisionMismatch}
	reg := registry.New()

	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "app.log_level",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		DefaultValue:     "info",
		ValueType:        domain.ValueTypeString,
		ApplyBehavior:    domain.ApplyBundleRebuild,
		MutableAtRuntime: true,
	}))

	err = cm.SeedStore(context.Background(), store, reg)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrRevisionMismatch)

	// Should NOT enter seed mode when the store rejects the write.
	assert.False(t, cm.InSeedMode(), "should not enter seed mode when store has pre-existing data")
}

func TestSeedStore_NilConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)

	// Force nil config for this edge case.
	cm.config.Store(nil)

	store := &mockStore{}
	reg := registry.New()

	err = cm.SeedStore(context.Background(), store, reg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config is nil")
}
