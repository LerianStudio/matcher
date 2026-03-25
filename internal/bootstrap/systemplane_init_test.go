// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
)

type seedInitStoreMock struct {
	putErr error
}

func (m *seedInitStoreMock) Get(_ context.Context, _ domain.Target) (ports.ReadResult, error) {
	return ports.ReadResult{}, nil
}

func (m *seedInitStoreMock) Put(
	_ context.Context,
	_ domain.Target,
	_ []ports.WriteOp,
	_ domain.Revision,
	_ domain.Actor,
	_ string,
) (domain.Revision, error) {
	return domain.RevisionZero, m.putErr
}

// ---------------------------------------------------------------------------
// ExtractBootstrapOnlyConfig
// ---------------------------------------------------------------------------

func TestExtractBootstrapOnlyConfig_NilConfig(t *testing.T) {
	t.Parallel()

	result, err := ExtractBootstrapOnlyConfig(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
	assert.Nil(t, result)
}

func TestExtractBootstrapOnlyConfig_Success(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		App: AppConfig{
			EnvName: "staging",
		},
		Server: ServerConfig{
			Address:               ":9090",
			TLSCertFile:           "/certs/tls.crt",
			TLSKeyFile:            "/certs/tls.key",
			TLSTerminatedUpstream: true,
			TrustedProxies:        "10.0.0.0/8",
		},
		Auth: AuthConfig{
			Enabled:     true,
			Host:        "https://auth.example.com",
			TokenSecret: "super-secret",
		},
		Telemetry: TelemetryConfig{
			Enabled:              true,
			ServiceName:          "matcher-staging",
			LibraryName:          "github.com/LerianStudio/matcher",
			ServiceVersion:       "2.0.0",
			DeploymentEnv:        "staging",
			CollectorEndpoint:    "otel-collector:4317",
			DBMetricsIntervalSec: 30,
		},
	}

	result, err := ExtractBootstrapOnlyConfig(cfg)

	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, "staging", result.EnvName)
	assert.Equal(t, ":9090", result.ServerAddress)
	assert.Equal(t, "/certs/tls.crt", result.TLSCertFile)
	assert.Equal(t, "/certs/tls.key", result.TLSKeyFile)
	assert.True(t, result.TLSTerminatedUpstream)
	assert.Equal(t, "10.0.0.0/8", result.TrustedProxies)
	assert.True(t, result.AuthEnabled)
	assert.Equal(t, "https://auth.example.com", result.AuthHost)
	assert.Equal(t, "super-secret", result.AuthTokenSecret)
	assert.True(t, result.TelemetryEnabled)
	assert.Equal(t, "matcher-staging", result.TelemetryServiceName)
	assert.Equal(t, "github.com/LerianStudio/matcher", result.TelemetryLibraryName)
	assert.Equal(t, "2.0.0", result.TelemetryServiceVersion)
	assert.Equal(t, "staging", result.TelemetryDeploymentEnv)
	assert.Equal(t, "otel-collector:4317", result.TelemetryCollectorEndpoint)
	assert.Equal(t, 30, result.TelemetryDBMetricsInterval)
}

func TestExtractBootstrapOnlyConfig_DefaultValues(t *testing.T) {
	t.Parallel()

	cfg := &Config{}

	result, err := ExtractBootstrapOnlyConfig(cfg)

	require.NoError(t, err)
	require.NotNil(t, result)

	// All fields should be zero values since Config has no values set.
	assert.Empty(t, result.EnvName)
	assert.Empty(t, result.ServerAddress)
	assert.Empty(t, result.TLSCertFile)
	assert.Empty(t, result.TLSKeyFile)
	assert.False(t, result.TLSTerminatedUpstream)
	assert.Empty(t, result.TrustedProxies)
	assert.False(t, result.AuthEnabled)
	assert.Empty(t, result.AuthHost)
	assert.Empty(t, result.AuthTokenSecret)
	assert.False(t, result.TelemetryEnabled)
	assert.Empty(t, result.TelemetryServiceName)
	assert.Empty(t, result.TelemetryLibraryName)
	assert.Empty(t, result.TelemetryServiceVersion)
	assert.Empty(t, result.TelemetryDeploymentEnv)
	assert.Empty(t, result.TelemetryCollectorEndpoint)
	assert.Zero(t, result.TelemetryDBMetricsInterval)
}

// ---------------------------------------------------------------------------
// LoadSystemplaneBackendConfig
// ---------------------------------------------------------------------------

func TestLoadSystemplaneBackendConfig_DefaultsToPostgres(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "0123456789abcdef0123456789abcdef")

	// Provide a minimal app config so the DSN is constructed from it.
	appCfg := &Config{
		Postgres: PostgresConfig{
			PrimaryHost:     "pg-host",
			PrimaryPort:     "5432",
			PrimaryUser:     "pguser",
			PrimaryPassword: "pgpass",
			PrimaryDB:       "pgdb",
			PrimarySSLMode:  "require",
		},
	}

	result, err := LoadSystemplaneBackendConfig(appCfg)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, domain.BackendPostgres, result.Backend)
	assert.NotNil(t, result.Postgres)
	assert.Contains(t, result.Postgres.DSN, "pg-host")
	assert.Contains(t, result.Postgres.DSN, "pguser")
	assert.Contains(t, result.Postgres.DSN, "pgdb")
	assert.Equal(t, "0123456789abcdef0123456789abcdef", result.Secrets.MasterKey)
}

func TestLoadSystemplaneBackendConfig_InvalidBackend(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "cassandra")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "0123456789abcdef0123456789abcdef")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidBackendKind)
	assert.Nil(t, result)
}

func TestLoadSystemplaneBackendConfig_ExplicitPostgresDSN(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "postgres")
	t.Setenv("SYSTEMPLANE_POSTGRES_DSN", "postgres://user:pass@sp-host:5432/spdb?sslmode=disable")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "0123456789abcdef0123456789abcdef")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, domain.BackendPostgres, result.Backend)
	assert.Equal(t, "postgres://user:pass@sp-host:5432/spdb?sslmode=disable", result.Postgres.DSN)
}

func TestLoadSystemplaneBackendConfig_PostgresSchemaOverrideRejected(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "postgres")
	t.Setenv("SYSTEMPLANE_POSTGRES_DSN", "postgres://u:p@h:5432/d?sslmode=disable")
	t.Setenv("SYSTEMPLANE_POSTGRES_SCHEMA", "custom_schema")
	t.Setenv("SYSTEMPLANE_POSTGRES_ENTRIES_TABLE", "custom_entries")
	t.Setenv("SYSTEMPLANE_POSTGRES_HISTORY_TABLE", "custom_history")
	t.Setenv("SYSTEMPLANE_POSTGRES_REVISION_TABLE", "custom_revisions")
	t.Setenv("SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL", "custom_channel")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "0123456789abcdef0123456789abcdef")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.ErrorIs(t, err, errSystemplaneCustomPostgresStore)
	assert.Nil(t, result)
}

func TestLoadSystemplaneBackendConfig_MongoBackend(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "mongodb")
	t.Setenv("SYSTEMPLANE_MONGODB_URI", "mongodb://localhost:27017")
	t.Setenv("SYSTEMPLANE_MONGODB_DATABASE", "test_sp")
	t.Setenv("SYSTEMPLANE_MONGODB_ENTRIES_COLLECTION", "entries_override")
	t.Setenv("SYSTEMPLANE_MONGODB_HISTORY_COLLECTION", "history_override")
	t.Setenv("SYSTEMPLANE_MONGODB_WATCH_MODE", "poll")
	t.Setenv("SYSTEMPLANE_MONGODB_POLL_INTERVAL_SEC", "30")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "0123456789abcdef0123456789abcdef")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, domain.BackendMongoDB, result.Backend)
	assert.NotNil(t, result.MongoDB)
	assert.Equal(t, "mongodb://localhost:27017", result.MongoDB.URI)
	assert.Equal(t, "test_sp", result.MongoDB.Database)
	assert.Equal(t, "entries_override", result.MongoDB.EntriesCollection)
	assert.Equal(t, "history_override", result.MongoDB.HistoryCollection)
	assert.Equal(t, "poll", result.MongoDB.WatchMode)
	assert.Equal(t, 30*time.Second, result.MongoDB.PollInterval)
}

func TestLoadSystemplaneBackendConfig_MongoMissingURI(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "mongodb")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "0123456789abcdef0123456789abcdef")
	// No SYSTEMPLANE_MONGO_URI set

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "validate systemplane config")
}

func TestLoadSystemplaneBackendConfig_PostgresFallbackFromAppConfig(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "postgres")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "0123456789abcdef0123456789abcdef")
	// No SYSTEMPLANE_POSTGRES_DSN — should fall back to app config.

	appCfg := &Config{
		App: AppConfig{EnvName: "development"},
		Postgres: PostgresConfig{
			PrimaryHost:     "matcher-pg",
			PrimaryPort:     "5433",
			PrimaryUser:     "matcher",
			PrimaryPassword: "se:cr@t/with?chars",
			PrimaryDB:       "matcher_db",
			PrimarySSLMode:  "verify-full",
		},
	}

	result, err := LoadSystemplaneBackendConfig(appCfg)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result.Postgres.DSN, "matcher-pg")
	assert.Contains(t, result.Postgres.DSN, "5433")
	assert.Contains(t, result.Postgres.DSN, "matcher_db")
	assert.Contains(t, result.Postgres.DSN, "se%3Acr%40t%2Fwith%3Fchars")
}

func TestLoadSystemplaneBackendConfig_ProductionRequiresSecretMasterKey(t *testing.T) {
	t.Setenv("SYSTEMPLANE_BACKEND", "postgres")
	t.Setenv("SYSTEMPLANE_POSTGRES_DSN", "postgres://user:pass@host:5432/db?sslmode=disable")
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "")

	result, err := LoadSystemplaneBackendConfig(&Config{App: AppConfig{EnvName: "production"}})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "SYSTEMPLANE_SECRET_MASTER_KEY is required")
}

// ---------------------------------------------------------------------------
// ExtractBundleFromSupervisor
// ---------------------------------------------------------------------------

// mockSupervisorForExtract is a minimal Supervisor mock that returns a
// configurable RuntimeBundle from Current().
type mockSupervisorForExtract struct {
	bundle domain.RuntimeBundle
}

func (m *mockSupervisorForExtract) Current() domain.RuntimeBundle { return m.bundle }

func (m *mockSupervisorForExtract) Snapshot() domain.Snapshot { return domain.Snapshot{} }

func (m *mockSupervisorForExtract) PublishSnapshot(_ context.Context, _ domain.Snapshot, _ string) error {
	return nil
}

func (m *mockSupervisorForExtract) ReconcileCurrent(_ context.Context, _ domain.Snapshot, _ string) error {
	return nil
}

func (m *mockSupervisorForExtract) Reload(_ context.Context, _ string, _ ...string) error {
	return nil
}

func (m *mockSupervisorForExtract) Stop(_ context.Context) error { return nil }

// Compile-time interface check.
var _ service.Supervisor = (*mockSupervisorForExtract)(nil)

// ---------------------------------------------------------------------------
// StartChangeFeed
// ---------------------------------------------------------------------------

func TestStartChangeFeed_NilFeed(t *testing.T) {
	t.Parallel()

	mock := &mockSupervisorForExtract{}

	cancel, err := startChangeFeed(context.Background(), nil, mock, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, cancel)

	// Should be a no-op; calling it should not panic.
	cancel()
}

type changeFeedTestStub struct {
	subscribe func(context.Context, func(ports.ChangeSignal)) error
}

func (stub *changeFeedTestStub) Subscribe(ctx context.Context, handler func(ports.ChangeSignal)) error {
	return stub.subscribe(ctx, handler)
}

type reloadCountingSupervisor struct {
	reloads    atomic.Int32
	lastReason atomic.Value
}

func (supervisor *reloadCountingSupervisor) Current() domain.RuntimeBundle { return nil }
func (supervisor *reloadCountingSupervisor) Snapshot() domain.Snapshot     { return domain.Snapshot{} }
func (supervisor *reloadCountingSupervisor) PublishSnapshot(context.Context, domain.Snapshot, string) error {
	return nil
}

func (supervisor *reloadCountingSupervisor) ReconcileCurrent(context.Context, domain.Snapshot, string) error {
	return nil
}

func (supervisor *reloadCountingSupervisor) Reload(_ context.Context, reason string, _ ...string) error {
	supervisor.reloads.Add(1)
	supervisor.lastReason.Store(reason)
	return nil
}
func (supervisor *reloadCountingSupervisor) Stop(context.Context) error { return nil }

func TestStartChangeFeed_ReloadsSupervisorOnSignal(t *testing.T) {
	t.Parallel()

	supervisor := &reloadCountingSupervisor{}
	feed := &changeFeedTestStub{subscribe: func(ctx context.Context, handler func(ports.ChangeSignal)) error {
		handler(ports.ChangeSignal{})
		<-ctx.Done()
		return nil
	}}

	cancel, err := startChangeFeed(context.Background(), feed, supervisor, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, cancel)
	defer cancel()

	require.Eventually(t, func() bool {
		return supervisor.reloads.Load() == 1
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return supervisor.lastReason.Load() == "changefeed"
	}, time.Second, 10*time.Millisecond)
}

func TestStartChangeFeed_RetriesAfterSubscribeFailure(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	supervisor := &reloadCountingSupervisor{}
	feed := &changeFeedTestStub{subscribe: func(ctx context.Context, handler func(ports.ChangeSignal)) error {
		attempt := attempts.Add(1)
		if attempt == 1 {
			return errors.New("temporary disconnect")
		}
		handler(ports.ChangeSignal{})
		<-ctx.Done()
		return nil
	}}

	cancel, err := startChangeFeed(context.Background(), feed, supervisor, &libLog.NopLogger{}, nil)
	require.NoError(t, err)
	defer cancel()

	require.Eventually(t, func() bool {
		return attempts.Load() >= 2 && supervisor.reloads.Load() == 1
	}, 3*time.Second, 20*time.Millisecond)
}

func TestStartChangeFeed_ApplySignalFailure_CancelsAndRetries(t *testing.T) {
	t.Parallel()

	var subscribeAttempts atomic.Int32
	var applyAttempts atomic.Int32
	supervisor := &reloadCountingSupervisor{}
	feed := &changeFeedTestStub{subscribe: func(ctx context.Context, handler func(ports.ChangeSignal)) error {
		subscribeAttempts.Add(1)
		handler(ports.ChangeSignal{Target: domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}})
		<-ctx.Done()
		return ctx.Err()
	}}

	cancel, err := startChangeFeed(context.Background(), feed, supervisor, &libLog.NopLogger{}, func(context.Context, ports.ChangeSignal) error {
		if applyAttempts.Add(1) == 1 {
			return errors.New("transient apply failure")
		}
		return nil
	})
	require.NoError(t, err)
	defer cancel()

	require.Eventually(t, func() bool {
		return subscribeAttempts.Load() >= 2 && applyAttempts.Load() >= 2
	}, 3*time.Second, 20*time.Millisecond)
}

// ---------------------------------------------------------------------------
// InitSystemplane
// ---------------------------------------------------------------------------

func TestInitSystemplane_NilConfig(t *testing.T) {
	t.Parallel()

	result, err := InitSystemplane(context.Background(), nil, nil, nil, nil, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
	assert.Nil(t, result)
}

func TestSeedStoreForInitialReload_NoConfigManager(t *testing.T) {
	t.Parallel()

	err := seedStoreForInitialReload(context.Background(), nil, nil, nil)

	require.NoError(t, err)
}

func TestSeedStoreForInitialReload_RevisionMismatch_EntersSeedMode(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Max = 999 // non-default forces SeedStore Put path

	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	store := &seedInitStoreMock{putErr: domain.ErrRevisionMismatch}

	err = seedStoreForInitialReload(context.Background(), cm, store, reg)

	require.NoError(t, err)
	assert.True(t, cm.InSeedMode())
}

func TestSeedStoreForInitialReload_UnexpectedSeedError(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Max = 999 // non-default forces SeedStore Put path

	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	store := &seedInitStoreMock{putErr: errors.New("boom")}

	err = seedStoreForInitialReload(context.Background(), cm, store, reg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "seed store")
	assert.False(t, cm.InSeedMode())
}

// ---------------------------------------------------------------------------
// reloadObserver
// ---------------------------------------------------------------------------

func TestReloadObserver_NilLogger_ReturnsNil(t *testing.T) {
	t.Parallel()

	obs := reloadObserver(context.Background(), nil)

	assert.Nil(t, obs, "reloadObserver should return nil when logger is nil")
}

func TestReloadObserver_WithLogger_ReturnsCallback(t *testing.T) {
	t.Parallel()

	obs := reloadObserver(context.Background(), &libLog.NopLogger{})

	require.NotNil(t, obs, "reloadObserver should return a non-nil callback when logger is present")

	// Should not panic when called.
	obs(service.ReloadEvent{Strategy: service.BuildStrategyFull, Reason: "test"})
	obs(service.ReloadEvent{Strategy: service.BuildStrategyIncremental, Reason: "config-change"})
}

// ---------------------------------------------------------------------------
// SystemplaneComponents
// ---------------------------------------------------------------------------

func TestSystemplaneComponents_AllFieldsAccessible(t *testing.T) {
	t.Parallel()

	// Verify the struct can be constructed with all fields set.
	// This is a compile-time + runtime sanity check, not a behavioral test.
	comp := &SystemplaneComponents{
		ChangeFeed: nil,
		Supervisor: nil,
		Manager:    nil,
		Backend:    nil,
	}

	assert.Nil(t, comp.ChangeFeed)
	assert.Nil(t, comp.Supervisor)
	assert.Nil(t, comp.Manager)
	assert.Nil(t, comp.Backend)
}

// ---------------------------------------------------------------------------
// buildReconcilers (internal helper)
// ---------------------------------------------------------------------------

func TestBuildReconcilers_NilBothManagers(t *testing.T) {
	t.Parallel()

	reconcilers, err := buildReconcilers(nil, nil, nil)

	require.NoError(t, err)
	require.Len(t, reconcilers, 2)
	assert.Equal(t, "http-policy-reconciler", reconcilers[0].Name())
	assert.Equal(t, "publisher-reconciler", reconcilers[1].Name())
}

func TestBuildReconcilers_WithWorkerManager(t *testing.T) {
	t.Parallel()

	wm := NewWorkerManager(nil, nil)

	reconcilers, err := buildReconcilers(wm, nil, nil)

	require.NoError(t, err)
	require.Len(t, reconcilers, 3)
	assert.Equal(t, "http-policy-reconciler", reconcilers[0].Name())
	assert.Equal(t, "publisher-reconciler", reconcilers[1].Name())
	assert.Equal(t, "worker-reconciler", reconcilers[2].Name())
}

// ---------------------------------------------------------------------------
// loadSystemplanePostgresConfig (internal helper)
// ---------------------------------------------------------------------------

func TestLoadSystemplanePostgresConfig_NoEnvNoAppConfig(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_POSTGRES_DSN", "")
	t.Setenv("SYSTEMPLANE_POSTGRES_SCHEMA", "")
	t.Setenv("SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL", "")

	result := loadSystemplanePostgresConfig(nil)

	require.NotNil(t, result)
	assert.Empty(t, result.DSN)
	assert.Empty(t, result.Schema)
	assert.Empty(t, result.NotifyChannel)
}

func TestLoadSystemplanePostgresConfig_EnvOverridesAppConfig(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_POSTGRES_DSN", "postgres://explicit:dsn@host:5432/db")
	t.Setenv("SYSTEMPLANE_POSTGRES_SCHEMA", "")
	t.Setenv("SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL", "")

	appCfg := &Config{
		Postgres: PostgresConfig{
			PrimaryHost: "should-not-appear",
		},
	}

	result := loadSystemplanePostgresConfig(appCfg)

	require.NotNil(t, result)
	assert.Equal(t, "postgres://explicit:dsn@host:5432/db", result.DSN)
	assert.NotContains(t, result.DSN, "should-not-appear")
}

// ---------------------------------------------------------------------------
// loadSystemplaneMongoConfig (internal helper)
// ---------------------------------------------------------------------------

func TestLoadSystemplaneMongoConfig_NoEnvVars(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_MONGODB_URI", "")
	t.Setenv("SYSTEMPLANE_MONGODB_DATABASE", "")
	t.Setenv("SYSTEMPLANE_MONGODB_WATCH_MODE", "")

	result := loadSystemplaneMongoConfig()

	require.NotNil(t, result)
	assert.Empty(t, result.URI)
	assert.Empty(t, result.Database)
	assert.Empty(t, result.WatchMode)
}

func TestLoadSystemplaneMongoConfig_AllEnvVars(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_MONGODB_URI", "mongodb://mongo:27017")
	t.Setenv("SYSTEMPLANE_MONGODB_DATABASE", "sp_test")
	t.Setenv("SYSTEMPLANE_MONGODB_WATCH_MODE", "change_stream")

	result := loadSystemplaneMongoConfig()

	require.NotNil(t, result)
	assert.Equal(t, "mongodb://mongo:27017", result.URI)
	assert.Equal(t, "sp_test", result.Database)
	assert.Equal(t, "change_stream", result.WatchMode)
}
