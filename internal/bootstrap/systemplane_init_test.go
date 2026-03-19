// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
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
}

func TestLoadSystemplaneBackendConfig_InvalidBackend(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "cassandra")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidBackendKind)
	assert.Nil(t, result)
}

func TestLoadSystemplaneBackendConfig_ExplicitPostgresDSN(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "postgres")
	t.Setenv("SYSTEMPLANE_POSTGRES_DSN", "postgres://user:pass@sp-host:5432/spdb?sslmode=disable")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, domain.BackendPostgres, result.Backend)
	assert.Equal(t, "postgres://user:pass@sp-host:5432/spdb?sslmode=disable", result.Postgres.DSN)
}

func TestLoadSystemplaneBackendConfig_PostgresSchemaOverride(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "postgres")
	t.Setenv("SYSTEMPLANE_POSTGRES_DSN", "postgres://u:p@h:5432/d?sslmode=disable")
	t.Setenv("SYSTEMPLANE_POSTGRES_SCHEMA", "custom_schema")
	t.Setenv("SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL", "custom_channel")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "custom_schema", result.Postgres.Schema)
	assert.Equal(t, "custom_channel", result.Postgres.NotifyChannel)
}

func TestLoadSystemplaneBackendConfig_MongoBackend(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "mongodb")
	t.Setenv("SYSTEMPLANE_MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("SYSTEMPLANE_MONGO_DATABASE", "test_sp")
	t.Setenv("SYSTEMPLANE_MONGO_WATCH_MODE", "poll")

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, domain.BackendMongoDB, result.Backend)
	assert.NotNil(t, result.MongoDB)
	assert.Equal(t, "mongodb://localhost:27017", result.MongoDB.URI)
	assert.Equal(t, "test_sp", result.MongoDB.Database)
	assert.Equal(t, "poll", result.MongoDB.WatchMode)
}

func TestLoadSystemplaneBackendConfig_MongoMissingURI(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "mongodb")
	// No SYSTEMPLANE_MONGO_URI set

	result, err := LoadSystemplaneBackendConfig(&Config{})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "validate systemplane config")
}

func TestLoadSystemplaneBackendConfig_PostgresFallbackFromAppConfig(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_BACKEND", "postgres")
	// No SYSTEMPLANE_POSTGRES_DSN — should fall back to app config.

	appCfg := &Config{
		Postgres: PostgresConfig{
			PrimaryHost:     "matcher-pg",
			PrimaryPort:     "5433",
			PrimaryUser:     "matcher",
			PrimaryPassword: "secret",
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

func (m *mockSupervisorForExtract) Reload(_ context.Context, _ string) error { return nil }

func (m *mockSupervisorForExtract) Stop(_ context.Context) error { return nil }

// Compile-time interface check.
var _ service.Supervisor = (*mockSupervisorForExtract)(nil)

func TestExtractBundleFromSupervisor_NilSupervisor(t *testing.T) {
	t.Parallel()

	result, err := ExtractBundleFromSupervisor(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errSupervisorNil)
	assert.Nil(t, result)
}

func TestExtractBundleFromSupervisor_NilBundle(t *testing.T) {
	t.Parallel()

	mock := &mockSupervisorForExtract{bundle: nil}

	result, err := ExtractBundleFromSupervisor(mock)

	require.Error(t, err)
	assert.ErrorIs(t, err, errNoCurrentBundle)
	assert.Nil(t, result)
}

func TestExtractBundleFromSupervisor_WrongType(t *testing.T) {
	t.Parallel()

	mock := &mockSupervisorForExtract{bundle: &wrongBundle{}}

	result, err := ExtractBundleFromSupervisor(mock)

	require.Error(t, err)
	assert.ErrorIs(t, err, errUnexpectedBundleType)
	assert.Nil(t, result)
}

// wrongBundle is a RuntimeBundle that is not a *MatcherBundle.
type wrongBundle struct{}

func (w *wrongBundle) Close(_ context.Context) error { return nil }

func TestExtractBundleFromSupervisor_Success(t *testing.T) {
	t.Parallel()

	expected := &MatcherBundle{
		HTTP: &HTTPPolicyBundle{
			BodyLimitBytes: 42,
		},
	}
	mock := &mockSupervisorForExtract{bundle: expected}

	result, err := ExtractBundleFromSupervisor(mock)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, expected, result)
	assert.Equal(t, 42, result.HTTP.BodyLimitBytes)
}

// ---------------------------------------------------------------------------
// StartChangeFeed
// ---------------------------------------------------------------------------

func TestStartChangeFeed_NilFeed(t *testing.T) {
	t.Parallel()

	mock := &mockSupervisorForExtract{}

	cancel, err := StartChangeFeed(context.Background(), nil, mock)

	require.NoError(t, err)
	require.NotNil(t, cancel)

	// Should be a no-op; calling it should not panic.
	cancel()
}

// ---------------------------------------------------------------------------
// InitSystemplane
// ---------------------------------------------------------------------------

func TestInitSystemplane_NilConfig(t *testing.T) {
	t.Parallel()

	result, err := InitSystemplane(context.Background(), nil, nil, nil)

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
// SystemplaneComponents
// ---------------------------------------------------------------------------

func TestSystemplaneComponents_AllFieldsAccessible(t *testing.T) {
	t.Parallel()

	// Verify the struct can be constructed with all fields set.
	// This is a compile-time + runtime sanity check, not a behavioral test.
	comp := &SystemplaneComponents{
		Registry:   nil,
		Store:      nil,
		History:    nil,
		ChangeFeed: nil,
		Builder:    nil,
		Supervisor: nil,
		Manager:    nil,
		Backend:    nil,
	}

	assert.Nil(t, comp.Registry)
	assert.Nil(t, comp.Store)
	assert.Nil(t, comp.History)
	assert.Nil(t, comp.ChangeFeed)
	assert.Nil(t, comp.Builder)
	assert.Nil(t, comp.Supervisor)
	assert.Nil(t, comp.Manager)
	assert.Nil(t, comp.Backend)
}

// ---------------------------------------------------------------------------
// buildReconcilers (internal helper)
// ---------------------------------------------------------------------------

func TestBuildReconcilers_NilBothManagers(t *testing.T) {
	t.Parallel()

	reconcilers, err := buildReconcilers(nil, nil)

	require.NoError(t, err)
	require.Len(t, reconcilers, 1)
	assert.Equal(t, "http-policy-reconciler", reconcilers[0].Name())
}

func TestBuildReconcilers_WithWorkerManager(t *testing.T) {
	t.Parallel()

	wm := NewWorkerManager(nil, nil)

	reconcilers, err := buildReconcilers(nil, wm)

	require.NoError(t, err)
	require.Len(t, reconcilers, 2)
	assert.Equal(t, "http-policy-reconciler", reconcilers[0].Name())
	assert.Equal(t, "worker-reconciler", reconcilers[1].Name())
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
	t.Setenv("SYSTEMPLANE_MONGO_URI", "")
	t.Setenv("SYSTEMPLANE_MONGO_DATABASE", "")
	t.Setenv("SYSTEMPLANE_MONGO_WATCH_MODE", "")

	result := loadSystemplaneMongoConfig()

	require.NotNil(t, result)
	assert.Empty(t, result.URI)
	assert.Empty(t, result.Database)
	assert.Empty(t, result.WatchMode)
}

func TestLoadSystemplaneMongoConfig_AllEnvVars(t *testing.T) {
	// Cannot use t.Parallel() due to t.Setenv.
	t.Setenv("SYSTEMPLANE_MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("SYSTEMPLANE_MONGO_DATABASE", "sp_test")
	t.Setenv("SYSTEMPLANE_MONGO_WATCH_MODE", "change_stream")

	result := loadSystemplaneMongoConfig()

	require.NotNil(t, result)
	assert.Equal(t, "mongodb://mongo:27017", result.URI)
	assert.Equal(t, "sp_test", result.Database)
	assert.Equal(t, "change_stream", result.WatchMode)
}
