// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateFromSystemplane_RejectNotInSeedMode(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	// Sanity: the manager is NOT in seed mode by default.
	require.False(t, cm.InSeedMode())

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level": {Value: "debug"},
		},
	}

	err = cm.UpdateFromSystemplane(snap)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in seed mode")
}

func TestUpdateFromSystemplane_Success(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	beforeUpdate := time.Now().UTC().Add(-time.Millisecond)

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level":  {Value: "debug"},
			"rate_limit.max": {Value: 999},
		},
	}

	err = cm.UpdateFromSystemplane(snap)
	require.NoError(t, err)

	// The atomic config pointer was updated.
	updated := cm.Get()
	require.NotNil(t, updated)
	assert.Equal(t, "debug", updated.App.LogLevel)
	assert.Equal(t, 999, updated.RateLimit.Max)

	// Version was incremented.
	assert.Equal(t, uint64(1), cm.Version())

	// lastReload was updated to a recent timestamp.
	assert.True(t, cm.LastReloadAt().After(beforeUpdate),
		"lastReload should be updated to a time after the call")
}

func TestUpdateFromSystemplane_PreservesBootstrapFields(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// Set distinctive bootstrap-only field values so we can verify they survive.
	cfg.App.EnvName = "staging-test"
	cfg.Server.Address = ":9999"
	cfg.Server.TLSCertFile = "/etc/tls/cert.pem"
	cfg.Server.TLSKeyFile = "/etc/tls/key.pem"
	cfg.Auth = AuthConfig{
		Enabled:     true,
		Host:        "https://auth.example.com",
		TokenSecret: "super-secret-jwt-key",
	}
	cfg.Telemetry = TelemetryConfig{
		Enabled:              true,
		ServiceName:          "matcher-staging",
		LibraryName:          "custom-lib",
		ServiceVersion:       "2.0.0",
		DeploymentEnv:        "staging",
		CollectorEndpoint:    "otel.example.com:4317",
		DBMetricsIntervalSec: 42,
	}
	cfg.Logger = &testLogger{}
	cfg.ShutdownGracePeriod = 15 * time.Second

	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	// An empty snapshot — all runtime fields will get defaults from
	// snapshotToFullConfig, but bootstrap fields must come from oldCfg.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{},
	}

	err = cm.UpdateFromSystemplane(snap)
	require.NoError(t, err)

	updated := cm.Get()
	require.NotNil(t, updated)

	// Bootstrap-only fields preserved.
	assert.Equal(t, "staging-test", updated.App.EnvName)
	assert.Equal(t, ":9999", updated.Server.Address)
	assert.Equal(t, "/etc/tls/cert.pem", updated.Server.TLSCertFile)
	assert.Equal(t, "/etc/tls/key.pem", updated.Server.TLSKeyFile)
	assert.Equal(t, cfg.Auth, updated.Auth)
	assert.Equal(t, cfg.Telemetry, updated.Telemetry)
	assert.Equal(t, cfg.Logger, updated.Logger)
	assert.Equal(t, 15*time.Second, updated.ShutdownGracePeriod)
}

func TestUpdateFromSystemplane_IncrementsVersion(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	assert.Equal(t, uint64(0), cm.Version(), "initial version should be 0")

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level": {Value: "warn"},
		},
	}

	err = cm.UpdateFromSystemplane(snap)
	require.NoError(t, err)

	assert.Equal(t, uint64(1), cm.Version(), "version should be 1 after first update")

	// Second update should increment to 2.
	err = cm.UpdateFromSystemplane(snap)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), cm.Version(), "version should be 2 after second update")
}

func TestSnapshotToFullConfig_RuntimeFields(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level":           {Value: "debug"},
			"rate_limit.max":          {Value: 200},
			"fetcher.enabled":         {Value: true},
			"archival.storage_bucket": {Value: "test-bucket"},
			"webhook.timeout_sec":     {Value: 45},
			"scheduler.interval_sec":  {Value: 120},
		},
	}

	result := snapshotToFullConfig(snap, oldCfg)

	assert.Equal(t, "debug", result.App.LogLevel)
	assert.Equal(t, 200, result.RateLimit.Max)
	assert.True(t, result.Fetcher.Enabled)
	assert.Equal(t, "test-bucket", result.Archival.StorageBucket)
	assert.Equal(t, 45, result.Webhook.TimeoutSec)
	assert.Equal(t, 120, result.Scheduler.IntervalSec)
}

func TestUpdateFromSystemplane_ValidationFailure_PreservesOldConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	initialVersion := cm.Version()
	initialLogLevel := cm.Get().App.LogLevel

	// Craft a snapshot with an invalid tenant ID to trigger Validate() failure.
	// Validate() calls libCommons.IsUUID(cfg.Tenancy.DefaultTenantID) — "not-a-uuid"
	// is not a valid UUID, so validation must fail.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"tenancy.default_tenant_id": {Value: "not-a-uuid"},
		},
	}

	err = cm.UpdateFromSystemplane(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validation")

	// Config and version should NOT have changed — the failed update was discarded.
	assert.Equal(t, initialLogLevel, cm.Get().App.LogLevel)
	assert.Equal(t, initialVersion, cm.Version())
}

func TestUpdateFromSystemplane_CanChangeFormerlyImmutableKeys(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.Enabled = false
	cfg.ExportWorker.Enabled = false

	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	// Keys like fetcher.enabled and export_worker.enabled were formerly immutable
	// during file-based reload. Via the systemplane path they must be changeable.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"fetcher.enabled":       {Value: true},
			"export_worker.enabled": {Value: true},
		},
	}

	require.NoError(t, cm.UpdateFromSystemplane(snap))
	assert.True(t, cm.Get().Fetcher.Enabled)
	assert.True(t, cm.Get().ExportWorker.Enabled)
}

func TestUpdateFromSystemplane_NilOldConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	// Force a nil config pointer to exercise the nil guard in UpdateFromSystemplane.
	cm.config.Store(nil)

	snap := domain.Snapshot{}
	err = cm.UpdateFromSystemplane(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpectedly nil")
}

func TestSnapshotToFullConfig_BootstrapFieldsPreserved(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	oldCfg.App.EnvName = "production"
	oldCfg.Server.Address = ":8080"
	oldCfg.Server.TLSCertFile = "/tls/cert.pem"
	oldCfg.Server.TLSKeyFile = "/tls/key.pem"
	oldCfg.Server.TLSTerminatedUpstream = true
	oldCfg.Server.TrustedProxies = "10.0.0.0/8"
	oldCfg.Auth = AuthConfig{
		Enabled:     true,
		Host:        "https://auth.prod.example.com",
		TokenSecret: "prod-jwt-secret",
	}
	oldCfg.Telemetry = TelemetryConfig{
		Enabled:              true,
		ServiceName:          "matcher-prod",
		LibraryName:          "prod-lib",
		ServiceVersion:       "3.0.0",
		DeploymentEnv:        "production",
		CollectorEndpoint:    "otel-prod.example.com:4317",
		DBMetricsIntervalSec: 30,
	}
	oldCfg.Logger = &testLogger{}
	oldCfg.ShutdownGracePeriod = 30 * time.Second

	// Snapshot has no keys — all runtime fields get defaults, but bootstrap
	// fields must be copied from oldCfg regardless.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{},
	}

	result := snapshotToFullConfig(snap, oldCfg)

	// Bootstrap-only fields come from oldCfg, not from snapshot defaults.
	assert.Equal(t, "production", result.App.EnvName)
	assert.Equal(t, ":8080", result.Server.Address)
	assert.Equal(t, "/tls/cert.pem", result.Server.TLSCertFile)
	assert.Equal(t, "/tls/key.pem", result.Server.TLSKeyFile)
	assert.True(t, result.Server.TLSTerminatedUpstream)
	assert.Equal(t, "10.0.0.0/8", result.Server.TrustedProxies)
	assert.Equal(t, oldCfg.Auth, result.Auth)
	assert.Equal(t, oldCfg.Telemetry, result.Telemetry)
	assert.Equal(t, oldCfg.Logger, result.Logger)
	assert.Equal(t, 30*time.Second, result.ShutdownGracePeriod)

	// Verify runtime fields got defaults (not zeros) since snapshot is empty.
	assert.Equal(t, "info", result.App.LogLevel, "runtime field should get default, not zero")
	assert.Equal(t, 100, result.RateLimit.Max, "runtime field should get default, not zero")
	assert.True(t, result.RateLimit.Enabled, "runtime field should get default")
}
