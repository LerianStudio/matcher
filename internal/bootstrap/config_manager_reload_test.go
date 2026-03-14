// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func configChangeKeys(changes []ConfigChange) map[string]bool {
	keys := make(map[string]bool, len(changes))
	for _, change := range changes {
		keys[change.Key] = true
	}

	return keys
}

func TestReloadLocked_StoppedManager_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	cm.Stop()

	cm.mu.Lock()
	_, reloadErr := cm.reloadLocked("")
	cm.mu.Unlock()

	require.Error(t, reloadErr)
	assert.Contains(t, reloadErr.Error(), "config manager stopped")
}

func TestReloadLocked_ValidYAMLChange_PicksUpNewValues(t *testing.T) {
	// This test writes a YAML file, creates a ConfigManager pointing at it,
	// updates the YAML, then calls reloadLocked to verify the new value is picked up.
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `app:
  log_level: info
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, yamlPath, logger)
	require.NoError(t, err)

	// Overwrite with new log level.
	updatedYAML := `app:
  log_level: debug
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)
	assert.Equal(t, uint64(1), result.Version)

	got := cm.Get()
	assert.Equal(t, "debug", got.App.LogLevel)
}

func TestReloadLocked_ImmutableFetcherChangeRejected(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `fetcher:
  enabled: false
  discovery_interval_sec: 60
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, yamlPath, logger)
	require.NoError(t, err)

	updatedYAML := `fetcher:
  enabled: true
  discovery_interval_sec: 60
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	_, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.Error(t, reloadErr)
	assert.Contains(t, reloadErr.Error(), "immutable keys changed via file reload")
	assert.Contains(t, reloadErr.Error(), "fetcher.enabled")

	got := cm.Get()
	require.NotNil(t, got)
	assert.False(t, got.Fetcher.Enabled)
	assert.Equal(t, 60, got.Fetcher.DiscoveryIntervalSec)
}

func TestReloadLocked_MutableFetcherIntervalApplied(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `fetcher:
  enabled: false
  discovery_interval_sec: 60
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, yamlPath, logger)
	require.NoError(t, err)

	updatedYAML := `fetcher:
  enabled: false
  discovery_interval_sec: 120
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, result.ChangesDetected, 1)

	got := cm.Get()
	require.NotNil(t, got)
	assert.False(t, got.Fetcher.Enabled)
	assert.Equal(t, 120, got.Fetcher.DiscoveryIntervalSec)
}

func TestReloadLocked_InvalidYAML_PreservesExistingConfig(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `app:
  log_level: info
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, yamlPath, logger)
	require.NoError(t, err)

	// Write invalid YAML.
	require.NoError(t, os.WriteFile(yamlPath, []byte("{{{{invalid yaml"), 0o600))

	cm.mu.Lock()
	_, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.Error(t, reloadErr)

	// Existing config preserved — still has original log level.
	got := cm.Get()
	assert.Equal(t, "info", got.App.LogLevel)
}

func TestReloadLocked_EmptySource_DefaultsToReload(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)
	assert.Equal(t, uint64(1), result.Version)
}

func TestReloadLocked_PreservesStartupOnlyWorkerEnableFlags(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `export_worker:
  enabled: false
cleanup_worker:
  enabled: false
archival:
  enabled: false
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	cfg.ExportWorker.Enabled = false
	cfg.CleanupWorker.Enabled = false
	cfg.Archival.Enabled = false

	cm, err := NewConfigManager(cfg, yamlPath, &libLog.NopLogger{})
	require.NoError(t, err)

	updatedYAML := `export_worker:
  enabled: true
cleanup_worker:
  enabled: true
archival:
  enabled: true
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	got := cm.Get()
	changeKeys := configChangeKeys(result.Changes)
	assert.False(t, got.ExportWorker.Enabled)
	assert.False(t, got.CleanupWorker.Enabled)
	assert.False(t, got.Archival.Enabled)
	assert.False(t, changeKeys["export_worker.enabled"])
	assert.False(t, changeKeys["cleanup_worker.enabled"])
	assert.False(t, changeKeys["archival.enabled"])
}

func TestReloadLocked_PreservesStartupBoundAuthAndTenantDefaults(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `auth:
  enabled: true
  service_address: http://auth-startup:8080
  token_secret: startup-secret
tenancy:
  default_tenant_id: "11111111-1111-1111-1111-111111111111"
  default_tenant_slug: startup
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Host = "http://auth-startup:8080"
	cfg.Auth.TokenSecret = "startup-secret"
	cfg.Tenancy.DefaultTenantID = "11111111-1111-1111-1111-111111111111"
	cfg.Tenancy.DefaultTenantSlug = "startup"

	cm, err := NewConfigManager(cfg, yamlPath, &libLog.NopLogger{})
	require.NoError(t, err)

	updatedYAML := `auth:
  enabled: false
  service_address: http://auth-reload:9090
  token_secret: reloaded-secret
tenancy:
  default_tenant_id: "22222222-2222-2222-2222-222222222222"
  default_tenant_slug: reloaded
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	got := cm.Get()
	changeKeys := configChangeKeys(result.Changes)
	assert.True(t, got.Auth.Enabled)
	assert.Equal(t, "http://auth-startup:8080", got.Auth.Host)
	assert.Equal(t, "startup-secret", got.Auth.TokenSecret)
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", got.Tenancy.DefaultTenantID)
	assert.Equal(t, "startup", got.Tenancy.DefaultTenantSlug)
	assert.False(t, changeKeys["auth.enabled"])
	assert.False(t, changeKeys["auth.service_address"])
	assert.False(t, changeKeys["auth.token_secret"])
	assert.False(t, changeKeys["tenancy.default_tenant_id"])
	assert.False(t, changeKeys["tenancy.default_tenant_slug"])
}

func TestReloadLocked_PreservesStartupBoundMultiTenantInfrastructureSettings(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `tenancy:
  multi_tenant_enabled: true
  multi_tenant_url: https://tenant-manager-startup:4003
  multi_tenant_environment: production
  multi_tenant_max_tenant_pools: 25
  multi_tenant_idle_timeout_sec: 120
  multi_tenant_circuit_breaker_threshold: 6
  multi_tenant_circuit_breaker_timeout_sec: 45
  multi_tenant_service_api_key: startup-key
  multi_tenant_infra_enabled: true
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantInfraEnabled = true
	cfg.Tenancy.MultiTenantURL = "https://tenant-manager-startup:4003"
	cfg.Tenancy.MultiTenantEnvironment = "production"
	cfg.Tenancy.MultiTenantMaxTenantPools = 25
	cfg.Tenancy.MultiTenantIdleTimeoutSec = 120
	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = 6
	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 45
	cfg.Tenancy.MultiTenantServiceAPIKey = "startup-key"

	cm, err := NewConfigManager(cfg, yamlPath, &libLog.NopLogger{})
	require.NoError(t, err)

	updatedYAML := `tenancy:
  multi_tenant_enabled: false
  multi_tenant_url: https://tenant-manager-reload:5000
  multi_tenant_environment: staging
  multi_tenant_max_tenant_pools: 10
  multi_tenant_idle_timeout_sec: 60
  multi_tenant_circuit_breaker_threshold: 2
  multi_tenant_circuit_breaker_timeout_sec: 15
  multi_tenant_service_api_key: reloaded-key
  multi_tenant_infra_enabled: false
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	got := cm.Get()
	changeKeys := configChangeKeys(result.Changes)
	assert.True(t, got.Tenancy.MultiTenantEnabled)
	assert.True(t, got.Tenancy.MultiTenantInfraEnabled)
	assert.Equal(t, "https://tenant-manager-startup:4003", got.Tenancy.MultiTenantURL)
	assert.Equal(t, "production", got.Tenancy.MultiTenantEnvironment)
	assert.Equal(t, 25, got.Tenancy.MultiTenantMaxTenantPools)
	assert.Equal(t, 120, got.Tenancy.MultiTenantIdleTimeoutSec)
	assert.Equal(t, 6, got.Tenancy.MultiTenantCircuitBreakerThreshold)
	assert.Equal(t, 45, got.Tenancy.MultiTenantCircuitBreakerTimeoutSec)
	assert.Equal(t, "startup-key", got.Tenancy.MultiTenantServiceAPIKey)
	assert.False(t, changeKeys["tenancy.multi_tenant_enabled"])
	assert.False(t, changeKeys["tenancy.multi_tenant_infra_enabled"])
	assert.False(t, changeKeys["tenancy.multi_tenant_url"])
	assert.False(t, changeKeys["tenancy.multi_tenant_environment"])
	assert.False(t, changeKeys["tenancy.multi_tenant_max_tenant_pools"])
	assert.False(t, changeKeys["tenancy.multi_tenant_idle_timeout_sec"])
	assert.False(t, changeKeys["tenancy.multi_tenant_circuit_breaker_threshold"])
	assert.False(t, changeKeys["tenancy.multi_tenant_circuit_breaker_timeout_sec"])
	assert.False(t, changeKeys["tenancy.multi_tenant_service_api_key"])
}

func TestReloadLocked_PreservesStartupBoundObjectStorageSettings(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `object_storage:
  endpoint: http://object-storage-startup:8333
  region: us-east-1
  bucket: startup-bucket
  access_key_id: startup-key
  secret_access_key: startup-secret
  use_path_style: true
archival:
  storage_bucket: archival-startup
  storage_prefix: archives/startup
  storage_class: GLACIER
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	cfg.ObjectStorage.Endpoint = "http://object-storage-startup:8333"
	cfg.ObjectStorage.Region = "us-east-1"
	cfg.ObjectStorage.Bucket = "startup-bucket"
	cfg.ObjectStorage.AccessKeyID = "startup-key"
	cfg.ObjectStorage.SecretAccessKey = "startup-secret"
	cfg.ObjectStorage.UsePathStyle = true
	cfg.Archival.StorageBucket = "archival-startup"
	cfg.Archival.StoragePrefix = "archives/startup"
	cfg.Archival.StorageClass = "GLACIER"

	cm, err := NewConfigManager(cfg, yamlPath, &libLog.NopLogger{})
	require.NoError(t, err)

	updatedYAML := `object_storage:
  endpoint: http://object-storage-reload:9444
  region: eu-west-1
  bucket: reloaded-bucket
  access_key_id: reloaded-key
  secret_access_key: reloaded-secret
  use_path_style: false
archival:
  storage_bucket: archival-reloaded
  storage_prefix: archives/reloaded
  storage_class: STANDARD
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	got := cm.Get()
	changeKeys := configChangeKeys(result.Changes)
	assert.Equal(t, "http://object-storage-startup:8333", got.ObjectStorage.Endpoint)
	assert.Equal(t, "us-east-1", got.ObjectStorage.Region)
	assert.Equal(t, "startup-bucket", got.ObjectStorage.Bucket)
	assert.Equal(t, "startup-key", got.ObjectStorage.AccessKeyID)
	assert.Equal(t, "startup-secret", got.ObjectStorage.SecretAccessKey)
	assert.True(t, got.ObjectStorage.UsePathStyle)
	assert.Equal(t, "archival-startup", got.Archival.StorageBucket)
	assert.Equal(t, "archives/startup", got.Archival.StoragePrefix)
	assert.Equal(t, "GLACIER", got.Archival.StorageClass)
	assert.False(t, changeKeys["object_storage.endpoint"])
	assert.False(t, changeKeys["object_storage.bucket"])
	assert.False(t, changeKeys["archival.storage_bucket"])
	assert.False(t, changeKeys["archival.storage_prefix"])
	assert.False(t, changeKeys["archival.storage_class"])
}

func TestReloadLocked_PreservesStartupBoundDedupeTTL(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	require.NoError(t, os.WriteFile(yamlPath, []byte("deduplication:\n  ttl_sec: 3600\n"), 0o600))

	cfg := defaultConfig()
	cfg.Dedupe.TTLSec = 3600

	cm, err := NewConfigManager(cfg, yamlPath, &libLog.NopLogger{})
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(yamlPath, []byte("deduplication:\n  ttl_sec: 7200\n"), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	got := cm.Get()
	changeKeys := configChangeKeys(result.Changes)
	assert.Equal(t, 3600, got.Dedupe.TTLSec)
	assert.False(t, changeKeys["deduplication.ttl_sec"])
}
