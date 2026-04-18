// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"sync/atomic"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ConfigManager manages configuration for the Matcher service.
//
// Thread-safety model:
//   - Readers call Get() which uses atomic.Pointer -- lock-free, zero contention.
//   - Writers call Update() which atomically swaps the config pointer.
//
// Lifecycle: NewConfigManager() -> Get() -> Stop().
//
// After systemplane initialization, runtime config changes flow through the
// systemplane Client's OnChange callbacks, which call Update() to refresh
// the config pointer.
type ConfigManager struct {
	config atomic.Pointer[Config]
	logger libLog.Logger
}

// NewConfigManager creates a ConfigManager that wraps the given initial config.
func NewConfigManager(cfg *Config, logger libLog.Logger) (*ConfigManager, error) {
	if cfg == nil {
		return nil, ErrConfigNil
	}

	if sharedPorts.IsNilValue(logger) {
		logger = &libLog.NopLogger{}
	}

	cm := &ConfigManager{
		logger: logger,
	}

	cm.config.Store(cfg)

	return cm, nil
}

// Get returns the current configuration. This is the hot path -- it uses an
// atomic load with zero locking overhead. Safe to call from any goroutine.
func (cm *ConfigManager) Get() *Config {
	if cm == nil {
		return nil
	}

	return cm.config.Load()
}

// Update atomically replaces the current configuration with a new one.
// Validates the new config before storing. Returns an error if cfg is nil
// or fails validation.
func (cm *ConfigManager) Update(cfg *Config) error {
	if cm == nil || cfg == nil {
		return ErrConfigNil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	cm.config.Store(cfg)

	return nil
}

// Stop is a no-op retained for the shutdown ordering contract.
// ConfigManager has no background goroutines.
func (cm *ConfigManager) Stop() {}

// WatchSystemplane subscribes the ConfigManager to the systemplane client so
// runtime changes to any registered key trigger a Config rebuild + atomic swap.
//
// Must be called AFTER the systemplane client has started (so the initial
// hydrate populated values). Safe to call with a nil client or nil receiver
// (returns nil, no-op in either case).
//
// The v5 systemplane Client exposes per-key OnChange subscriptions rather than
// a namespace-wide callback, so WatchSystemplane registers one subscription per
// runtime-mutable key. Each subscription triggers the same Update path, which
// re-reads every runtime key from the client and atomically swaps the Config
// pointer. This design keeps subscription handlers trivial and lets operators
// rotate any key without callers needing to know which subscription fired.
func (cm *ConfigManager) WatchSystemplane(client *systemplane.Client) error {
	if cm == nil || client == nil {
		return nil
	}

	// reload re-reads overrides from systemplane and atomically swaps the
	// config pointer. Safe to invoke from multiple subscription callbacks.
	reload := func(ctx context.Context, key string) {
		current := cm.Get()
		if current == nil {
			return
		}

		next := applySystemplaneOverrides(*current, client)

		//nolint:contextcheck // Update is a sync internal state swap; its Validate call is a pure state check with no IO and no request scope.
		if err := cm.Update(&next); err != nil {
			if cm.logger != nil {
				cm.logger.Log(ctx, libLog.LevelWarn, "config reload failed",
					libLog.String("key", key),
					libLog.String("error", err.Error()))
			}
		}
	}

	// Client.Start() hydrates persisted values before WatchSystemplane is called,
	// so perform one eager reload to make the initial snapshot visible before any
	// future OnChange callbacks arrive.
	reload(context.Background(), "initial_hydrate")

	// Register one subscription per runtime-mutable key that feeds the Config
	// struct. These mirror the keys resolved in applySystemplaneOverrides.
	watched := watchedSystemplaneKeys()
	for _, key := range watched {
		k := key // capture

		client.OnChange(systemplaneNamespace, k, func(_ any) {
			reload(context.Background(), k)
		})
	}

	return nil
}

// watchedSystemplaneKeys returns the subset of systemplane keys whose changes
// should trigger a Config rebuild via ConfigManager.Update. This list must
// stay in sync with applySystemplaneOverrides — any key read there should be
// watched here so ConfigManager.Get() reflects the latest value.
//
// Keys are enumerated explicitly (not derived from matcherKeyDefs) because not
// every registered key maps to a mutable Config field. For example, writable
// keys that require a process restart to take effect (credentials, connection
// identities) are intentionally absent from matcherKeyDefs, and a handful of
// registered keys are purely observational. Each entry here MUST have a
// corresponding read in applySystemplaneOverrides — the drift is asserted by
// TestWatchedSystemplaneKeys_CoversMatcherDefs.
//
//nolint:funlen // large flat list of runtime-mutable keys; splitting hurts readability.
func watchedSystemplaneKeys() []string {
	return []string{
		// App
		"app.env_name",

		// Server
		"server.address",
		"server.body_limit_bytes",
		"cors.allowed_origins",
		"cors.allowed_methods",
		"cors.allowed_headers",
		"server.tls_cert_file",
		"server.tls_key_file",
		"server.tls_terminated_upstream",
		"server.trusted_proxies",

		// Tenancy
		"tenancy.multi_tenant_enabled",
		"tenancy.multi_tenant_url",
		"tenancy.multi_tenant_environment",
		"tenancy.multi_tenant_redis_host",
		"tenancy.multi_tenant_redis_port",
		"tenancy.multi_tenant_redis_password",
		"tenancy.multi_tenant_redis_tls",
		"tenancy.multi_tenant_max_tenant_pools",
		"tenancy.multi_tenant_idle_timeout_sec",
		"tenancy.multi_tenant_timeout",
		"tenancy.multi_tenant_circuit_breaker_threshold",
		"tenancy.multi_tenant_circuit_breaker_timeout_sec",
		"tenancy.multi_tenant_service_api_key",
		"tenancy.multi_tenant_cache_ttl_sec",
		"tenancy.multi_tenant_connections_check_interval_sec",

		// PostgreSQL (runtime-tunable pool knobs only)
		"postgres.max_open_conns",
		"postgres.max_idle_conns",
		"postgres.conn_max_lifetime_mins",
		"postgres.conn_max_idle_time_mins",
		"postgres.query_timeout_sec",

		// Redis (runtime-tunable pool knobs only)
		"redis.pool_size",
		"redis.min_idle_conns",
		"redis.read_timeout_ms",
		"redis.write_timeout_ms",

		// Telemetry
		"telemetry.enabled",
		"telemetry.service_name",
		"telemetry.library_name",
		"telemetry.service_version",
		"telemetry.deployment_env",
		"telemetry.collector_endpoint",
		"telemetry.db_metrics_interval_sec",

		// Swagger
		"swagger.enabled",
		"swagger.host",
		"swagger.schemes",

		// Rate Limit
		"rate_limit.enabled",
		"rate_limit.max",
		"rate_limit.expiry_sec",
		"rate_limit.export_max",
		"rate_limit.export_expiry_sec",
		"rate_limit.dispatch_max",
		"rate_limit.dispatch_expiry_sec",

		// Infrastructure
		"infrastructure.connect_timeout_sec",
		"infrastructure.health_check_timeout_sec",

		// Idempotency
		"idempotency.retry_window_sec",
		"idempotency.success_ttl_hours",
		"idempotency.hmac_secret",

		// Deduplication
		"deduplication.ttl_sec",

		// Callback Rate Limit
		"callback_rate_limit.per_minute",

		// Webhook
		"webhook.timeout_sec",

		// Fetcher
		"fetcher.enabled",
		"fetcher.url",
		"fetcher.allow_private_ips",
		"fetcher.health_timeout_sec",
		"fetcher.request_timeout_sec",
		"fetcher.discovery_interval_sec",
		"fetcher.schema_cache_ttl_sec",
		"fetcher.extraction_poll_sec",
		"fetcher.extraction_timeout_sec",
		"fetcher.max_extraction_bytes",
		"fetcher.bridge_interval_sec",
		"fetcher.bridge_batch_size",
		"fetcher.bridge_stale_threshold_sec",
		"fetcher.bridge_retry_max_attempts",
		"fetcher.custody_retention_sweep_interval_sec",
		"fetcher.custody_retention_grace_period_sec",

		// M2M
		"m2m.m2m_target_service",
		"m2m.m2m_credential_cache_ttl_sec",
		"m2m.aws_region",

		// Object Storage
		"object_storage.endpoint",
		"object_storage.region",
		"object_storage.bucket",
		"object_storage.access_key_id",
		"object_storage.secret_access_key",
		"object_storage.use_path_style",
		"object_storage.allow_insecure_endpoint",

		// Export Worker
		"export_worker.enabled",
		"export_worker.poll_interval_sec",
		"export_worker.page_size",
		"export_worker.presign_expiry_sec",

		// Cleanup Worker
		"cleanup_worker.enabled",
		"cleanup_worker.interval_sec",
		"cleanup_worker.batch_size",
		"cleanup_worker.grace_period_sec",

		// Scheduler
		"scheduler.interval_sec",

		// Archival
		"archival.enabled",
		"archival.interval_hours",
		"archival.hot_retention_days",
		"archival.warm_retention_months",
		"archival.cold_retention_months",
		"archival.batch_size",
		"archival.partition_lookahead",
		"archival.storage_bucket",
		"archival.storage_prefix",
		"archival.storage_class",
		"archival.presign_expiry_sec",
	}
}

// applySystemplaneOverrides returns a Config with systemplane-managed fields
// refreshed from the client. Fields not listed here fall through unchanged,
// preserving bootstrap/env-only values.
//
// Every key registered via matcherKeyDefs (except those without a
// corresponding Config field) is mirrored here and watched by
// watchedSystemplaneKeys. When a new runtime-mutable key is added to
// matcherKeyDefs, both this function AND watchedSystemplaneKeys must be
// updated to propagate admin-plane changes end-to-end.
//
// Naming convention: helpers are grouped by Config sub-struct to keep the
// mapping between systemplane key name and Config field name obvious.
//
//nolint:funlen // pre-existing: large mapping of Config fields to systemplane keys; splitting hurts readability.
func applySystemplaneOverrides(base Config, client *systemplane.Client) Config {
	if client == nil {
		return base
	}

	// --- App ---
	base.App.EnvName = SystemplaneGetString(client, "app.env_name", base.App.EnvName)

	// --- Server / CORS / TLS ---
	base.Server.Address = SystemplaneGetString(client, "server.address", base.Server.Address)
	base.Server.BodyLimitBytes = SystemplaneGetInt(client, "server.body_limit_bytes", base.Server.BodyLimitBytes)
	base.Server.CORSAllowedOrigins = SystemplaneGetString(client, "cors.allowed_origins", base.Server.CORSAllowedOrigins)
	base.Server.CORSAllowedMethods = SystemplaneGetString(client, "cors.allowed_methods", base.Server.CORSAllowedMethods)
	base.Server.CORSAllowedHeaders = SystemplaneGetString(client, "cors.allowed_headers", base.Server.CORSAllowedHeaders)
	base.Server.TLSCertFile = SystemplaneGetString(client, "server.tls_cert_file", base.Server.TLSCertFile)
	base.Server.TLSKeyFile = SystemplaneGetString(client, "server.tls_key_file", base.Server.TLSKeyFile)
	base.Server.TLSTerminatedUpstream = SystemplaneGetBool(client, "server.tls_terminated_upstream", base.Server.TLSTerminatedUpstream)
	base.Server.TrustedProxies = SystemplaneGetString(client, "server.trusted_proxies", base.Server.TrustedProxies)

	// --- Tenancy ---
	base.Tenancy.MultiTenantEnabled = SystemplaneGetBool(client, "tenancy.multi_tenant_enabled", base.Tenancy.MultiTenantEnabled)
	base.Tenancy.MultiTenantURL = SystemplaneGetString(client, "tenancy.multi_tenant_url", base.Tenancy.MultiTenantURL)
	base.Tenancy.MultiTenantEnvironment = SystemplaneGetString(client, "tenancy.multi_tenant_environment", base.Tenancy.MultiTenantEnvironment)
	base.Tenancy.MultiTenantRedisHost = SystemplaneGetString(client, "tenancy.multi_tenant_redis_host", base.Tenancy.MultiTenantRedisHost)
	base.Tenancy.MultiTenantRedisPort = SystemplaneGetString(client, "tenancy.multi_tenant_redis_port", base.Tenancy.MultiTenantRedisPort)
	base.Tenancy.MultiTenantRedisPassword = SystemplaneGetString(client, "tenancy.multi_tenant_redis_password", base.Tenancy.MultiTenantRedisPassword)
	base.Tenancy.MultiTenantRedisTLS = SystemplaneGetBool(client, "tenancy.multi_tenant_redis_tls", base.Tenancy.MultiTenantRedisTLS)
	base.Tenancy.MultiTenantMaxTenantPools = SystemplaneGetInt(client, "tenancy.multi_tenant_max_tenant_pools", base.Tenancy.MultiTenantMaxTenantPools)
	base.Tenancy.MultiTenantIdleTimeoutSec = SystemplaneGetInt(client, "tenancy.multi_tenant_idle_timeout_sec", base.Tenancy.MultiTenantIdleTimeoutSec)
	base.Tenancy.MultiTenantTimeout = SystemplaneGetInt(client, "tenancy.multi_tenant_timeout", base.Tenancy.MultiTenantTimeout)
	base.Tenancy.MultiTenantCircuitBreakerThreshold = SystemplaneGetInt(client, "tenancy.multi_tenant_circuit_breaker_threshold", base.Tenancy.MultiTenantCircuitBreakerThreshold)
	base.Tenancy.MultiTenantCircuitBreakerTimeoutSec = SystemplaneGetInt(client, "tenancy.multi_tenant_circuit_breaker_timeout_sec", base.Tenancy.MultiTenantCircuitBreakerTimeoutSec)
	base.Tenancy.MultiTenantServiceAPIKey = SystemplaneGetString(client, "tenancy.multi_tenant_service_api_key", base.Tenancy.MultiTenantServiceAPIKey)
	base.Tenancy.MultiTenantCacheTTLSec = SystemplaneGetInt(client, "tenancy.multi_tenant_cache_ttl_sec", base.Tenancy.MultiTenantCacheTTLSec)
	base.Tenancy.MultiTenantConnectionsCheckIntervalSec = SystemplaneGetInt(client, "tenancy.multi_tenant_connections_check_interval_sec", base.Tenancy.MultiTenantConnectionsCheckIntervalSec)

	// --- PostgreSQL (runtime-tunable pool knobs) ---
	base.Postgres.MaxOpenConnections = SystemplaneGetInt(client, "postgres.max_open_conns", base.Postgres.MaxOpenConnections)
	base.Postgres.MaxIdleConnections = SystemplaneGetInt(client, "postgres.max_idle_conns", base.Postgres.MaxIdleConnections)
	base.Postgres.ConnMaxLifetimeMins = SystemplaneGetInt(client, "postgres.conn_max_lifetime_mins", base.Postgres.ConnMaxLifetimeMins)
	base.Postgres.ConnMaxIdleTimeMins = SystemplaneGetInt(client, "postgres.conn_max_idle_time_mins", base.Postgres.ConnMaxIdleTimeMins)
	base.Postgres.QueryTimeoutSec = SystemplaneGetInt(client, "postgres.query_timeout_sec", base.Postgres.QueryTimeoutSec)

	// --- Redis (runtime-tunable pool knobs) ---
	base.Redis.PoolSize = SystemplaneGetInt(client, "redis.pool_size", base.Redis.PoolSize)
	base.Redis.MinIdleConn = SystemplaneGetInt(client, "redis.min_idle_conns", base.Redis.MinIdleConn)
	base.Redis.ReadTimeoutMs = SystemplaneGetInt(client, "redis.read_timeout_ms", base.Redis.ReadTimeoutMs)
	base.Redis.WriteTimeoutMs = SystemplaneGetInt(client, "redis.write_timeout_ms", base.Redis.WriteTimeoutMs)

	// --- Telemetry ---
	base.Telemetry.Enabled = SystemplaneGetBool(client, "telemetry.enabled", base.Telemetry.Enabled)
	base.Telemetry.ServiceName = SystemplaneGetString(client, "telemetry.service_name", base.Telemetry.ServiceName)
	base.Telemetry.LibraryName = SystemplaneGetString(client, "telemetry.library_name", base.Telemetry.LibraryName)
	base.Telemetry.ServiceVersion = SystemplaneGetString(client, "telemetry.service_version", base.Telemetry.ServiceVersion)
	base.Telemetry.DeploymentEnv = SystemplaneGetString(client, "telemetry.deployment_env", base.Telemetry.DeploymentEnv)
	base.Telemetry.CollectorEndpoint = SystemplaneGetString(client, "telemetry.collector_endpoint", base.Telemetry.CollectorEndpoint)
	base.Telemetry.DBMetricsIntervalSec = SystemplaneGetInt(client, "telemetry.db_metrics_interval_sec", base.Telemetry.DBMetricsIntervalSec)

	// --- Swagger ---
	base.Swagger.Enabled = SystemplaneGetBool(client, "swagger.enabled", base.Swagger.Enabled)
	base.Swagger.Host = SystemplaneGetString(client, "swagger.host", base.Swagger.Host)
	base.Swagger.Schemes = SystemplaneGetString(client, "swagger.schemes", base.Swagger.Schemes)

	// --- Rate Limit ---
	base.RateLimit.Enabled = SystemplaneGetBool(client, "rate_limit.enabled", base.RateLimit.Enabled)
	base.RateLimit.Max = SystemplaneGetInt(client, "rate_limit.max", base.RateLimit.Max)
	base.RateLimit.ExpirySec = SystemplaneGetInt(client, "rate_limit.expiry_sec", base.RateLimit.ExpirySec)
	base.RateLimit.ExportMax = SystemplaneGetInt(client, "rate_limit.export_max", base.RateLimit.ExportMax)
	base.RateLimit.ExportExpirySec = SystemplaneGetInt(client, "rate_limit.export_expiry_sec", base.RateLimit.ExportExpirySec)
	base.RateLimit.DispatchMax = SystemplaneGetInt(client, "rate_limit.dispatch_max", base.RateLimit.DispatchMax)
	base.RateLimit.DispatchExpirySec = SystemplaneGetInt(client, "rate_limit.dispatch_expiry_sec", base.RateLimit.DispatchExpirySec)

	// --- Infrastructure ---
	base.Infrastructure.ConnectTimeoutSec = SystemplaneGetInt(client, "infrastructure.connect_timeout_sec", base.Infrastructure.ConnectTimeoutSec)
	base.Infrastructure.HealthCheckTimeoutSec = SystemplaneGetInt(client, "infrastructure.health_check_timeout_sec", base.Infrastructure.HealthCheckTimeoutSec)

	// --- Idempotency ---
	base.Idempotency.RetryWindowSec = SystemplaneGetInt(client, "idempotency.retry_window_sec", base.Idempotency.RetryWindowSec)
	base.Idempotency.SuccessTTLHours = SystemplaneGetInt(client, "idempotency.success_ttl_hours", base.Idempotency.SuccessTTLHours)
	base.Idempotency.HMACSecret = SystemplaneGetString(client, "idempotency.hmac_secret", base.Idempotency.HMACSecret)

	// --- Deduplication ---
	base.Dedupe.TTLSec = SystemplaneGetInt(client, "deduplication.ttl_sec", base.Dedupe.TTLSec)

	// --- Callback Rate Limit ---
	base.CallbackRateLimit.PerMinute = SystemplaneGetInt(client, "callback_rate_limit.per_minute", base.CallbackRateLimit.PerMinute)

	// --- Webhook ---
	base.Webhook.TimeoutSec = SystemplaneGetInt(client, "webhook.timeout_sec", base.Webhook.TimeoutSec)

	// --- Fetcher ---
	base.Fetcher.Enabled = SystemplaneGetBool(client, "fetcher.enabled", base.Fetcher.Enabled)
	base.Fetcher.URL = SystemplaneGetString(client, "fetcher.url", base.Fetcher.URL)
	base.Fetcher.AllowPrivateIPs = SystemplaneGetBool(client, "fetcher.allow_private_ips", base.Fetcher.AllowPrivateIPs)
	base.Fetcher.HealthTimeoutSec = SystemplaneGetInt(client, "fetcher.health_timeout_sec", base.Fetcher.HealthTimeoutSec)
	base.Fetcher.RequestTimeoutSec = SystemplaneGetInt(client, "fetcher.request_timeout_sec", base.Fetcher.RequestTimeoutSec)
	base.Fetcher.DiscoveryIntervalSec = SystemplaneGetInt(client, "fetcher.discovery_interval_sec", base.Fetcher.DiscoveryIntervalSec)
	base.Fetcher.SchemaCacheTTLSec = SystemplaneGetInt(client, "fetcher.schema_cache_ttl_sec", base.Fetcher.SchemaCacheTTLSec)
	base.Fetcher.ExtractionPollSec = SystemplaneGetInt(client, "fetcher.extraction_poll_sec", base.Fetcher.ExtractionPollSec)
	base.Fetcher.ExtractionTimeoutSec = SystemplaneGetInt(client, "fetcher.extraction_timeout_sec", base.Fetcher.ExtractionTimeoutSec)
	base.Fetcher.MaxExtractionBytes = SystemplaneGetInt64(client, "fetcher.max_extraction_bytes", base.Fetcher.MaxExtractionBytes)
	base.Fetcher.BridgeIntervalSec = SystemplaneGetInt(client, "fetcher.bridge_interval_sec", base.Fetcher.BridgeIntervalSec)
	base.Fetcher.BridgeBatchSize = SystemplaneGetInt(client, "fetcher.bridge_batch_size", base.Fetcher.BridgeBatchSize)
	base.Fetcher.BridgeStaleThresholdSec = SystemplaneGetInt(client, "fetcher.bridge_stale_threshold_sec", base.Fetcher.BridgeStaleThresholdSec)
	base.Fetcher.BridgeRetryMaxAttempts = SystemplaneGetInt(client, "fetcher.bridge_retry_max_attempts", base.Fetcher.BridgeRetryMaxAttempts)
	base.Fetcher.CustodyRetentionSweepIntervalSec = SystemplaneGetInt(client, "fetcher.custody_retention_sweep_interval_sec", base.Fetcher.CustodyRetentionSweepIntervalSec)
	base.Fetcher.CustodyRetentionGracePeriodSec = SystemplaneGetInt(client, "fetcher.custody_retention_grace_period_sec", base.Fetcher.CustodyRetentionGracePeriodSec)

	// --- M2M ---
	base.M2M.M2MTargetService = SystemplaneGetString(client, "m2m.m2m_target_service", base.M2M.M2MTargetService)
	base.M2M.M2MCredentialCacheTTLSec = SystemplaneGetInt(client, "m2m.m2m_credential_cache_ttl_sec", base.M2M.M2MCredentialCacheTTLSec)
	base.M2M.AWSRegion = SystemplaneGetString(client, "m2m.aws_region", base.M2M.AWSRegion)

	// --- Object Storage ---
	base.ObjectStorage.Endpoint = SystemplaneGetString(client, "object_storage.endpoint", base.ObjectStorage.Endpoint)
	base.ObjectStorage.Region = SystemplaneGetString(client, "object_storage.region", base.ObjectStorage.Region)
	base.ObjectStorage.Bucket = SystemplaneGetString(client, "object_storage.bucket", base.ObjectStorage.Bucket)
	base.ObjectStorage.AccessKeyID = SystemplaneGetString(client, "object_storage.access_key_id", base.ObjectStorage.AccessKeyID)
	base.ObjectStorage.SecretAccessKey = SystemplaneGetString(client, "object_storage.secret_access_key", base.ObjectStorage.SecretAccessKey)
	base.ObjectStorage.UsePathStyle = SystemplaneGetBool(client, "object_storage.use_path_style", base.ObjectStorage.UsePathStyle)
	base.ObjectStorage.AllowInsecure = SystemplaneGetBool(client, "object_storage.allow_insecure_endpoint", base.ObjectStorage.AllowInsecure)

	// --- Export Worker ---
	base.ExportWorker.Enabled = SystemplaneGetBool(client, "export_worker.enabled", base.ExportWorker.Enabled)
	base.ExportWorker.PollIntervalSec = SystemplaneGetInt(client, "export_worker.poll_interval_sec", base.ExportWorker.PollIntervalSec)
	base.ExportWorker.PageSize = SystemplaneGetInt(client, "export_worker.page_size", base.ExportWorker.PageSize)
	base.ExportWorker.PresignExpirySec = SystemplaneGetInt(client, "export_worker.presign_expiry_sec", base.ExportWorker.PresignExpirySec)

	// --- Cleanup Worker ---
	base.CleanupWorker.Enabled = SystemplaneGetBool(client, "cleanup_worker.enabled", base.CleanupWorker.Enabled)
	base.CleanupWorker.IntervalSec = SystemplaneGetInt(client, "cleanup_worker.interval_sec", base.CleanupWorker.IntervalSec)
	base.CleanupWorker.BatchSize = SystemplaneGetInt(client, "cleanup_worker.batch_size", base.CleanupWorker.BatchSize)
	base.CleanupWorker.GracePeriodSec = SystemplaneGetInt(client, "cleanup_worker.grace_period_sec", base.CleanupWorker.GracePeriodSec)

	// --- Scheduler ---
	base.Scheduler.IntervalSec = SystemplaneGetInt(client, "scheduler.interval_sec", base.Scheduler.IntervalSec)

	// --- Archival ---
	base.Archival.Enabled = SystemplaneGetBool(client, "archival.enabled", base.Archival.Enabled)
	base.Archival.IntervalHours = SystemplaneGetInt(client, "archival.interval_hours", base.Archival.IntervalHours)
	base.Archival.HotRetentionDays = SystemplaneGetInt(client, "archival.hot_retention_days", base.Archival.HotRetentionDays)
	base.Archival.WarmRetentionMonths = SystemplaneGetInt(client, "archival.warm_retention_months", base.Archival.WarmRetentionMonths)
	base.Archival.ColdRetentionMonths = SystemplaneGetInt(client, "archival.cold_retention_months", base.Archival.ColdRetentionMonths)
	base.Archival.BatchSize = SystemplaneGetInt(client, "archival.batch_size", base.Archival.BatchSize)
	base.Archival.PartitionLookahead = SystemplaneGetInt(client, "archival.partition_lookahead", base.Archival.PartitionLookahead)
	base.Archival.StorageBucket = SystemplaneGetString(client, "archival.storage_bucket", base.Archival.StorageBucket)
	base.Archival.StoragePrefix = SystemplaneGetString(client, "archival.storage_prefix", base.Archival.StoragePrefix)
	base.Archival.StorageClass = SystemplaneGetString(client, "archival.storage_class", base.Archival.StorageClass)
	base.Archival.PresignExpirySec = SystemplaneGetInt(client, "archival.presign_expiry_sec", base.Archival.PresignExpirySec)

	return base
}
