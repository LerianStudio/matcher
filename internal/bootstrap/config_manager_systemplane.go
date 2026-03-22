// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

// configFromSnapshot hydrates every Config field from the snapshot alone, with
// no dependency on a previous Config. This is the pure hydration path — used by
// defaultConfig() to derive defaults from KeyDefs and by snapshotToFullConfig
// as the first step before overlaying bootstrap-only values.
//
//nolint:funlen // Full config hydration is inherently large; splitting would hurt auditability.
func configFromSnapshot(snap domain.Snapshot) *Config {
	cfg := &Config{}

	// --- All fields from snapshot. ---

	// App.
	cfg.App.EnvName = snapString(snap, "app.env_name", "")
	cfg.App.LogLevel = snapString(snap, "app.log_level", defaultLogLevel)

	// Server.
	cfg.Server.Address = snapString(snap, "server.address", "")
	cfg.Server.TLSCertFile = snapString(snap, "server.tls_cert_file", "")
	cfg.Server.TLSKeyFile = snapString(snap, "server.tls_key_file", "")
	cfg.Server.TLSTerminatedUpstream = snapBool(snap, "server.tls_terminated_upstream", false)
	cfg.Server.TrustedProxies = snapString(snap, "server.trusted_proxies", "")
	cfg.Server.BodyLimitBytes = snapInt(snap, "server.body_limit_bytes", defaultKeyBodyLimitBytes)
	cfg.Server.CORSAllowedOrigins = snapString(snap, "server.cors_allowed_origins", defaultCORSAllowedOrigins)
	cfg.Server.CORSAllowedMethods = snapString(snap, "server.cors_allowed_methods", defaultCORSAllowedMethods)
	cfg.Server.CORSAllowedHeaders = snapString(snap, "server.cors_allowed_headers", defaultCORSAllowedHeaders)

	// Auth (bootstrap-only in practice, but hydrated from snapshot for defaults).
	cfg.Auth.Enabled = snapBool(snap, "auth.enabled", false)
	cfg.Auth.Host = snapString(snap, "auth.host", "")
	cfg.Auth.TokenSecret = snapString(snap, "auth.token_secret", "")

	// Telemetry (bootstrap-only in practice, but hydrated from snapshot for defaults).
	cfg.Telemetry.Enabled = snapBool(snap, "telemetry.enabled", false)
	cfg.Telemetry.ServiceName = snapString(snap, "telemetry.service_name", "")
	cfg.Telemetry.LibraryName = snapString(snap, "telemetry.library_name", "")
	cfg.Telemetry.ServiceVersion = snapString(snap, "telemetry.service_version", "")
	cfg.Telemetry.DeploymentEnv = snapString(snap, "telemetry.deployment_env", "")
	cfg.Telemetry.CollectorEndpoint = snapString(snap, "telemetry.collector_endpoint", "")
	cfg.Telemetry.DBMetricsIntervalSec = snapInt(snap, "telemetry.db_metrics_interval_sec", 0)

	// Tenancy.
	cfg.Tenancy.DefaultTenantID = snapString(snap, "tenancy.default_tenant_id", defaultTenantID)
	cfg.Tenancy.DefaultTenantSlug = snapString(snap, "tenancy.default_tenant_slug", defaultTenantSlug)
	cfg.Tenancy.MultiTenantEnabled = snapBool(snap, "tenancy.multi_tenant_enabled", defaultMultiTenantEnabled)
	cfg.Tenancy.MultiTenantURL = snapString(snap, "tenancy.multi_tenant_url", "")
	cfg.Tenancy.MultiTenantEnvironment = snapString(snap, "tenancy.multi_tenant_environment", "")
	cfg.Tenancy.MultiTenantMaxTenantPools = snapInt(snap, "tenancy.multi_tenant_max_tenant_pools", defaultMultiTenantMaxTenantPools)
	cfg.Tenancy.MultiTenantIdleTimeoutSec = snapInt(snap, "tenancy.multi_tenant_idle_timeout_sec", defaultMultiTenantIdleTimeoutSec)
	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = snapInt(snap, "tenancy.multi_tenant_circuit_breaker_threshold", defaultMultiTenantCircuitBreakerThresh)
	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = snapInt(snap, "tenancy.multi_tenant_circuit_breaker_timeout_sec", defaultMultiTenantCircuitBreakerSec)
	cfg.Tenancy.MultiTenantServiceAPIKey = snapString(snap, "tenancy.multi_tenant_service_api_key", "")
	cfg.Tenancy.MultiTenantInfraEnabled = snapBool(snap, "tenancy.multi_tenant_infra_enabled", defaultMultiTenantInfraEnabled)

	// Postgres.
	cfg.Postgres.PrimaryHost = snapString(snap, "postgres.primary_host", defaultPGHost)
	cfg.Postgres.PrimaryPort = snapString(snap, "postgres.primary_port", defaultPGPort)
	cfg.Postgres.PrimaryUser = snapString(snap, "postgres.primary_user", defaultPGUser)
	cfg.Postgres.PrimaryPassword = snapString(snap, "postgres.primary_password", defaultPGPassword)
	cfg.Postgres.PrimaryDB = snapString(snap, "postgres.primary_db", defaultPGDB)
	cfg.Postgres.PrimarySSLMode = snapString(snap, "postgres.primary_ssl_mode", defaultPGSSLMode)
	cfg.Postgres.ReplicaHost = snapString(snap, "postgres.replica_host", "")
	cfg.Postgres.ReplicaPort = snapString(snap, "postgres.replica_port", "")
	cfg.Postgres.ReplicaUser = snapString(snap, "postgres.replica_user", "")
	cfg.Postgres.ReplicaPassword = snapString(snap, "postgres.replica_password", "")
	cfg.Postgres.ReplicaDB = snapString(snap, "postgres.replica_db", "")
	cfg.Postgres.ReplicaSSLMode = snapString(snap, "postgres.replica_ssl_mode", "")
	cfg.Postgres.MaxOpenConnections = snapInt(snap, "postgres.max_open_connections", defaultPGMaxOpenConns)
	cfg.Postgres.MaxIdleConnections = snapInt(snap, "postgres.max_idle_connections", defaultPGMaxIdleConns)
	cfg.Postgres.ConnMaxLifetimeMins = snapInt(snap, "postgres.conn_max_lifetime_mins", defaultPGConnMaxLifeMins)
	cfg.Postgres.ConnMaxIdleTimeMins = snapInt(snap, "postgres.conn_max_idle_time_mins", defaultPGConnMaxIdleMins)
	cfg.Postgres.ConnectTimeoutSec = snapInt(snap, "postgres.connect_timeout_sec", defaultPGConnectTimeout)
	cfg.Postgres.QueryTimeoutSec = snapInt(snap, "postgres.query_timeout_sec", defaultPGQueryTimeout)
	cfg.Postgres.MigrationsPath = snapString(snap, "postgres.migrations_path", defaultPGMigrationsPath)

	// Redis.
	cfg.Redis.Host = snapString(snap, "redis.host", defaultRedisHost)
	cfg.Redis.MasterName = snapString(snap, "redis.master_name", "")
	cfg.Redis.Password = snapString(snap, "redis.password", "")
	cfg.Redis.DB = snapInt(snap, "redis.db", defaultRedisDB)
	cfg.Redis.Protocol = snapInt(snap, "redis.protocol", defaultRedisProtocol)
	cfg.Redis.TLS = snapBool(snap, "redis.tls", defaultRedisTLS)
	cfg.Redis.CACert = snapString(snap, "redis.ca_cert", "")
	cfg.Redis.PoolSize = snapInt(snap, "redis.pool_size", defaultRedisPoolSize)
	cfg.Redis.MinIdleConn = snapInt(snap, "redis.min_idle_conn", defaultRedisMinIdleConn)
	cfg.Redis.ReadTimeoutMs = snapInt(snap, "redis.read_timeout_ms", defaultRedisReadTimeout)
	cfg.Redis.WriteTimeoutMs = snapInt(snap, "redis.write_timeout_ms", defaultRedisWriteTimeout)
	cfg.Redis.DialTimeoutMs = snapInt(snap, "redis.dial_timeout_ms", defaultRedisDialTimeout)

	// RabbitMQ.
	cfg.RabbitMQ.URI = snapString(snap, "rabbitmq.uri", defaultRabbitURI)
	cfg.RabbitMQ.Host = snapString(snap, "rabbitmq.host", defaultRabbitHost)
	cfg.RabbitMQ.Port = snapString(snap, "rabbitmq.port", defaultRabbitPort)
	cfg.RabbitMQ.User = snapString(snap, "rabbitmq.user", defaultRabbitUser)
	cfg.RabbitMQ.Password = snapString(snap, "rabbitmq.password", defaultRabbitPassword)
	cfg.RabbitMQ.VHost = snapString(snap, "rabbitmq.vhost", defaultRabbitVHost)
	cfg.RabbitMQ.HealthURL = snapString(snap, "rabbitmq.health_url", defaultRabbitHealthURL)
	cfg.RabbitMQ.AllowInsecureHealthCheck = snapBool(snap, "rabbitmq.allow_insecure_health_check", defaultRabbitAllowInsecureHealth)

	// Object Storage.
	cfg.ObjectStorage.Endpoint = snapString(snap, "object_storage.endpoint", defaultObjStorageEndpoint)
	cfg.ObjectStorage.Region = snapString(snap, "object_storage.region", defaultObjStorageRegion)
	cfg.ObjectStorage.Bucket = snapString(snap, "object_storage.bucket", defaultObjStorageBucket)
	cfg.ObjectStorage.AccessKeyID = snapString(snap, "object_storage.access_key_id", "")
	cfg.ObjectStorage.SecretAccessKey = snapString(snap, "object_storage.secret_access_key", "")
	cfg.ObjectStorage.UsePathStyle = snapBool(snap, "object_storage.use_path_style", defaultObjStoragePathStyle)

	// Swagger.
	cfg.Swagger.Enabled = snapBool(snap, "swagger.enabled", defaultSwaggerEnabled)
	cfg.Swagger.Host = snapString(snap, "swagger.host", "")
	cfg.Swagger.Schemes = snapString(snap, "swagger.schemes", defaultSwaggerSchemes)

	// Rate Limit.
	cfg.RateLimit.Enabled = snapBool(snap, "rate_limit.enabled", defaultRateLimitEnabled)
	cfg.RateLimit.Max = snapInt(snap, "rate_limit.max", defaultRateLimitMax)
	cfg.RateLimit.ExpirySec = snapInt(snap, "rate_limit.expiry_sec", defaultRateLimitExpirySec)
	cfg.RateLimit.ExportMax = snapInt(snap, "rate_limit.export_max", defaultRateLimitExportMax)
	cfg.RateLimit.ExportExpirySec = snapInt(snap, "rate_limit.export_expiry_sec", defaultRateLimitExportExpiry)
	cfg.RateLimit.DispatchMax = snapInt(snap, "rate_limit.dispatch_max", defaultRateLimitDispatchMax)
	cfg.RateLimit.DispatchExpirySec = snapInt(snap, "rate_limit.dispatch_expiry_sec", defaultRateLimitDispatchExp)

	// Infrastructure.
	cfg.Infrastructure.ConnectTimeoutSec = snapInt(snap, "infrastructure.connect_timeout_sec", defaultInfraConnectTimeout)
	cfg.Infrastructure.HealthCheckTimeoutSec = snapInt(snap, "infrastructure.health_check_timeout_sec", defaultInfraHealthCheckTimeout)

	// Idempotency.
	cfg.Idempotency.RetryWindowSec = snapInt(snap, "idempotency.retry_window_sec", defaultIdempotencyRetryWindow)
	cfg.Idempotency.SuccessTTLHours = snapInt(snap, "idempotency.success_ttl_hours", defaultIdempotencySuccessTTL)
	cfg.Idempotency.HMACSecret = snapString(snap, "idempotency.hmac_secret", "")

	// Deduplication.
	cfg.Dedupe.TTLSec = snapInt(snap, "deduplication.ttl_sec", defaultDedupeTTLSec)

	// Callback Rate Limit.
	cfg.CallbackRateLimit.PerMinute = snapInt(snap, "callback_rate_limit.per_minute", defaultCallbackPerMinute)

	// Webhook.
	cfg.Webhook.TimeoutSec = snapInt(snap, "webhook.timeout_sec", defaultWebhookTimeout)

	// Fetcher.
	cfg.Fetcher.Enabled = snapBool(snap, "fetcher.enabled", defaultFetcherEnabled)
	cfg.Fetcher.URL = snapString(snap, "fetcher.url", defaultFetcherURL)
	cfg.Fetcher.AllowPrivateIPs = snapBool(snap, "fetcher.allow_private_ips", defaultFetcherAllowPrivateIPs)
	cfg.Fetcher.HealthTimeoutSec = snapInt(snap, "fetcher.health_timeout_sec", defaultKeyFetcherHealthTimeout)
	cfg.Fetcher.RequestTimeoutSec = snapInt(snap, "fetcher.request_timeout_sec", defaultKeyFetcherRequestTimeout)
	cfg.Fetcher.DiscoveryIntervalSec = snapInt(snap, "fetcher.discovery_interval_sec", defaultFetcherDiscoveryInt)
	cfg.Fetcher.SchemaCacheTTLSec = snapInt(snap, "fetcher.schema_cache_ttl_sec", defaultKeyFetcherSchemaCacheTTL)
	cfg.Fetcher.ExtractionPollSec = snapInt(snap, "fetcher.extraction_poll_sec", defaultFetcherExtractionPoll)
	cfg.Fetcher.ExtractionTimeoutSec = snapInt(snap, "fetcher.extraction_timeout_sec", defaultFetcherExtractionTO)

	// Export Worker.
	cfg.ExportWorker.Enabled = snapBool(snap, "export_worker.enabled", defaultExportEnabled)
	cfg.ExportWorker.PollIntervalSec = snapInt(snap, "export_worker.poll_interval_sec", defaultExportPollInt)
	cfg.ExportWorker.PageSize = snapInt(snap, "export_worker.page_size", defaultExportPageSize)
	cfg.ExportWorker.PresignExpirySec = snapInt(snap, "export_worker.presign_expiry_sec", defaultExportPresignExp)

	// Cleanup Worker.
	cfg.CleanupWorker.Enabled = snapBool(snap, "cleanup_worker.enabled", defaultCleanupEnabled)
	cfg.CleanupWorker.IntervalSec = snapInt(snap, "cleanup_worker.interval_sec", defaultCleanupInterval)
	cfg.CleanupWorker.BatchSize = snapInt(snap, "cleanup_worker.batch_size", defaultCleanupBatchSize)
	cfg.CleanupWorker.GracePeriodSec = snapInt(snap, "cleanup_worker.grace_period_sec", defaultCleanupGracePeriod)

	// Scheduler.
	cfg.Scheduler.IntervalSec = snapInt(snap, "scheduler.interval_sec", defaultSchedulerInterval)

	// Archival.
	cfg.Archival.Enabled = snapBool(snap, "archival.enabled", defaultArchivalEnabled)
	cfg.Archival.IntervalHours = snapInt(snap, "archival.interval_hours", defaultArchivalInterval)
	cfg.Archival.HotRetentionDays = snapInt(snap, "archival.hot_retention_days", defaultArchivalHotDays)
	cfg.Archival.WarmRetentionMonths = snapInt(snap, "archival.warm_retention_months", defaultArchivalWarmMonths)
	cfg.Archival.ColdRetentionMonths = snapInt(snap, "archival.cold_retention_months", defaultArchivalColdMonths)
	cfg.Archival.BatchSize = snapInt(snap, "archival.batch_size", defaultArchivalBatchSize)
	cfg.Archival.PartitionLookahead = snapInt(snap, "archival.partition_lookahead", defaultArchivalPartitionLA)
	cfg.Archival.StorageBucket = snapString(snap, "archival.storage_bucket", "")
	cfg.Archival.StoragePrefix = snapString(snap, "archival.storage_prefix", defaultArchivalStoragePrefix)
	cfg.Archival.StorageClass = snapString(snap, "archival.storage_class", defaultArchivalStorageClass)
	cfg.Archival.PresignExpirySec = snapInt(snap, "archival.presign_expiry_sec", defaultArchivalPresignExpiry)

	return cfg
}

// snapshotToFullConfig builds a complete *Config from a systemplane snapshot,
// hydrating all fields from the snapshot and then overlaying bootstrap-only
// fields from oldCfg (which are immutable after startup).
//
// This two-step composition:
//  1. configFromSnapshot(snap) — hydrates every field from snapshot values
//  2. overlay bootstrap-only fields from oldCfg — they never change at runtime
//
// cleanly separates the default-derivation path (where configFromSnapshot is
// called standalone) from the runtime-hydration path (where bootstrap-only
// fields must be preserved from the running config).
func snapshotToFullConfig(snap domain.Snapshot, oldCfg *Config) *Config {
	// Step 1: hydrate everything from the snapshot.
	cfg := configFromSnapshot(snap)

	// Step 2: overlay bootstrap-only fields from the running config.
	if oldCfg == nil {
		return cfg
	}

	cfg.App.EnvName = oldCfg.App.EnvName
	cfg.Server.Address = oldCfg.Server.Address
	cfg.Server.TLSCertFile = oldCfg.Server.TLSCertFile
	cfg.Server.TLSKeyFile = oldCfg.Server.TLSKeyFile
	cfg.Server.TLSTerminatedUpstream = oldCfg.Server.TLSTerminatedUpstream
	cfg.Server.TrustedProxies = oldCfg.Server.TrustedProxies
	cfg.Auth = oldCfg.Auth
	cfg.Tenancy.DefaultTenantID = oldCfg.Tenancy.DefaultTenantID
	cfg.Tenancy.DefaultTenantSlug = oldCfg.Tenancy.DefaultTenantSlug
	cfg.Telemetry = oldCfg.Telemetry
	cfg.Idempotency.HMACSecret = oldCfg.Idempotency.HMACSecret
	cfg.Logger = oldCfg.Logger
	cfg.ShutdownGracePeriod = oldCfg.ShutdownGracePeriod

	return cfg
}
