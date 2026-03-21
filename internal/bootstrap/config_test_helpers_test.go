// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"os"
	"testing"
)

// clearConfigEnvVars removes every environment variable that loadConfigFromEnv
// reads via libCommons.SetConfigFromEnvVars.
//
// We must unset (not set to empty string): restoreZeroedFields treats presence
// of an env var as an explicit override, including empty-string values.
// Setting to "" would therefore blank fields like DEFAULT_TENANT_ID and break
// validation paths that depend on default values.
//
// Original values are restored via t.Cleanup so neighbouring tests are unaffected.
// Because process env is global mutable state, the calling test MUST NOT call
// t.Parallel().
func clearConfigEnvVars(t *testing.T) {
	t.Helper()

	for _, key := range configEnvVarKeys {
		value, exists := os.LookupEnv(key)
		requireNoError(t, os.Unsetenv(key))

		t.Cleanup(func() {
			if exists {
				requireNoError(t, os.Setenv(key, value))
				return
			}

			requireNoError(t, os.Unsetenv(key))
		})
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected env operation error: %v", err)
	}
}

// configEnvVarKeys lists every env var read by loadConfigFromEnv (via the `env:`
// struct tags on Config sub-structs). Keep in sync with config.go.
var configEnvVarKeys = []string{
	// AppConfig
	"ENV_NAME",
	"LOG_LEVEL",

	// ServerConfig
	"SERVER_ADDRESS",
	"HTTP_BODY_LIMIT_BYTES",
	"CORS_ALLOWED_ORIGINS",
	"CORS_ALLOWED_METHODS",
	"CORS_ALLOWED_HEADERS",
	"SERVER_TLS_CERT_FILE",
	"SERVER_TLS_KEY_FILE",
	"TLS_TERMINATED_UPSTREAM",
	"TRUSTED_PROXIES",

	// TenancyConfig
	"DEFAULT_TENANT_ID",
	"DEFAULT_TENANT_SLUG",
	"MULTI_TENANT_ENABLED",
	"MULTI_TENANT_URL",
	"MULTI_TENANT_ENVIRONMENT",
	"MULTI_TENANT_MAX_TENANT_POOLS",
	"MULTI_TENANT_IDLE_TIMEOUT_SEC",
	"MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD",
	"MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC",
	"MULTI_TENANT_SERVICE_API_KEY",
	"MULTI_TENANT_INFRA_ENABLED",

	// PostgresConfig
	"POSTGRES_HOST",
	"POSTGRES_PORT",
	"POSTGRES_USER",
	"POSTGRES_PASSWORD",
	"POSTGRES_DB",
	"POSTGRES_SSLMODE",
	"POSTGRES_REPLICA_HOST",
	"POSTGRES_REPLICA_PORT",
	"POSTGRES_REPLICA_USER",
	"POSTGRES_REPLICA_PASSWORD",
	"POSTGRES_REPLICA_DB",
	"POSTGRES_REPLICA_SSLMODE",
	"POSTGRES_MAX_OPEN_CONNS",
	"POSTGRES_MAX_IDLE_CONNS",
	"POSTGRES_CONN_MAX_LIFETIME_MINS",
	"POSTGRES_CONN_MAX_IDLE_TIME_MINS",
	"POSTGRES_CONNECT_TIMEOUT_SEC",
	"POSTGRES_QUERY_TIMEOUT_SEC",
	"MIGRATIONS_PATH",

	// RedisConfig
	"REDIS_HOST",
	"REDIS_MASTER_NAME",
	"REDIS_PASSWORD",
	"REDIS_DB",
	"REDIS_PROTOCOL",
	"REDIS_TLS",
	"REDIS_CA_CERT",
	"REDIS_POOL_SIZE",
	"REDIS_MIN_IDLE_CONNS",
	"REDIS_READ_TIMEOUT_MS",
	"REDIS_WRITE_TIMEOUT_MS",
	"REDIS_DIAL_TIMEOUT_MS",

	// RabbitMQConfig
	"RABBITMQ_URI",
	"RABBITMQ_HOST",
	"RABBITMQ_PORT",
	"RABBITMQ_USER",
	"RABBITMQ_PASSWORD",
	"RABBITMQ_VHOST",
	"RABBITMQ_HEALTH_URL",
	"RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK",

	// AuthConfig
	"AUTH_ENABLED",
	"AUTH_SERVICE_ADDRESS",
	"AUTH_JWT_SECRET",

	// SwaggerConfig
	"SWAGGER_ENABLED",
	"SWAGGER_HOST",
	"SWAGGER_SCHEMES",

	// TelemetryConfig
	"ENABLE_TELEMETRY",
	"OTEL_RESOURCE_SERVICE_NAME",
	"OTEL_LIBRARY_NAME",
	"OTEL_RESOURCE_SERVICE_VERSION",
	"OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT",
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"DB_METRICS_INTERVAL_SEC",

	// RateLimitConfig
	"RATE_LIMIT_ENABLED",
	"RATE_LIMIT_MAX",
	"RATE_LIMIT_EXPIRY_SEC",
	"EXPORT_RATE_LIMIT_MAX",
	"EXPORT_RATE_LIMIT_EXPIRY_SEC",
	"DISPATCH_RATE_LIMIT_MAX",
	"DISPATCH_RATE_LIMIT_EXPIRY_SEC",

	// InfrastructureConfig
	"INFRA_CONNECT_TIMEOUT_SEC",
	"HEALTH_CHECK_TIMEOUT_SEC",

	// IdempotencyConfig
	"IDEMPOTENCY_RETRY_WINDOW_SEC",
	"IDEMPOTENCY_SUCCESS_TTL_HOURS",
	"IDEMPOTENCY_HMAC_SECRET",

	// DedupeConfig
	"DEDUPE_TTL_SEC",

	// ObjectStorageConfig
	"OBJECT_STORAGE_ENDPOINT",
	"OBJECT_STORAGE_REGION",
	"OBJECT_STORAGE_BUCKET",
	"OBJECT_STORAGE_ACCESS_KEY_ID",
	"OBJECT_STORAGE_SECRET_ACCESS_KEY",
	"OBJECT_STORAGE_USE_PATH_STYLE",

	// ExportWorkerConfig
	"EXPORT_WORKER_ENABLED",
	"EXPORT_WORKER_POLL_INTERVAL_SEC",
	"EXPORT_WORKER_PAGE_SIZE",
	"EXPORT_PRESIGN_EXPIRY_SEC",

	// WebhookConfig
	"WEBHOOK_TIMEOUT_SEC",

	// CleanupWorkerConfig
	"CLEANUP_WORKER_ENABLED",
	"CLEANUP_WORKER_INTERVAL_SEC",
	"CLEANUP_WORKER_BATCH_SIZE",
	"CLEANUP_WORKER_GRACE_PERIOD_SEC",

	// SchedulerConfig
	"SCHEDULER_INTERVAL_SEC",

	// ArchivalConfig
	"ARCHIVAL_WORKER_ENABLED",
	"ARCHIVAL_WORKER_INTERVAL_HOURS",
	"ARCHIVAL_HOT_RETENTION_DAYS",
	"ARCHIVAL_WARM_RETENTION_MONTHS",
	"ARCHIVAL_COLD_RETENTION_MONTHS",
	"ARCHIVAL_BATCH_SIZE",
	"ARCHIVAL_STORAGE_BUCKET",
	"ARCHIVAL_STORAGE_PREFIX",
	"ARCHIVAL_STORAGE_CLASS",
	"ARCHIVAL_PARTITION_LOOKAHEAD",
	"ARCHIVAL_PRESIGN_EXPIRY_SEC",

	// CallbackRateLimitConfig
	"CALLBACK_RATE_LIMIT_PER_MIN",

	// FetcherConfig
	"FETCHER_ENABLED",
	"FETCHER_URL",
	"FETCHER_ALLOW_PRIVATE_IPS",
	"FETCHER_HEALTH_TIMEOUT_SEC",
	"FETCHER_REQUEST_TIMEOUT_SEC",
	"FETCHER_DISCOVERY_INTERVAL_SEC",
	"FETCHER_SCHEMA_CACHE_TTL_SEC",
	"FETCHER_EXTRACTION_POLL_INTERVAL_SEC",
	"FETCHER_EXTRACTION_TIMEOUT_SEC",
}
