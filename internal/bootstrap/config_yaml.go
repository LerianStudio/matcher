// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// configFilePathEnv is the environment variable to override the YAML config file path.
const configFilePathEnv = "CONFIG_FILE_PATH"

// defaultConfigFilePath is the fallback YAML config file location.
const defaultConfigFilePath = "config/matcher.yaml"

var errInvalidConfigFilePathOverride = errors.New("invalid CONFIG_FILE_PATH override")

// resolveConfigFilePath returns the YAML config file path.
//
// It reads CONFIG_FILE_PATH from the environment; if unset or empty,
// it returns the default "config/matcher.yaml".
func resolveConfigFilePath() string {
	path, err := resolveConfigFilePathStrict()
	if err != nil {
		return defaultConfigFilePath
	}

	return path
}

func resolveConfigFilePathStrict() (string, error) {
	path := strings.TrimSpace(os.Getenv(configFilePathEnv))
	if path == "" {
		return defaultConfigFilePath, nil
	}

	if strings.ContainsRune(path, '\x00') {
		return "", fmt.Errorf("%w: contains null byte", errInvalidConfigFilePathOverride)
	}

	cleaned := filepath.Clean(path)
	if cleaned == "." {
		return "", fmt.Errorf("%w: path resolves to current directory", errInvalidConfigFilePathOverride)
	}

	if !isPathContained(cleaned) {
		return "", fmt.Errorf("%w: path must be within working directory", errInvalidConfigFilePathOverride)
	}

	if !hasYAMLExtension(cleaned) {
		return "", fmt.Errorf("%w: file extension must be .yaml or .yml", errInvalidConfigFilePathOverride)
	}

	return cleaned, nil
}

// isPathContained returns true if the given path is safe to use as a config file.
// Both relative and absolute paths must resolve within the current working directory
// (after cleaning and symlink evaluation) to prevent traversal.
func isPathContained(cleaned string) bool {
	cwd, err := os.Getwd()
	if err != nil {
		return false
	}

	baseAbs, err := filepath.Abs(cwd)
	if err != nil {
		return false
	}

	baseResolved, err := filepath.EvalSymlinks(baseAbs)
	if err != nil {
		baseResolved = baseAbs
	}

	candidateAbs := filepath.Clean(cleaned)
	if !filepath.IsAbs(candidateAbs) {
		candidateAbs = filepath.Join(baseResolved, candidateAbs)
	}

	candidateResolved := candidateAbs
	if fileResolved, resolveErr := filepath.EvalSymlinks(candidateAbs); resolveErr == nil {
		candidateResolved = fileResolved
	} else if !os.IsNotExist(resolveErr) {
		return false
	}

	if dirResolved, resolveErr := filepath.EvalSymlinks(filepath.Dir(candidateAbs)); resolveErr == nil {
		candidateResolved = filepath.Join(dirResolved, filepath.Base(candidateAbs))
	} else if !os.IsNotExist(resolveErr) {
		return false
	}

	rel, err := filepath.Rel(baseResolved, candidateResolved)
	if err != nil {
		return false
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}

	return true
}

func hasYAMLExtension(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))

	return ext == ".yaml" || ext == ".yml"
}

// bindDefaults registers all default values from defaultConfig() into the viper
// instance. This is critical because viper's AutomaticEnv() only recognises keys
// that already exist in its store (via SetDefault, config file, or BindEnv).
// By calling SetDefault for every key, we guarantee that the corresponding
// MATCHER_<SECTION>_<KEY> environment variable will be picked up automatically.
//
//nolint:funlen // Intentionally long — one SetDefault call per config field for explicit mapping.
func bindDefaults(viperCfg *viper.Viper) {
	// --- App ---
	viperCfg.SetDefault("app.env_name", "development")
	viperCfg.SetDefault("app.log_level", "info")

	// --- Server ---
	viperCfg.SetDefault("server.address", ":4018")
	viperCfg.SetDefault("server.body_limit_bytes", 104857600) //nolint:mnd // 100 MB
	viperCfg.SetDefault("server.cors_allowed_origins", "http://localhost:3000")
	viperCfg.SetDefault("server.cors_allowed_methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
	viperCfg.SetDefault("server.cors_allowed_headers", "Origin,Content-Type,Accept,Authorization,X-Request-ID")
	viperCfg.SetDefault("server.tls_cert_file", "")
	viperCfg.SetDefault("server.tls_key_file", "")
	viperCfg.SetDefault("server.tls_terminated_upstream", false)
	viperCfg.SetDefault("server.trusted_proxies", "")

	// --- Tenancy ---
	viperCfg.SetDefault("tenancy.default_tenant_id", "11111111-1111-1111-1111-111111111111")
	viperCfg.SetDefault("tenancy.default_tenant_slug", "default")
	viperCfg.SetDefault("tenancy.multi_tenant_infra_enabled", false)

	// --- Postgres ---
	viperCfg.SetDefault("postgres.primary_host", "localhost")
	viperCfg.SetDefault("postgres.primary_port", "5432")
	viperCfg.SetDefault("postgres.primary_user", "matcher")
	viperCfg.SetDefault("postgres.primary_password", "")
	viperCfg.SetDefault("postgres.primary_db", "matcher")
	viperCfg.SetDefault("postgres.primary_ssl_mode", "disable")
	viperCfg.SetDefault("postgres.replica_host", "")
	viperCfg.SetDefault("postgres.replica_port", "")
	viperCfg.SetDefault("postgres.replica_user", "")
	viperCfg.SetDefault("postgres.replica_password", "")
	viperCfg.SetDefault("postgres.replica_db", "")
	viperCfg.SetDefault("postgres.replica_ssl_mode", "")
	viperCfg.SetDefault("postgres.max_open_connections", 25)   //nolint:mnd // connection pool default
	viperCfg.SetDefault("postgres.max_idle_connections", 5)    //nolint:mnd // connection pool default
	viperCfg.SetDefault("postgres.conn_max_lifetime_mins", 30) //nolint:mnd // 30 minutes
	viperCfg.SetDefault("postgres.conn_max_idle_time_mins", 5) //nolint:mnd // 5 minutes
	viperCfg.SetDefault("postgres.connect_timeout_sec", 10)    //nolint:mnd // 10 seconds
	viperCfg.SetDefault("postgres.query_timeout_sec", 30)      //nolint:mnd // 30 seconds
	viperCfg.SetDefault("postgres.migrations_path", "migrations")

	// --- Redis ---
	viperCfg.SetDefault("redis.host", "localhost:6379")
	viperCfg.SetDefault("redis.master_name", "")
	viperCfg.SetDefault("redis.password", "")
	viperCfg.SetDefault("redis.db", 0)
	viperCfg.SetDefault("redis.protocol", 3) //nolint:mnd // RESP3
	viperCfg.SetDefault("redis.tls", false)
	viperCfg.SetDefault("redis.ca_cert", "")
	viperCfg.SetDefault("redis.pool_size", 10)          //nolint:mnd // pool default
	viperCfg.SetDefault("redis.min_idle_conn", 2)       //nolint:mnd // pool default
	viperCfg.SetDefault("redis.read_timeout_ms", 3000)  //nolint:mnd // 3 seconds
	viperCfg.SetDefault("redis.write_timeout_ms", 3000) //nolint:mnd // 3 seconds
	viperCfg.SetDefault("redis.dial_timeout_ms", 5000)  //nolint:mnd // 5 seconds

	// --- RabbitMQ ---
	viperCfg.SetDefault("rabbitmq.uri", "amqp")
	viperCfg.SetDefault("rabbitmq.host", "localhost")
	viperCfg.SetDefault("rabbitmq.port", "5672")
	viperCfg.SetDefault("rabbitmq.user", "guest")
	viperCfg.SetDefault("rabbitmq.password", "guest")
	viperCfg.SetDefault("rabbitmq.vhost", "/")
	viperCfg.SetDefault("rabbitmq.health_url", "http://localhost:15672")
	viperCfg.SetDefault("rabbitmq.allow_insecure_health_check", false)

	// --- Auth ---
	viperCfg.SetDefault("auth.enabled", false)
	viperCfg.SetDefault("auth.host", "")
	viperCfg.SetDefault("auth.token_secret", "")

	// --- Swagger ---
	viperCfg.SetDefault("swagger.enabled", false)
	viperCfg.SetDefault("swagger.host", "")
	viperCfg.SetDefault("swagger.schemes", "https")

	// --- Telemetry ---
	viperCfg.SetDefault("telemetry.enabled", false)
	viperCfg.SetDefault("telemetry.service_name", "matcher")
	viperCfg.SetDefault("telemetry.library_name", "github.com/LerianStudio/matcher")
	viperCfg.SetDefault("telemetry.service_version", "1.0.0")
	viperCfg.SetDefault("telemetry.deployment_env", "development")
	viperCfg.SetDefault("telemetry.collector_endpoint", "localhost:4317")
	viperCfg.SetDefault("telemetry.db_metrics_interval_sec", 15) //nolint:mnd // 15 seconds

	// --- RateLimit ---
	viperCfg.SetDefault("rate_limit.enabled", true)
	viperCfg.SetDefault("rate_limit.max", 100)                //nolint:mnd // requests per window
	viperCfg.SetDefault("rate_limit.expiry_sec", 60)          //nolint:mnd // 60 seconds
	viperCfg.SetDefault("rate_limit.export_max", 10)          //nolint:mnd // export rate limit
	viperCfg.SetDefault("rate_limit.export_expiry_sec", 60)   //nolint:mnd // 60 seconds
	viperCfg.SetDefault("rate_limit.dispatch_max", 50)        //nolint:mnd // dispatch rate limit
	viperCfg.SetDefault("rate_limit.dispatch_expiry_sec", 60) //nolint:mnd // 60 seconds

	// --- Infrastructure ---
	viperCfg.SetDefault("infrastructure.connect_timeout_sec", 30)     //nolint:mnd // 30 seconds
	viperCfg.SetDefault("infrastructure.health_check_timeout_sec", 5) //nolint:mnd // 5 seconds

	// --- Idempotency ---
	viperCfg.SetDefault("idempotency.retry_window_sec", 300)  //nolint:mnd // 5 minutes
	viperCfg.SetDefault("idempotency.success_ttl_hours", 168) //nolint:mnd // 7 days
	viperCfg.SetDefault("idempotency.hmac_secret", "")

	// --- Deduplication ---
	viperCfg.SetDefault("deduplication.ttl_sec", 3600) //nolint:mnd // 1 hour

	// --- ObjectStorage ---
	viperCfg.SetDefault("object_storage.endpoint", "http://localhost:8333")
	viperCfg.SetDefault("object_storage.region", "us-east-1")
	viperCfg.SetDefault("object_storage.bucket", "matcher-exports")
	viperCfg.SetDefault("object_storage.access_key_id", "")
	viperCfg.SetDefault("object_storage.secret_access_key", "")
	viperCfg.SetDefault("object_storage.use_path_style", true)

	// --- ExportWorker ---
	viperCfg.SetDefault("export_worker.enabled", true)
	viperCfg.SetDefault("export_worker.poll_interval_sec", 5)     //nolint:mnd // 5 seconds
	viperCfg.SetDefault("export_worker.page_size", 1000)          //nolint:mnd // rows per page
	viperCfg.SetDefault("export_worker.presign_expiry_sec", 3600) //nolint:mnd // 1 hour

	// --- CleanupWorker ---
	viperCfg.SetDefault("cleanup_worker.enabled", true)
	viperCfg.SetDefault("cleanup_worker.interval_sec", 3600)     //nolint:mnd // 1 hour
	viperCfg.SetDefault("cleanup_worker.batch_size", 100)        //nolint:mnd // files per batch
	viperCfg.SetDefault("cleanup_worker.grace_period_sec", 3600) //nolint:mnd // 1 hour

	// --- Scheduler ---
	viperCfg.SetDefault("scheduler.interval_sec", 60) //nolint:mnd // 1 minute

	// --- Archival ---
	viperCfg.SetDefault("archival.enabled", false)
	viperCfg.SetDefault("archival.interval_hours", 24)        //nolint:mnd // 24 hours
	viperCfg.SetDefault("archival.hot_retention_days", 90)    //nolint:mnd // 90 days
	viperCfg.SetDefault("archival.warm_retention_months", 24) //nolint:mnd // 2 years
	viperCfg.SetDefault("archival.cold_retention_months", 84) //nolint:mnd // 7 years
	viperCfg.SetDefault("archival.batch_size", 5000)          //nolint:mnd // records per batch
	viperCfg.SetDefault("archival.storage_bucket", "")
	viperCfg.SetDefault("archival.storage_prefix", "archives/audit-logs")
	viperCfg.SetDefault("archival.storage_class", "GLACIER")
	viperCfg.SetDefault("archival.partition_lookahead", 3)   //nolint:mnd // future partitions
	viperCfg.SetDefault("archival.presign_expiry_sec", 3600) //nolint:mnd // 1 hour

	// --- Webhook ---
	viperCfg.SetDefault("webhook.timeout_sec", 30) //nolint:mnd // 30 seconds

	// --- CallbackRateLimit ---
	viperCfg.SetDefault("callback_rate_limit.per_minute", 60) //nolint:mnd // callbacks per minute

	// --- Fetcher ---
	viperCfg.SetDefault("fetcher.enabled", false)
	viperCfg.SetDefault("fetcher.url", "http://localhost:4006")
	viperCfg.SetDefault("fetcher.allow_private_ips", true)
	viperCfg.SetDefault("fetcher.health_timeout_sec", 5)       //nolint:mnd // 5 seconds
	viperCfg.SetDefault("fetcher.request_timeout_sec", 30)     //nolint:mnd // 30 seconds
	viperCfg.SetDefault("fetcher.discovery_interval_sec", 60)  //nolint:mnd // 1 minute
	viperCfg.SetDefault("fetcher.schema_cache_ttl_sec", 300)   //nolint:mnd // 5 minutes
	viperCfg.SetDefault("fetcher.extraction_poll_sec", 5)      //nolint:mnd // 5 seconds
	viperCfg.SetDefault("fetcher.extraction_timeout_sec", 600) //nolint:mnd // 10 minutes
}

// loadConfigFromYAML reads a YAML config file and unmarshals it into the given Config struct.
// It creates a local viper instance (no global state) with:
//   - Defaults matching defaultConfig() so AutomaticEnv works for all keys
//   - MATCHER_ env var prefix, with "." → "_" replacement for env key mapping
//   - Graceful handling of missing files (returns nil, not error)
//
// This function is called BEFORE the application logger is available. It returns errors
// for actionable failures (malformed YAML, permission denied) and returns nil for
// expected conditions (file not found). The caller logs the error if needed.
func loadConfigFromYAML(cfg *Config, filePath string) error {
	if cfg == nil {
		return ErrConfigNil
	}

	viperCfg := viper.New()

	// Register all defaults so AutomaticEnv can discover every key.
	bindDefaults(viperCfg)

	// Environment variable overlay: MATCHER_APP_LOG_LEVEL → app.log_level
	viperCfg.SetEnvPrefix("MATCHER")
	viperCfg.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viperCfg.AutomaticEnv()

	// Point viper at the config file.
	viperCfg.SetConfigFile(filePath)

	// Attempt to read the file. Missing files are expected (env-only deploys).
	if err := viperCfg.ReadInConfig(); err != nil {
		if isConfigFileNotFound(err) {
			// File-not-found is normal for env-only deployments.
			// Even without a file, defaults + MATCHER_* env vars still apply.
			if unmarshalErr := viperCfg.Unmarshal(cfg); unmarshalErr != nil {
				return fmt.Errorf("unmarshal defaults config: %w", unmarshalErr)
			}

			return nil
		}

		// Real error (parse failure, permission denied, etc.)
		return fmt.Errorf("read YAML config %s: %w", filePath, err)
	}

	// Unmarshal YAML + env overrides into the Config struct.
	if err := viperCfg.Unmarshal(cfg); err != nil {
		return fmt.Errorf("unmarshal YAML config: %w", err)
	}

	return nil
}

// isConfigFileNotFound returns true if the error indicates the config file does not exist.
// Handles both viper's ConfigFileNotFoundError and OS-level file-not-found errors.
func isConfigFileNotFound(err error) bool {
	var notFoundErr viper.ConfigFileNotFoundError
	if errors.As(err, &notFoundErr) {
		return true
	}

	return os.IsNotExist(err)
}
