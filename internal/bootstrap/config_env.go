// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	defaultFetcherHealthTimeout        = 5 * time.Second
	defaultFetcherRequestTimeout       = 30 * time.Second
	defaultFetcherSchemaCacheTTL       = 5 * time.Minute
	defaultFetcherExtractionPollPeriod = 5 * time.Second
	defaultFetcherExtractionTimeout    = 10 * time.Minute
)

// logConfigWarn logs a warning if the config logger is available.
// Safe to call even when cfg.Logger is nil (e.g., during early bootstrap).
func (cfg *Config) logConfigWarn(ctx context.Context, msg string) {
	if cfg != nil && !sharedPorts.IsNilValue(cfg.Logger) {
		cfg.Logger.Log(ctx, libLog.LevelWarn, msg)
	}
}

func (cfg *Config) normalizeTenancyConfig() {
	if cfg == nil {
		return
	}

	if cfg.Tenancy.MultiTenantEnvironment == "" {
		cfg.Tenancy.MultiTenantEnvironment = cfg.effectiveMultiTenantEnvironment()
	}
}

func (cfg *Config) effectiveMultiTenantEnvironment() string {
	if cfg == nil {
		return ""
	}

	if configured := strings.TrimSpace(cfg.Tenancy.MultiTenantEnvironment); configured != "" {
		return configured
	}

	return strings.TrimSpace(cfg.App.EnvName)
}

// MultiTenantTimeoutDuration returns the tenant-manager API timeout as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
func (cfg *Config) MultiTenantTimeoutDuration() time.Duration {
	if cfg.Tenancy.MultiTenantTimeout <= 0 {
		return time.Second
	}

	return time.Duration(cfg.Tenancy.MultiTenantTimeout) * time.Second
}

// MultiTenantCacheTTL returns the tenant config cache TTL as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
func (cfg *Config) MultiTenantCacheTTL() time.Duration {
	if cfg.Tenancy.MultiTenantCacheTTLSec <= 0 {
		return time.Second
	}

	return time.Duration(cfg.Tenancy.MultiTenantCacheTTLSec) * time.Second
}

// MultiTenantConnectionsCheckInterval returns the pgManager settings revalidation
// interval as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
func (cfg *Config) MultiTenantConnectionsCheckInterval() time.Duration {
	if cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec <= 0 {
		return time.Second
	}

	return time.Duration(cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec) * time.Second
}

// MultiTenantIdleTimeout returns the tenant pool idle timeout as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
func (cfg *Config) MultiTenantIdleTimeout() time.Duration {
	if cfg.Tenancy.MultiTenantIdleTimeoutSec <= 0 {
		return time.Second
	}

	return time.Duration(cfg.Tenancy.MultiTenantIdleTimeoutSec) * time.Second
}

// PrimaryDSN returns the PostgreSQL connection string for the primary database.
func (cfg *Config) PrimaryDSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		cfg.Postgres.PrimaryHost,
		cfg.Postgres.PrimaryPort,
		cfg.Postgres.PrimaryUser,
		cfg.Postgres.PrimaryPassword,
		cfg.Postgres.PrimaryDB,
		cfg.Postgres.PrimarySSLMode,
		cfg.Postgres.ConnectTimeoutSec,
	)
}

// ReplicaDSN returns the PostgreSQL connection string for the replica database,
// falling back to primary settings when replica-specific values are not configured.
func (cfg *Config) ReplicaDSN() string {
	if cfg.Postgres.ReplicaHost == "" {
		return cfg.PrimaryDSN()
	}

	host := cfg.Postgres.ReplicaHost

	port := cfg.Postgres.ReplicaPort
	if port == "" {
		port = cfg.Postgres.PrimaryPort
	}

	user := cfg.Postgres.ReplicaUser
	if user == "" {
		user = cfg.Postgres.PrimaryUser
	}

	password := cfg.Postgres.ReplicaPassword
	if password == "" {
		password = cfg.Postgres.PrimaryPassword
	}

	dbname := cfg.Postgres.ReplicaDB
	if dbname == "" {
		dbname = cfg.Postgres.PrimaryDB
	}

	sslmode := cfg.Postgres.ReplicaSSLMode
	if sslmode == "" {
		sslmode = cfg.Postgres.PrimarySSLMode
	}

	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		host,
		port,
		user,
		password,
		dbname,
		sslmode,
		cfg.Postgres.ConnectTimeoutSec,
	)
}

// PrimaryDSNMasked returns the primary connection string with password redacted for logging.
func (cfg *Config) PrimaryDSNMasked() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=***REDACTED*** dbname=%s sslmode=%s",
		cfg.Postgres.PrimaryHost, cfg.Postgres.PrimaryPort, cfg.Postgres.PrimaryUser,
		cfg.Postgres.PrimaryDB, cfg.Postgres.PrimarySSLMode)
}

// ReplicaDSNMasked returns the replica connection string with password redacted for logging.
func (cfg *Config) ReplicaDSNMasked() string {
	if cfg.Postgres.ReplicaHost == "" {
		return cfg.PrimaryDSNMasked()
	}

	host := cfg.Postgres.ReplicaHost

	port := cfg.Postgres.ReplicaPort
	if port == "" {
		port = cfg.Postgres.PrimaryPort
	}

	user := cfg.Postgres.ReplicaUser
	if user == "" {
		user = cfg.Postgres.PrimaryUser
	}

	dbname := cfg.Postgres.ReplicaDB
	if dbname == "" {
		dbname = cfg.Postgres.PrimaryDB
	}

	sslmode := cfg.Postgres.ReplicaSSLMode
	if sslmode == "" {
		sslmode = cfg.Postgres.PrimarySSLMode
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=***REDACTED*** dbname=%s sslmode=%s",
		host, port, user, dbname, sslmode)
}

// RabbitMQDSN returns the AMQP connection string with properly URL-encoded credentials and vhost.
func (cfg *Config) RabbitMQDSN() string {
	var userinfo *url.Userinfo
	if cfg.RabbitMQ.Password == "" {
		userinfo = url.User(cfg.RabbitMQ.User)
	} else {
		userinfo = url.UserPassword(cfg.RabbitMQ.User, cfg.RabbitMQ.Password)
	}

	connURL := url.URL{
		Scheme: cfg.RabbitMQ.URI,
		User:   userinfo,
		Host:   net.JoinHostPort(cfg.RabbitMQ.Host, cfg.RabbitMQ.Port),
	}

	// RabbitMQ vhost is represented as the URL path segment and must be URL-encoded.
	// Default vhost is "/" which must be encoded as "%2F".
	vhostRaw := strings.TrimSpace(cfg.RabbitMQ.VHost)
	if vhostRaw != "" {
		if strings.Trim(vhostRaw, "/") == "" {
			connURL.Path = "//"
			connURL.RawPath = "/%2F"
		} else {
			vhost := strings.TrimPrefix(vhostRaw, "/")
			connURL.Path = "/" + vhost
			connURL.RawPath = "/" + url.PathEscape(vhost)
		}
	}

	return connURL.String()
}

// RedisReadTimeout returns the Redis read timeout as a time.Duration.
func (cfg *Config) RedisReadTimeout() time.Duration {
	return time.Duration(cfg.Redis.ReadTimeoutMs) * time.Millisecond
}

// RedisWriteTimeout returns the Redis write timeout as a time.Duration.
func (cfg *Config) RedisWriteTimeout() time.Duration {
	return time.Duration(cfg.Redis.WriteTimeoutMs) * time.Millisecond
}

// RedisDialTimeout returns the Redis dial timeout as a time.Duration.
func (cfg *Config) RedisDialTimeout() time.Duration {
	return time.Duration(cfg.Redis.DialTimeoutMs) * time.Millisecond
}

// ConnMaxLifetime returns the PostgreSQL connection max lifetime as a time.Duration.
func (cfg *Config) ConnMaxLifetime() time.Duration {
	return time.Duration(cfg.Postgres.ConnMaxLifetimeMins) * time.Minute
}

// ConnMaxIdleTime returns the PostgreSQL connection max idle time as a time.Duration.
func (cfg *Config) ConnMaxIdleTime() time.Duration {
	return time.Duration(cfg.Postgres.ConnMaxIdleTimeMins) * time.Minute
}

// QueryTimeout returns the PostgreSQL query timeout as a time.Duration.
// This bounds the maximum time any database operation (query, transaction) can take.
// Prevents indefinite hangs when the connection pool is exhausted and no context
// deadline is set by the caller. Returns defaultQueryTimeoutSec (30 seconds) if
// the configured value is non-positive.
func (cfg *Config) QueryTimeout() time.Duration {
	if cfg.Postgres.QueryTimeoutSec <= 0 {
		return defaultQueryTimeoutSec * time.Second
	}

	return time.Duration(cfg.Postgres.QueryTimeoutSec) * time.Second
}

// InfraConnectTimeout returns the overall infrastructure connection timeout as a time.Duration.
// This is the maximum time allowed for all infrastructure connections (PostgreSQL, Redis, RabbitMQ)
// to complete during application startup.
func (cfg *Config) InfraConnectTimeout() time.Duration {
	const defaultInfraConnectTimeoutSec = 30

	if cfg.Infrastructure.ConnectTimeoutSec <= 0 {
		return time.Duration(defaultInfraConnectTimeoutSec) * time.Second
	}

	if cfg.Infrastructure.ConnectTimeoutSec > maxInfraConnectTimeoutSec {
		cfg.logConfigWarn(
			context.Background(),
			fmt.Sprintf(
				"INFRA_CONNECT_TIMEOUT_SEC=%d exceeds maximum of %d seconds, capping to maximum",
				cfg.Infrastructure.ConnectTimeoutSec,
				maxInfraConnectTimeoutSec,
			),
		)

		return time.Duration(maxInfraConnectTimeoutSec) * time.Second
	}

	return time.Duration(cfg.Infrastructure.ConnectTimeoutSec) * time.Second
}

// DBMetricsInterval returns the database metrics collection interval as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
func (cfg *Config) DBMetricsInterval() time.Duration {
	if cfg.Telemetry.DBMetricsIntervalSec <= 0 {
		return time.Second
	}

	return time.Duration(cfg.Telemetry.DBMetricsIntervalSec) * time.Second
}

// IdempotencyRetryWindow returns the idempotency retry window as a time.Duration.
// Returns a minimum of 1 minute if configured value is non-positive.
func (cfg *Config) IdempotencyRetryWindow() time.Duration {
	if cfg.Idempotency.RetryWindowSec <= 0 {
		return time.Minute
	}

	return time.Duration(cfg.Idempotency.RetryWindowSec) * time.Second
}

// IdempotencySuccessTTL returns the idempotency success TTL as a time.Duration.
// Returns a minimum of 1 hour if configured value is non-positive.
func (cfg *Config) IdempotencySuccessTTL() time.Duration {
	if cfg.Idempotency.SuccessTTLHours <= 0 {
		return time.Hour
	}

	return time.Duration(cfg.Idempotency.SuccessTTLHours) * time.Hour
}

// WebhookTimeout returns the default webhook dispatch timeout as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
// Caps at 300 seconds (5 minutes) to prevent runaway connections.
func (cfg *Config) WebhookTimeout() time.Duration {
	const (
		maxWebhookTimeoutSec     = 300 // 5 minutes
		defaultWebhookTimeoutSec = 30
	)

	if cfg.Webhook.TimeoutSec <= 0 {
		return time.Duration(defaultWebhookTimeoutSec) * time.Second
	}

	if cfg.Webhook.TimeoutSec > maxWebhookTimeoutSec {
		cfg.logConfigWarn(context.Background(), fmt.Sprintf("WEBHOOK_TIMEOUT_SEC=%d exceeds maximum of %d seconds, capping to maximum",
			cfg.Webhook.TimeoutSec, maxWebhookTimeoutSec))

		return time.Duration(maxWebhookTimeoutSec) * time.Second
	}

	return time.Duration(cfg.Webhook.TimeoutSec) * time.Second
}

// FetcherHealthTimeout returns the fetcher health-check timeout.
// Returns a default of 5 seconds if configured value is non-positive.
func (cfg *Config) FetcherHealthTimeout() time.Duration {
	if cfg.Fetcher.HealthTimeoutSec <= 0 {
		return defaultFetcherHealthTimeout
	}

	return time.Duration(cfg.Fetcher.HealthTimeoutSec) * time.Second
}

// FetcherRequestTimeout returns the fetcher request timeout.
// Returns a default of 30 seconds if configured value is non-positive.
func (cfg *Config) FetcherRequestTimeout() time.Duration {
	if cfg.Fetcher.RequestTimeoutSec <= 0 {
		return defaultFetcherRequestTimeout
	}

	return time.Duration(cfg.Fetcher.RequestTimeoutSec) * time.Second
}

// FetcherDiscoveryInterval returns the discovery worker interval.
// Returns a default of 1 minute if configured value is non-positive.
func (cfg *Config) FetcherDiscoveryInterval() time.Duration {
	if cfg.Fetcher.DiscoveryIntervalSec <= 0 {
		return time.Minute
	}

	return time.Duration(cfg.Fetcher.DiscoveryIntervalSec) * time.Second
}

// FetcherSchemaCacheTTL returns the schema cache TTL.
// Returns a default of 5 minutes if configured value is non-positive.
func (cfg *Config) FetcherSchemaCacheTTL() time.Duration {
	if cfg.Fetcher.SchemaCacheTTLSec <= 0 {
		return defaultFetcherSchemaCacheTTL
	}

	return time.Duration(cfg.Fetcher.SchemaCacheTTLSec) * time.Second
}

// FetcherExtractionPollInterval returns the extraction poll interval.
// Returns a default of 5 seconds if configured value is non-positive.
func (cfg *Config) FetcherExtractionPollInterval() time.Duration {
	if cfg.Fetcher.ExtractionPollSec <= 0 {
		return defaultFetcherExtractionPollPeriod
	}

	return time.Duration(cfg.Fetcher.ExtractionPollSec) * time.Second
}

// FetcherExtractionTimeout returns the extraction timeout.
// Returns a default of 10 minutes if configured value is non-positive.
func (cfg *Config) FetcherExtractionTimeout() time.Duration {
	if cfg.Fetcher.ExtractionTimeoutSec <= 0 {
		return defaultFetcherExtractionTimeout
	}

	return time.Duration(cfg.Fetcher.ExtractionTimeoutSec) * time.Second
}

// ExportWorkerPollInterval returns the export worker poll interval as a time.Duration.
// Returns a default of 5 seconds if configured value is non-positive.
func (cfg *Config) ExportWorkerPollInterval() time.Duration {
	if cfg.ExportWorker.PollIntervalSec <= 0 {
		return defaultExportWorkerPollIntervalSec * time.Second
	}

	return time.Duration(cfg.ExportWorker.PollIntervalSec) * time.Second
}

// ExportPresignExpiry returns the presigned URL expiry duration for export downloads.
// Returns a default of 1 hour if configured value is non-positive.
// Caps at S3's maximum of 7 days (604800 seconds) if exceeded.
func (cfg *Config) ExportPresignExpiry() time.Duration {
	const (
		maxPresignExpiry     = 604800 // S3 maximum: 7 days in seconds
		defaultPresignExpiry = 3600   // 1 hour default
	)

	if cfg.ExportWorker.PresignExpirySec <= 0 {
		return time.Duration(defaultPresignExpiry) * time.Second
	}

	if cfg.ExportWorker.PresignExpirySec > maxPresignExpiry {
		cfg.logConfigWarn(context.Background(), fmt.Sprintf("EXPORT_PRESIGN_EXPIRY_SEC=%d exceeds S3 maximum of %d seconds, capping to maximum",
			cfg.ExportWorker.PresignExpirySec, maxPresignExpiry))

		return time.Duration(maxPresignExpiry) * time.Second
	}

	return time.Duration(cfg.ExportWorker.PresignExpirySec) * time.Second
}

// CleanupWorkerInterval returns the cleanup worker run interval as a time.Duration.
// Returns a default of 1 hour if configured value is non-positive.
func (cfg *Config) CleanupWorkerInterval() time.Duration {
	const defaultInterval = 3600 // 1 hour default

	if cfg.CleanupWorker.IntervalSec <= 0 {
		return time.Duration(defaultInterval) * time.Second
	}

	return time.Duration(cfg.CleanupWorker.IntervalSec) * time.Second
}

// CleanupWorkerBatchSize returns the cleanup worker batch size.
// Returns a default of 100 if configured value is non-positive.
func (cfg *Config) CleanupWorkerBatchSize() int {
	const defaultBatch = 100

	if cfg.CleanupWorker.BatchSize <= 0 {
		return defaultBatch
	}

	return cfg.CleanupWorker.BatchSize
}

// CleanupWorkerGracePeriod returns the file deletion grace period as a time.Duration.
// This controls how long after expiry the worker waits before deleting S3 files,
// allowing presigned download URLs to complete.
// Returns a default of 1 hour if configured value is non-positive.
func (cfg *Config) CleanupWorkerGracePeriod() time.Duration {
	const defaultGrace = 3600 // 1 hour default

	if cfg.CleanupWorker.GracePeriodSec <= 0 {
		return time.Duration(defaultGrace) * time.Second
	}

	return time.Duration(cfg.CleanupWorker.GracePeriodSec) * time.Second
}

// ArchivalInterval returns the archival worker run interval as a time.Duration.
// Returns a minimum of 1 hour if configured value is non-positive.
func (cfg *Config) ArchivalInterval() time.Duration {
	if cfg.Archival.IntervalHours <= 0 {
		return time.Hour
	}

	return time.Duration(cfg.Archival.IntervalHours) * time.Hour
}

// ArchivalPresignExpiry returns the presigned URL expiry duration for archived audit log downloads.
// Returns a default of 1 hour if configured value is non-positive.
// Caps at S3's maximum of 7 days (604800 seconds) if exceeded.
func (cfg *Config) ArchivalPresignExpiry() time.Duration {
	const (
		maxPresignExpiry     = 604800 // S3 maximum: 7 days in seconds
		defaultPresignExpiry = 3600   // 1 hour default
	)

	if cfg.Archival.PresignExpirySec <= 0 {
		return time.Duration(defaultPresignExpiry) * time.Second
	}

	if cfg.Archival.PresignExpirySec > maxPresignExpiry {
		cfg.logConfigWarn(context.Background(), fmt.Sprintf("ARCHIVAL_PRESIGN_EXPIRY_SEC=%d exceeds S3 maximum of %d seconds, capping to maximum",
			cfg.Archival.PresignExpirySec, maxPresignExpiry))

		return time.Duration(maxPresignExpiry) * time.Second
	}

	return time.Duration(cfg.Archival.PresignExpirySec) * time.Second
}

// CallbackRateLimitPerMinute returns the callback rate limit per minute.
// Returns a minimum of 1 if configured value is non-positive.
func (cfg *Config) CallbackRateLimitPerMinute() int {
	if cfg.CallbackRateLimit.PerMinute <= 0 {
		return 60 //nolint:mnd // sensible default: 60 callbacks per minute per external system
	}

	return cfg.CallbackRateLimit.PerMinute
}

// DedupeTTL returns the deduplication TTL as a time.Duration.
// Returns a default of 1 hour if configured value is non-positive.
func (cfg *Config) DedupeTTL() time.Duration {
	if cfg == nil || cfg.Dedupe.TTLSec <= 0 {
		return time.Hour
	}

	return time.Duration(cfg.Dedupe.TTLSec) * time.Second
}

// M2MCredentialCacheTTL returns the M2M credential L2 cache TTL as a time.Duration.
// Returns a default of 5 minutes if configured value is non-positive.
func (cfg *Config) M2MCredentialCacheTTL() time.Duration {
	const defaultM2MCacheTTLSec = 300 // 5 minutes

	if cfg.M2M.M2MCredentialCacheTTLSec <= 0 {
		return time.Duration(defaultM2MCacheTTLSec) * time.Second
	}

	return time.Duration(cfg.M2M.M2MCredentialCacheTTLSec) * time.Second
}

// SchedulerInterval returns the scheduler worker poll interval as a time.Duration.
// Returns a default of 1 minute if configured value is non-positive.
func (cfg *Config) SchedulerInterval() time.Duration {
	if cfg.Scheduler.IntervalSec <= 0 {
		return time.Minute
	}

	return time.Duration(cfg.Scheduler.IntervalSec) * time.Second
}
