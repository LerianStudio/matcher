// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"
)

var (
	errFetcherURLMustBeString      = errors.New("fetcher url must be a string")
	errFetcherURLMustBeAbsolute    = errors.New("fetcher url must be an absolute URL")
	errFetcherURLMustUseHTTPScheme = errors.New("fetcher url must use http or https")
)

// Default values for Matcher configuration keys. These constants match the
// envDefault tag values in the Config struct hierarchy and serve as the
// canonical source of truth for the systemplane registry.
const (
	// App defaults.
	defaultEnvName  = "development"
	defaultLogLevel = "info"

	// Server defaults.
	defaultServerAddress         = ":4018"
	defaultCORSAllowedOrigins    = "http://localhost:3000"
	defaultCORSAllowedMethods    = "GET,POST,PUT,PATCH,DELETE,OPTIONS"
	defaultCORSAllowedHeaders    = "Origin,Content-Type,Accept,Authorization,X-Request-ID"
	defaultKeyBodyLimitBytes     = 32 * 1024 * 1024 // 32 MiB
	defaultTLSTerminatedUpstream = false
	defaultServerTrustedProxies  = ""
	defaultServerTLSCertFile     = ""
	defaultServerTLSKeyFile      = ""

	// Tenancy defaults.
	defaultTenantID                         = "11111111-1111-1111-1111-111111111111"
	defaultTenantSlug                       = "default"
	defaultMultiTenantEnabled               = false
	defaultMultiTenantRedisPort             = "6379"
	defaultMultiTenantRedisTLS              = false
	defaultMultiTenantMaxTenantPools        = 100
	defaultMultiTenantIdleTimeoutSec        = 300
	defaultMultiTenantTimeout               = 30
	defaultMultiTenantCircuitBreakerThresh  = 5
	defaultMultiTenantCircuitBreakerSec     = 30
	defaultMultiTenantCacheTTLSec           = 120
	defaultMultiTenantConnsCheckIntervalSec = 30

	// PostgreSQL defaults.
	defaultPGHost            = "localhost"
	defaultPGPort            = "5432"
	defaultPGUser            = "matcher"
	defaultPGPassword        = "matcher_dev_password" // #nosec G101 -- Dev-mode default; rejected by validateProductionConfig in production. //nolint:gosec
	defaultPGDB              = "matcher"
	defaultPGSSLMode         = "disable"
	defaultPGMaxOpenConns    = 25
	defaultPGMaxIdleConns    = 5
	defaultPGConnMaxLifeMins = 30
	defaultPGConnMaxIdleMins = 5
	defaultPGConnectTimeout  = 10
	defaultPGQueryTimeout    = 30
	defaultPGMigrationsPath  = "migrations"

	// Redis defaults.
	defaultRedisHost         = "localhost:6379"
	defaultRedisDB           = 0
	defaultRedisProtocol     = 3
	defaultRedisTLS          = false
	defaultRedisPoolSize     = 10
	defaultRedisMinIdleConn  = 2
	defaultRedisReadTimeout  = 3000
	defaultRedisWriteTimeout = 3000
	defaultRedisDialTimeout  = 5000

	// RabbitMQ defaults.
	defaultRabbitURI                 = "amqp"
	defaultRabbitHost                = "localhost"
	defaultRabbitPort                = "5672"
	defaultRabbitUser                = "matcher_admin"
	defaultRabbitPassword            = "matcher_dev_password" // #nosec G101 -- Dev-mode default; rejected by validateProductionConfig in production. //nolint:gosec
	defaultRabbitVHost               = "/"
	defaultRabbitHealthURL           = "http://localhost:15672"
	defaultRabbitAllowInsecureHealth = false

	// Auth defaults.
	defaultAuthEnabled = false

	// Swagger defaults.
	defaultSwaggerEnabled = false
	defaultSwaggerSchemes = "https"

	// Telemetry defaults.
	defaultTelemetryEnabled         = false
	defaultTelemetryServiceName     = "matcher"
	defaultTelemetryLibraryName     = "github.com/LerianStudio/matcher"
	defaultTelemetryServiceVersion  = "1.1.0"
	defaultTelemetryDeploymentEnv   = "development"
	defaultTelemetryCollectorEP     = "localhost:4317"
	defaultTelemetryDBMetricsIntSec = 15

	// Rate limit defaults.
	defaultRateLimitEnabled      = true
	defaultRateLimitMax          = 100
	defaultRateLimitExpirySec    = 60
	defaultRateLimitExportMax    = 10
	defaultRateLimitExportExpiry = 60
	defaultRateLimitDispatchMax  = 50
	defaultRateLimitDispatchExp  = 60

	// Infrastructure defaults.
	defaultInfraConnectTimeout     = 30
	defaultInfraHealthCheckTimeout = 5

	// Idempotency defaults.
	defaultIdempotencyRetryWindow = 300
	defaultIdempotencySuccessTTL  = 168

	// Callback rate limit defaults.
	defaultCallbackPerMinute = 60

	// Fetcher defaults.
	defaultFetcherEnabled           = false
	defaultFetcherURL               = "http://localhost:4006"
	defaultFetcherAllowPrivateIPs   = false
	defaultKeyFetcherHealthTimeout  = 5
	defaultKeyFetcherRequestTimeout = 30
	defaultFetcherDiscoveryInt      = 60
	defaultKeyFetcherSchemaCacheTTL = 300
	defaultFetcherExtractionPoll    = 5
	defaultFetcherExtractionTO      = 600

	// M2M defaults.
	defaultM2MTargetService      = "fetcher"
	defaultM2MCredentialCacheTTL = 300

	// Deduplication defaults.
	defaultDedupeTTLSec = 3600

	// Object storage defaults.
	defaultObjStorageEndpoint      = "http://localhost:8333"
	defaultObjStorageRegion        = "us-east-1"
	defaultObjStorageBucket        = "matcher-exports"
	defaultObjStoragePathStyle     = true
	defaultObjStorageAllowInsecure = false

	// Export worker defaults.
	defaultExportEnabled    = true
	defaultExportPollInt    = 5
	defaultExportPageSize   = 1000
	defaultExportPresignExp = 3600

	// Webhook defaults.
	defaultWebhookTimeout = 30

	// Cleanup worker defaults.
	defaultCleanupEnabled     = true
	defaultCleanupInterval    = 3600
	defaultCleanupBatchSize   = 100
	defaultCleanupGracePeriod = 3600

	// Scheduler defaults.
	defaultSchedulerInterval = 60

	// Archival defaults.
	defaultArchivalEnabled       = false
	defaultArchivalInterval      = 24
	defaultArchivalHotDays       = 90
	defaultArchivalWarmMonths    = 24
	defaultArchivalColdMonths    = 84
	defaultArchivalBatchSize     = 5000
	defaultArchivalStorageBucket = "matcher-archives"
	defaultArchivalStoragePrefix = "archives/audit-logs"
	defaultArchivalStorageClass  = "GLACIER"
	defaultArchivalPartitionLA   = 3
	defaultArchivalPresignExpiry = 3600
)

// RegisterMatcherKeys registers all Matcher configuration keys in the
// systemplane registry. Each key corresponds to a field in the Config struct
// and its sub-structs, using dotted mapstructure tag paths as key names.
func RegisterMatcherKeys(reg registry.Registry) error {
	for _, def := range matcherKeyDefs() {
		if err := reg.Register(def); err != nil {
			return fmt.Errorf("register matcher key %q: %w", def.Key, err)
		}
	}

	return nil
}

// matcherKeyDefs returns all Matcher configuration key definitions. The order
// follows the Config struct field order for auditability. Every field with a
// mapstructure tag gets a corresponding KeyDef entry.
//
// The definitions are split across focused helpers so the registry remains the
// canonical source of truth without concentrating all keys in a single file.
func matcherKeyDefs() []domain.KeyDef {
	return concatKeyDefs(
		matcherKeyDefsAppServer(),
		matcherKeyDefsTenancy(),
		matcherKeyDefsPostgres(),
		matcherKeyDefsRedisRabbitMQ(),
		matcherKeyDefsRuntimeHTTP(),
		matcherKeyDefsInfrastructure(),
		matcherKeyDefsStorageExport(),
		matcherKeyDefsWorkers(),
		matcherKeyDefsArchival(),
	)
}
