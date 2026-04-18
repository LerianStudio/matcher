// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// defaultConfig returns a Config populated with sensible defaults.
// The constants used here match the defaults registered in matcherKeyDefs()
// and the envDefault struct tags on the Config struct.
//
// Logger and ShutdownGracePeriod are left as zero values; they are set
// during bootstrap.
//
//nolint:funlen // pre-existing: large struct-literal builder; splitting across helpers hurts readability without reducing complexity.
func defaultConfig() *Config {
	return &Config{
		App: AppConfig{
			EnvName:  defaultEnvName,
			LogLevel: defaultLogLevel,
		},
		Server: ServerConfig{
			Address:               defaultServerAddress,
			BodyLimitBytes:        defaultKeyBodyLimitBytes,
			CORSAllowedOrigins:    defaultCORSAllowedOrigins,
			CORSAllowedMethods:    defaultCORSAllowedMethods,
			CORSAllowedHeaders:    defaultCORSAllowedHeaders,
			TLSTerminatedUpstream: defaultTLSTerminatedUpstream,
		},
		Tenancy: TenancyConfig{
			DefaultTenantID:                        defaultTenantID,
			DefaultTenantSlug:                      defaultTenantSlug,
			MultiTenantEnabled:                     defaultMultiTenantEnabled,
			MultiTenantRedisPort:                   defaultMultiTenantRedisPort,
			MultiTenantRedisTLS:                    defaultMultiTenantRedisTLS,
			MultiTenantMaxTenantPools:              defaultMultiTenantMaxTenantPools,
			MultiTenantIdleTimeoutSec:              defaultMultiTenantIdleTimeoutSec,
			MultiTenantTimeout:                     defaultMultiTenantTimeout,
			MultiTenantCircuitBreakerThreshold:     defaultMultiTenantCircuitBreakerThresh,
			MultiTenantCircuitBreakerTimeoutSec:    defaultMultiTenantCircuitBreakerSec,
			MultiTenantCacheTTLSec:                 defaultMultiTenantCacheTTLSec,
			MultiTenantConnectionsCheckIntervalSec: defaultMultiTenantConnsCheckIntervalSec,
		},
		Postgres: PostgresConfig{
			PrimaryHost:         defaultPGHost,
			PrimaryPort:         defaultPGPort,
			PrimaryUser:         defaultPGUser,
			PrimaryPassword:     defaultPGPassword,
			PrimaryDB:           defaultPGDB,
			PrimarySSLMode:      defaultPGSSLMode,
			MaxOpenConnections:  defaultPGMaxOpenConns,
			MaxIdleConnections:  defaultPGMaxIdleConns,
			ConnMaxLifetimeMins: defaultPGConnMaxLifeMins,
			ConnMaxIdleTimeMins: defaultPGConnMaxIdleMins,
			ConnectTimeoutSec:   defaultPGConnectTimeout,
			QueryTimeoutSec:     defaultPGQueryTimeout,
			MigrationsPath:      defaultPGMigrationsPath,
		},
		Redis: RedisConfig{
			Host:           defaultRedisHost,
			DB:             defaultRedisDB,
			Protocol:       defaultRedisProtocol,
			TLS:            defaultRedisTLS,
			PoolSize:       defaultRedisPoolSize,
			MinIdleConn:    defaultRedisMinIdleConn,
			ReadTimeoutMs:  defaultRedisReadTimeout,
			WriteTimeoutMs: defaultRedisWriteTimeout,
			DialTimeoutMs:  defaultRedisDialTimeout,
		},
		RabbitMQ: RabbitMQConfig{
			URI:                      defaultRabbitURI,
			Host:                     defaultRabbitHost,
			Port:                     defaultRabbitPort,
			User:                     defaultRabbitUser,
			Password:                 defaultRabbitPassword,
			VHost:                    defaultRabbitVHost,
			HealthURL:                defaultRabbitHealthURL,
			AllowInsecureHealthCheck: defaultRabbitAllowInsecureHealth,
		},
		Auth: AuthConfig{
			Enabled: defaultAuthEnabled,
		},
		Telemetry: TelemetryConfig{
			Enabled:              defaultTelemetryEnabled,
			ServiceName:          defaultTelemetryServiceName,
			LibraryName:          defaultTelemetryLibraryName,
			ServiceVersion:       defaultTelemetryServiceVersion,
			DeploymentEnv:        defaultTelemetryDeploymentEnv,
			CollectorEndpoint:    defaultTelemetryCollectorEP,
			DBMetricsIntervalSec: defaultTelemetryDBMetricsIntSec,
		},
		Swagger: SwaggerConfig{
			Enabled: defaultSwaggerEnabled,
			Schemes: defaultSwaggerSchemes,
		},
		RateLimit: RateLimitConfig{
			Enabled:           defaultRateLimitEnabled,
			Max:               defaultRateLimitMax,
			ExpirySec:         defaultRateLimitExpirySec,
			ExportMax:         defaultRateLimitExportMax,
			ExportExpirySec:   defaultRateLimitExportExpiry,
			DispatchMax:       defaultRateLimitDispatchMax,
			DispatchExpirySec: defaultRateLimitDispatchExp,
			AdminMax:          defaultRateLimitAdminMax,
			AdminExpirySec:    defaultRateLimitAdminExp,
		},
		Infrastructure: InfrastructureConfig{
			ConnectTimeoutSec:     defaultInfraConnectTimeout,
			HealthCheckTimeoutSec: defaultInfraHealthCheckTimeout,
		},
		Idempotency: IdempotencyConfig{
			RetryWindowSec:  defaultIdempotencyRetryWindow,
			SuccessTTLHours: defaultIdempotencySuccessTTL,
		},
		Outbox: OutboxConfig{
			RetryWindowSec:      defaultOutboxRetryWindow,
			DispatchIntervalSec: defaultOutboxDispatchIntervalSec,
		},
		Dedupe: DedupeConfig{
			TTLSec: defaultDedupeTTLSec,
		},
		CallbackRateLimit: CallbackRateLimitConfig{
			PerMinute: defaultCallbackPerMinute,
		},
		Webhook: WebhookConfig{
			TimeoutSec: defaultWebhookTimeout,
		},
		Fetcher: FetcherConfig{
			Enabled:                          defaultFetcherEnabled,
			URL:                              defaultFetcherURL,
			AllowPrivateIPs:                  defaultFetcherAllowPrivateIPs,
			HealthTimeoutSec:                 defaultKeyFetcherHealthTimeout,
			RequestTimeoutSec:                defaultKeyFetcherRequestTimeout,
			DiscoveryIntervalSec:             defaultFetcherDiscoveryInt,
			SchemaCacheTTLSec:                defaultKeyFetcherSchemaCacheTTL,
			ExtractionPollSec:                defaultFetcherExtractionPoll,
			ExtractionTimeoutSec:             defaultFetcherExtractionTO,
			MaxExtractionBytes:               defaultFetcherMaxExtractionBytes,
			BridgeIntervalSec:                defaultBridgeIntervalSec,
			BridgeBatchSize:                  defaultBridgeBatchSize,
			BridgeStaleThresholdSec:          defaultBridgeStaleThresholdSec,
			BridgeRetryMaxAttempts:           defaultBridgeRetryMaxAttempts,
			CustodyRetentionSweepIntervalSec: defaultCustodyRetentionSweepIntervalSec,
			CustodyRetentionGracePeriodSec:   defaultCustodyRetentionGracePeriodSec,
		},
		M2M: M2MConfig{
			M2MTargetService:         defaultM2MTargetService,
			M2MCredentialCacheTTLSec: defaultM2MCredentialCacheTTL,
		},
		ObjectStorage: ObjectStorageConfig{
			Endpoint:      defaultObjStorageEndpoint,
			Region:        defaultObjStorageRegion,
			Bucket:        defaultObjStorageBucket,
			UsePathStyle:  defaultObjStoragePathStyle,
			AllowInsecure: defaultObjStorageAllowInsecure,
		},
		ExportWorker: ExportWorkerConfig{
			Enabled:          defaultExportEnabled,
			PollIntervalSec:  defaultExportPollInt,
			PageSize:         defaultExportPageSize,
			PresignExpirySec: defaultExportPresignExp,
		},
		CleanupWorker: CleanupWorkerConfig{
			Enabled:        defaultCleanupEnabled,
			IntervalSec:    defaultCleanupInterval,
			BatchSize:      defaultCleanupBatchSize,
			GracePeriodSec: defaultCleanupGracePeriod,
		},
		Scheduler: SchedulerConfig{
			IntervalSec: defaultSchedulerInterval,
		},
		Archival: ArchivalConfig{
			Enabled:             defaultArchivalEnabled,
			IntervalHours:       defaultArchivalInterval,
			HotRetentionDays:    defaultArchivalHotDays,
			WarmRetentionMonths: defaultArchivalWarmMonths,
			ColdRetentionMonths: defaultArchivalColdMonths,
			BatchSize:           defaultArchivalBatchSize,
			PartitionLookahead:  defaultArchivalPartitionLA,
			StorageBucket:       defaultArchivalStorageBucket,
			StoragePrefix:       defaultArchivalStoragePrefix,
			StorageClass:        defaultArchivalStorageClass,
			PresignExpirySec:    defaultArchivalPresignExpiry,
		},
	}
}
