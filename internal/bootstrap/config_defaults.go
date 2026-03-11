package bootstrap

// defaultConfig returns a Config populated with sensible defaults for the YAML-only
// loading path. These defaults MUST stay in sync with the envDefault struct tags on
// Config fields — the envDefault tags govern the env-var-only path, while this function
// governs the YAML path. Both must produce equivalent baseline values to ensure
// consistent behavior regardless of config source.
//
// Secret fields (passwords, tokens, keys, certificates) are left as zero values
// and must be supplied via environment variables.
// Logger and ShutdownGracePeriod are left as zero values; they are set during bootstrap.
//
//nolint:mnd,funlen // This function defines configuration defaults — numeric literals and length are inherent.
func defaultConfig() *Config {
	return &Config{
		App: AppConfig{
			EnvName:  "development",
			LogLevel: "info",
		},
		Server: ServerConfig{
			Address:               ":4018",
			BodyLimitBytes:        104857600,
			CORSAllowedOrigins:    "http://localhost:3000",
			CORSAllowedMethods:    "GET,POST,PUT,PATCH,DELETE,OPTIONS",
			CORSAllowedHeaders:    "Origin,Content-Type,Accept,Authorization,X-Request-ID",
			TLSTerminatedUpstream: false,
		},
		Tenancy: TenancyConfig{
			DefaultTenantID:         "11111111-1111-1111-1111-111111111111",
			DefaultTenantSlug:       "default",
			MultiTenantInfraEnabled: false,
		},
		Postgres: PostgresConfig{
			PrimaryHost:         "localhost",
			PrimaryPort:         "5432",
			PrimaryUser:         "matcher",
			PrimaryDB:           "matcher",
			PrimarySSLMode:      "disable",
			MaxOpenConnections:  25,
			MaxIdleConnections:  5,
			ConnMaxLifetimeMins: 30,
			ConnMaxIdleTimeMins: 5,
			ConnectTimeoutSec:   10,
			QueryTimeoutSec:     30,
			MigrationsPath:      "migrations",
		},
		Redis: RedisConfig{
			Host:           "localhost:6379",
			DB:             0,
			Protocol:       3,
			TLS:            false,
			PoolSize:       10,
			MinIdleConn:    2,
			ReadTimeoutMs:  3000,
			WriteTimeoutMs: 3000,
			DialTimeoutMs:  5000,
		},
		RabbitMQ: RabbitMQConfig{
			URI:                      "amqp",
			Host:                     "localhost",
			Port:                     "5672",
			User:                     "guest",
			Password:                 "guest",
			VHost:                    "/",
			HealthURL:                "http://localhost:15672",
			AllowInsecureHealthCheck: false,
		},
		Auth: AuthConfig{
			Enabled: false,
		},
		Swagger: SwaggerConfig{
			Enabled: false,
			Schemes: "https",
		},
		Telemetry: TelemetryConfig{
			Enabled:              false,
			ServiceName:          "matcher",
			LibraryName:          "github.com/LerianStudio/matcher",
			ServiceVersion:       "1.0.0",
			DeploymentEnv:        "development",
			CollectorEndpoint:    "localhost:4317",
			DBMetricsIntervalSec: 15,
		},
		RateLimit: RateLimitConfig{
			Enabled:           true,
			Max:               100,
			ExpirySec:         60,
			ExportMax:         10,
			ExportExpirySec:   60,
			DispatchMax:       50,
			DispatchExpirySec: 60,
		},
		Infrastructure: InfrastructureConfig{
			ConnectTimeoutSec: 30,
		},
		Idempotency: IdempotencyConfig{
			RetryWindowSec:  300,
			SuccessTTLHours: 168,
		},
		Dedupe: DedupeConfig{
			TTLSec: 3600,
		},
		ObjectStorage: ObjectStorageConfig{
			Endpoint:     "http://localhost:8333",
			Region:       "us-east-1",
			Bucket:       "matcher-exports",
			UsePathStyle: true,
		},
		ExportWorker: ExportWorkerConfig{
			Enabled:          true,
			PollIntervalSec:  5,
			PageSize:         1000,
			PresignExpirySec: 3600,
		},
		CleanupWorker: CleanupWorkerConfig{
			Enabled:        true,
			IntervalSec:    3600,
			BatchSize:      100,
			GracePeriodSec: 3600,
		},
		Scheduler: SchedulerConfig{
			IntervalSec: 60,
		},
		Archival: ArchivalConfig{
			Enabled:             false,
			IntervalHours:       24,
			HotRetentionDays:    90,
			WarmRetentionMonths: 24,
			ColdRetentionMonths: 84,
			BatchSize:           5000,
			StoragePrefix:       "archives/audit-logs",
			StorageClass:        "GLACIER",
			PartitionLookahead:  3,
			PresignExpirySec:    3600,
		},
		Webhook: WebhookConfig{
			TimeoutSec: 30,
		},
		CallbackRateLimit: CallbackRateLimitConfig{
			PerMinute: 60,
		},
		Fetcher: FetcherConfig{
			Enabled:              false,
			URL:                  "http://localhost:4006",
			HealthTimeoutSec:     5,
			RequestTimeoutSec:    30,
			DiscoveryIntervalSec: 60,
			SchemaCacheTTLSec:    300,
			ExtractionPollSec:    5,
			ExtractionTimeoutSec: 600,
		},
	}
}
