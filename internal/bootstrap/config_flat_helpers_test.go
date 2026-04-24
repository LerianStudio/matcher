// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap


type flatConfig struct {
	EnvName                     string
	LogLevel                    string
	DefaultTenantID             string
	DefaultTenantSlug           string
	MultiTenantEnabled          bool
	MultiTenantURL              string
	BodyLimitBytes              int
	CORSAllowedOrigins          string
	TLSTerminatedUpstream       bool
	ServerTLSCertFile           string
	ServerTLSKeyFile            string
	PrimaryDBHost               string
	PrimaryDBPort               string
	PrimaryDBUser               string
	PrimaryDBPassword           string
	PrimaryDBName               string
	PrimaryDBSSLMode            string
	ReplicaDBHost               string
	ReplicaDBPort               string
	ReplicaDBUser               string
	ReplicaDBPassword           string
	ReplicaDBName               string
	ReplicaDBSSLMode            string
	PostgresConnectTimeoutSec   int
	PostgresQueryTimeoutSec     int
	MaxOpenConnections          int
	MaxIdleConnections          int
	ConnMaxLifetimeMins         int
	ConnMaxIdleTimeMins         int
	RedisHost                   string
	RedisMasterName             string
	RedisPassword               string
	RedisDB                     int
	RedisProtocol               int
	RedisTLS                    bool
	RedisPoolSize               int
	RedisMinIdleConn            int
	RedisReadTimeoutMs          int
	RedisWriteTimeoutMs         int
	RedisDialTimeoutMs          int
	RabbitMQURI                 string
	RabbitMQHost                string
	RabbitMQPort                string
	RabbitMQUser                string
	RabbitMQPassword            string
	RabbitMQVHost               string
	RabbitMQHealthURL           string
	RabbitMQAllowInsecureHealth bool
	AuthEnabled                 bool
	AuthHost                    string
	AuthTokenSecret             string
	SwaggerEnabled              bool
	SwaggerHost                 string
	EnableTelemetry             bool
	OtelDeploymentEnv           string
	DBMetricsIntervalSec        int
	RateLimitEnabled            bool
	RateLimitMax                int
	RateLimitExpirySec          int
	ExportRateLimitMax          int
	ExportRateLimitExpirySec    int
	DispatchRateLimitMax        int
	DispatchRateLimitExpirySec  int
	AdminRateLimitMax           int
	AdminRateLimitExpirySec     int
	InfraConnectTimeoutSec      int
	IdempotencyRetryWindowSec   int
	IdempotencySuccessTTLHours  int
	ExportWorkerPollIntervalSec int
	ObjectStorageEndpoint       string
	ObjectStorageBucket         string
	WebhookTimeoutSec           int
	ArchivalEnabled             bool
	ArchivalIntervalHours       int
	ArchivalHotRetentionDays    int
	ArchivalWarmRetentionMonths int
	ArchivalColdRetentionMonths int
	ArchivalBatchSize           int
	ArchivalStorageBucket       string
	ArchivalStoragePrefix       string
	ArchivalStorageClass        string
	ArchivalPartitionLookahead  int
	ArchivalPresignExpirySec    int
}

func buildConfig(fc flatConfig) Config {
	cfg := Config{}
	cfg.App.EnvName = fc.EnvName
	cfg.App.LogLevel = fc.LogLevel
	cfg.Tenancy.DefaultTenantID = fc.DefaultTenantID
	cfg.Tenancy.DefaultTenantSlug = fc.DefaultTenantSlug
	cfg.Tenancy.MultiTenantEnabled = fc.MultiTenantEnabled
	cfg.Tenancy.MultiTenantURL = fc.MultiTenantURL
	cfg.Server.BodyLimitBytes = fc.BodyLimitBytes
	cfg.Server.CORSAllowedOrigins = fc.CORSAllowedOrigins
	cfg.Server.TLSTerminatedUpstream = fc.TLSTerminatedUpstream
	cfg.Server.TLSCertFile = fc.ServerTLSCertFile
	cfg.Server.TLSKeyFile = fc.ServerTLSKeyFile
	cfg.Postgres.PrimaryHost = fc.PrimaryDBHost
	cfg.Postgres.PrimaryPort = fc.PrimaryDBPort
	cfg.Postgres.PrimaryUser = fc.PrimaryDBUser
	cfg.Postgres.PrimaryPassword = fc.PrimaryDBPassword
	cfg.Postgres.PrimaryDB = fc.PrimaryDBName
	cfg.Postgres.PrimarySSLMode = fc.PrimaryDBSSLMode
	cfg.Postgres.ReplicaHost = fc.ReplicaDBHost
	cfg.Postgres.ReplicaPort = fc.ReplicaDBPort
	cfg.Postgres.ReplicaUser = fc.ReplicaDBUser
	cfg.Postgres.ReplicaPassword = fc.ReplicaDBPassword
	cfg.Postgres.ReplicaDB = fc.ReplicaDBName
	cfg.Postgres.ReplicaSSLMode = fc.ReplicaDBSSLMode
	cfg.Postgres.ConnectTimeoutSec = fc.PostgresConnectTimeoutSec
	cfg.Postgres.QueryTimeoutSec = fc.PostgresQueryTimeoutSec
	cfg.Postgres.MaxOpenConnections = fc.MaxOpenConnections
	cfg.Postgres.MaxIdleConnections = fc.MaxIdleConnections
	cfg.Postgres.ConnMaxLifetimeMins = fc.ConnMaxLifetimeMins
	cfg.Postgres.ConnMaxIdleTimeMins = fc.ConnMaxIdleTimeMins
	cfg.Redis.Host = fc.RedisHost
	cfg.Redis.MasterName = fc.RedisMasterName
	cfg.Redis.Password = fc.RedisPassword
	cfg.Redis.DB = fc.RedisDB
	cfg.Redis.Protocol = fc.RedisProtocol
	cfg.Redis.TLS = fc.RedisTLS
	cfg.Redis.PoolSize = fc.RedisPoolSize
	cfg.Redis.MinIdleConn = fc.RedisMinIdleConn
	cfg.Redis.ReadTimeoutMs = fc.RedisReadTimeoutMs
	cfg.Redis.WriteTimeoutMs = fc.RedisWriteTimeoutMs
	cfg.Redis.DialTimeoutMs = fc.RedisDialTimeoutMs
	cfg.RabbitMQ.URI = fc.RabbitMQURI
	cfg.RabbitMQ.Host = fc.RabbitMQHost
	cfg.RabbitMQ.Port = fc.RabbitMQPort
	cfg.RabbitMQ.User = fc.RabbitMQUser
	cfg.RabbitMQ.Password = fc.RabbitMQPassword
	cfg.RabbitMQ.VHost = fc.RabbitMQVHost
	cfg.RabbitMQ.HealthURL = fc.RabbitMQHealthURL
	cfg.RabbitMQ.AllowInsecureHealthCheck = fc.RabbitMQAllowInsecureHealth
	cfg.Auth.Enabled = fc.AuthEnabled
	cfg.Auth.Host = fc.AuthHost
	cfg.Auth.TokenSecret = fc.AuthTokenSecret
	cfg.Swagger.Enabled = fc.SwaggerEnabled
	cfg.Swagger.Host = fc.SwaggerHost
	cfg.Telemetry.Enabled = fc.EnableTelemetry
	cfg.Telemetry.DeploymentEnv = fc.OtelDeploymentEnv
	cfg.Telemetry.DBMetricsIntervalSec = fc.DBMetricsIntervalSec
	cfg.RateLimit.Enabled = fc.RateLimitEnabled
	cfg.RateLimit.Max = fc.RateLimitMax
	cfg.RateLimit.ExpirySec = fc.RateLimitExpirySec
	cfg.RateLimit.ExportMax = fc.ExportRateLimitMax
	cfg.RateLimit.ExportExpirySec = fc.ExportRateLimitExpirySec
	cfg.RateLimit.DispatchMax = fc.DispatchRateLimitMax
	cfg.RateLimit.DispatchExpirySec = fc.DispatchRateLimitExpirySec
	cfg.RateLimit.AdminMax = fc.AdminRateLimitMax
	cfg.RateLimit.AdminExpirySec = fc.AdminRateLimitExpirySec
	cfg.Infrastructure.ConnectTimeoutSec = fc.InfraConnectTimeoutSec
	cfg.Idempotency.RetryWindowSec = fc.IdempotencyRetryWindowSec
	cfg.Idempotency.SuccessTTLHours = fc.IdempotencySuccessTTLHours
	cfg.ExportWorker.PollIntervalSec = fc.ExportWorkerPollIntervalSec
	cfg.ObjectStorage.Endpoint = fc.ObjectStorageEndpoint
	cfg.ObjectStorage.Bucket = fc.ObjectStorageBucket
	cfg.Webhook.TimeoutSec = fc.WebhookTimeoutSec
	cfg.Archival.Enabled = fc.ArchivalEnabled
	cfg.Archival.IntervalHours = fc.ArchivalIntervalHours
	cfg.Archival.StorageBucket = fc.ArchivalStorageBucket
	cfg.Archival.StoragePrefix = fc.ArchivalStoragePrefix
	cfg.Archival.StorageClass = fc.ArchivalStorageClass
	cfg.Archival.PresignExpirySec = fc.ArchivalPresignExpirySec

	// Apply archival defaults matching envDefault values to prevent
	// validation failures in tests that don't set archival fields.
	cfg.Archival.HotRetentionDays = applyDefault(fc.ArchivalHotRetentionDays, 90)
	cfg.Archival.WarmRetentionMonths = applyDefault(fc.ArchivalWarmRetentionMonths, 24)
	cfg.Archival.ColdRetentionMonths = applyDefault(fc.ArchivalColdRetentionMonths, 84)
	cfg.Archival.BatchSize = applyDefault(fc.ArchivalBatchSize, 5000)
	cfg.Archival.PartitionLookahead = applyDefault(fc.ArchivalPartitionLookahead, 3)

	return cfg
}

// applyDefault returns the value if non-zero, otherwise the default.
func applyDefault(value, defaultValue int) int {
	if value != 0 {
		return value
	}

	return defaultValue
}
