// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.BundleFactory = (*MatcherBundleFactory)(nil)

// MatcherBundleFactory creates MatcherBundle instances from snapshot configuration.
// It holds references to long-lived dependencies that remain constant across config
// changes (e.g., bootstrap-only keys extracted once at startup).
type MatcherBundleFactory struct {
	bootstrapCfg *BootstrapOnlyConfig
}

// BootstrapOnlyConfig holds the keys that are marked ApplyBootstrapOnly in the
// systemplane key definitions. These are extracted once at startup and never
// change at runtime.
type BootstrapOnlyConfig struct {
	// App
	EnvName string

	// Server
	ServerAddress         string
	TLSCertFile           string
	TLSKeyFile            string
	TLSTerminatedUpstream bool
	TrustedProxies        string

	// Auth
	AuthEnabled bool
	AuthHost    string
	// AuthTokenSecret holds the JWT signing secret. Stored as string for
	// consistency with the Config.Auth.TokenSecret field. Consider migrating
	// to []byte for future secret-zeroing on shutdown.
	AuthTokenSecret string

	// Telemetry
	TelemetryEnabled           bool
	TelemetryServiceName       string
	TelemetryLibraryName       string
	TelemetryServiceVersion    string
	TelemetryDeploymentEnv     string
	TelemetryCollectorEndpoint string
	TelemetryDBMetricsInterval int
}

// ErrBootstrapConfigNil indicates a nil bootstrap config was provided to the factory.
var ErrBootstrapConfigNil = errors.New("new matcher bundle factory: bootstrap config is required")

// NewMatcherBundleFactory creates a new factory with the given bootstrap config.
func NewMatcherBundleFactory(bootstrapCfg *BootstrapOnlyConfig) (*MatcherBundleFactory, error) {
	if bootstrapCfg == nil {
		return nil, ErrBootstrapConfigNil
	}

	return &MatcherBundleFactory{bootstrapCfg: bootstrapCfg}, nil
}

// Build creates a new MatcherBundle by reading config values from the snapshot
// and constructing infrastructure clients. On partial failure, already-constructed
// clients are closed before returning the error.
func (factory *MatcherBundleFactory) Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error) {
	loggerBundle, err := factory.buildLogger(snap)
	if err != nil {
		return nil, fmt.Errorf("build logger bundle: %w", err)
	}

	infra, err := factory.buildInfra(ctx, snap, loggerBundle.Logger)
	if err != nil {
		// Best-effort sync the logger before returning.
		_ = loggerBundle.Logger.Sync(ctx)

		return nil, fmt.Errorf("build infra bundle: %w", err)
	}

	httpPolicy := factory.buildHTTPPolicy(snap)

	return &MatcherBundle{
		Infra:  infra,
		HTTP:   httpPolicy,
		Logger: loggerBundle,
	}, nil
}

// buildHTTPPolicy extracts HTTP policy values from the snapshot with defaults.
func (factory *MatcherBundleFactory) buildHTTPPolicy(snap domain.Snapshot) *HTTPPolicyBundle {
	return &HTTPPolicyBundle{
		BodyLimitBytes:     snapInt(snap, "server.body_limit_bytes", defaultHTTPBodyLimitBytes),
		CORSAllowedOrigins: snapString(snap, "server.cors_allowed_origins", "http://localhost:3000"),
		CORSAllowedMethods: snapString(snap, "server.cors_allowed_methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS"),
		CORSAllowedHeaders: snapString(snap, "server.cors_allowed_headers", "Origin,Content-Type,Accept,Authorization,X-Request-ID"),
		SwaggerEnabled:     snapBool(snap, "swagger.enabled", false),
		SwaggerHost:        snapString(snap, "swagger.host", ""),
		SwaggerSchemes:     snapString(snap, "swagger.schemes", "https"),
	}
}

// buildLogger constructs a new logger from the snapshot's log level and the
// bootstrap environment name.
func (factory *MatcherBundleFactory) buildLogger(snap domain.Snapshot) (*LoggerBundle, error) {
	level := snapString(snap, "app.log_level", defaultLoggerLevel)
	resolvedLevel := ResolveLoggerLevel(level)
	env := ResolveLoggerEnvironment(factory.bootstrapCfg.EnvName)

	logger, err := libZap.New(libZap.Config{
		Environment:     env,
		Level:           resolvedLevel,
		OTelLibraryName: constants.ApplicationName,
	})
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}

	return &LoggerBundle{
		Logger: logger,
		Level:  resolvedLevel,
	}, nil
}

// buildInfra constructs all infrastructure clients from the snapshot.
// On failure, already-constructed clients are closed in reverse order.
func (factory *MatcherBundleFactory) buildInfra(
	ctx context.Context,
	snap domain.Snapshot,
	logger libLog.Logger,
) (*InfraBundle, error) {
	pgClient, err := factory.buildPostgresClient(snap, logger)
	if err != nil {
		return nil, fmt.Errorf("build postgres: %w", err)
	}

	redisClient, err := factory.buildRedisClient(ctx, snap, logger)
	if err != nil {
		_ = pgClient.Close()

		return nil, fmt.Errorf("build redis: %w", err)
	}

	rmqConn := factory.buildRabbitMQConnection(snap, logger)

	s3Client, err := factory.buildObjectStorageClient(ctx, snap)
	if err != nil {
		_ = closeRabbitMQ(rmqConn)
		_ = redisClient.Close()
		_ = pgClient.Close()

		return nil, fmt.Errorf("build object storage: %w", err)
	}

	return &InfraBundle{
		Postgres:      pgClient,
		Redis:         redisClient,
		RabbitMQ:      rmqConn,
		ObjectStorage: s3Client,
	}, nil
}

// buildPostgresClient creates a PostgreSQL client from snapshot values.
// It mirrors the DSN construction from Config.PrimaryDSN/ReplicaDSN and
// reuses the same libPostgres.New call pattern as init.go's
// createPostgresConnection.
func (factory *MatcherBundleFactory) buildPostgresClient(
	snap domain.Snapshot,
	logger libLog.Logger,
) (*libPostgres.Client, error) {
	cfg := factory.extractPostgresConfig(snap)

	primaryDSN := buildPostgresDSN(
		cfg.PrimaryHost, cfg.PrimaryPort, cfg.PrimaryUser,
		cfg.PrimaryPassword, cfg.PrimaryDB, cfg.PrimarySSLMode,
		cfg.ConnectTimeoutSec,
	)

	replicaDSN := primaryDSN
	if cfg.ReplicaHost != "" {
		replicaDSN = buildPostgresDSN(
			coalesce(cfg.ReplicaHost, cfg.PrimaryHost),
			coalesce(cfg.ReplicaPort, cfg.PrimaryPort),
			coalesce(cfg.ReplicaUser, cfg.PrimaryUser),
			coalesce(cfg.ReplicaPassword, cfg.PrimaryPassword),
			coalesce(cfg.ReplicaDB, cfg.PrimaryDB),
			coalesce(cfg.ReplicaSSLMode, cfg.PrimarySSLMode),
			cfg.ConnectTimeoutSec,
		)
	}

	conn, err := libPostgres.New(libPostgres.Config{
		PrimaryDSN:         primaryDSN,
		ReplicaDSN:         replicaDSN,
		Logger:             logger,
		MaxOpenConnections: cfg.MaxOpenConnections,
		MaxIdleConnections: cfg.MaxIdleConnections,
	})
	if err != nil {
		return nil, fmt.Errorf("create postgres client: %w", err)
	}

	return conn, nil
}

// buildRedisClient creates a Redis client from snapshot values. It replicates
// the topology detection logic from init.go's buildRedisConfig.
func (factory *MatcherBundleFactory) buildRedisClient(
	ctx context.Context,
	snap domain.Snapshot,
	logger libLog.Logger,
) (*libRedis.Client, error) {
	cfg := factory.extractRedisConfig(snap)
	redisCfg := buildLibRedisConfig(cfg, factory.bootstrapCfg.EnvName, logger)

	conn, err := libRedis.New(ctx, redisCfg)
	if err != nil {
		return nil, fmt.Errorf("create redis client: %w", err)
	}

	return conn, nil
}

// buildRabbitMQConnection creates a RabbitMQ connection struct from snapshot
// values. This mirrors init.go's createRabbitMQConnection but without the
// insecure health check policy evaluation (the factory delegates that to
// the connection itself).
func (factory *MatcherBundleFactory) buildRabbitMQConnection(
	snap domain.Snapshot,
	logger libLog.Logger,
) *libRabbitmq.RabbitMQConnection {
	cfg := factory.extractRabbitMQConfig(snap)

	allowInsecure := cfg.AllowInsecureHealthCheck
	if IsProductionEnvironment(factory.bootstrapCfg.EnvName) {
		allowInsecure = false
	}

	return &libRabbitmq.RabbitMQConnection{
		ConnectionStringSource:   buildRabbitMQDSN(cfg),
		HealthCheckURL:           cfg.HealthURL,
		Host:                     cfg.Host,
		Port:                     cfg.Port,
		User:                     cfg.User,
		Pass:                     cfg.Password,
		Logger:                   logger,
		AllowInsecureHealthCheck: allowInsecure,
	}
}

// objectStorageCloser wraps an S3Client (which has no Close method) in an
// io.Closer-compatible adapter. The AWS S3 SDK client is stateless over HTTP,
// so Close is a no-op.
type objectStorageCloser struct {
	client *storage.S3Client
}

// Close is a no-op for the stateless S3 SDK client.
func (c *objectStorageCloser) Close() error { return nil }

// buildObjectStorageClient creates an S3-compatible storage client from snapshot
// values. Returns (nil, nil) if endpoint or bucket is empty. The returned
// io.Closer wraps the stateless S3Client with a no-op Close.
func (factory *MatcherBundleFactory) buildObjectStorageClient(
	ctx context.Context,
	snap domain.Snapshot,
) (*objectStorageCloser, error) {
	endpoint := snapString(snap, "object_storage.endpoint", "")
	bucket := snapString(snap, "object_storage.bucket", "")

	if endpoint == "" || bucket == "" {
		// Not configured — endpoint or bucket is empty. Returns (nil, nil)
		// which callers must handle. MatcherBundle.Close() and InfraBundle
		// consumers guard with nil checks before accessing ObjectStorage.
		return nil, nil
	}

	s3Cfg := storage.S3Config{
		Endpoint:        endpoint,
		Region:          snapString(snap, "object_storage.region", "us-east-1"),
		Bucket:          bucket,
		AccessKeyID:     snapString(snap, "object_storage.access_key_id", ""),
		SecretAccessKey: snapString(snap, "object_storage.secret_access_key", ""),
		UsePathStyle:    snapBool(snap, "object_storage.use_path_style", true),
	}

	client, err := storage.NewS3Client(ctx, s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}

	return &objectStorageCloser{client: client}, nil
}

// extractPostgresConfig reads all postgres-related keys from the snapshot.
func (factory *MatcherBundleFactory) extractPostgresConfig(snap domain.Snapshot) PostgresConfig {
	return PostgresConfig{
		PrimaryHost:         snapString(snap, "postgres.primary_host", "localhost"),
		PrimaryPort:         snapString(snap, "postgres.primary_port", "5432"),
		PrimaryUser:         snapString(snap, "postgres.primary_user", "matcher"),
		PrimaryPassword:     snapString(snap, "postgres.primary_password", ""),
		PrimaryDB:           snapString(snap, "postgres.primary_db", "matcher"),
		PrimarySSLMode:      snapString(snap, "postgres.primary_ssl_mode", "disable"),
		ReplicaHost:         snapString(snap, "postgres.replica_host", ""),
		ReplicaPort:         snapString(snap, "postgres.replica_port", ""),
		ReplicaUser:         snapString(snap, "postgres.replica_user", ""),
		ReplicaPassword:     snapString(snap, "postgres.replica_password", ""),
		ReplicaDB:           snapString(snap, "postgres.replica_db", ""),
		ReplicaSSLMode:      snapString(snap, "postgres.replica_ssl_mode", ""),
		MaxOpenConnections:  snapInt(snap, "postgres.max_open_connections", defaultPGMaxOpenConns),
		MaxIdleConnections:  snapInt(snap, "postgres.max_idle_connections", defaultPGMaxIdleConns),
		ConnMaxLifetimeMins: snapInt(snap, "postgres.conn_max_lifetime_mins", defaultPGConnMaxLifeMins),
		ConnMaxIdleTimeMins: snapInt(snap, "postgres.conn_max_idle_time_mins", defaultPGConnMaxIdleMins),
		ConnectTimeoutSec:   snapInt(snap, "postgres.connect_timeout_sec", defaultPGConnectTimeout),
		QueryTimeoutSec:     snapInt(snap, "postgres.query_timeout_sec", defaultPGQueryTimeout),
		MigrationsPath:      snapString(snap, "postgres.migrations_path", "migrations"),
	}
}

// redisConfigSnapshot is a snapshot-local Redis config holder, separate from the
// env-based RedisConfig to avoid confusion.
type redisConfigSnapshot struct {
	Host           string
	MasterName     string
	Password       string
	DB             int
	Protocol       int
	TLS            bool
	CACert         string
	PoolSize       int
	MinIdleConn    int
	ReadTimeoutMs  int
	WriteTimeoutMs int
	DialTimeoutMs  int
}

// extractRedisConfig reads all redis-related keys from the snapshot.
func (factory *MatcherBundleFactory) extractRedisConfig(snap domain.Snapshot) redisConfigSnapshot {
	return redisConfigSnapshot{
		Host:           snapString(snap, "redis.host", "localhost:6379"),
		MasterName:     snapString(snap, "redis.master_name", ""),
		Password:       snapString(snap, "redis.password", ""),
		DB:             snapInt(snap, "redis.db", 0),
		Protocol:       snapInt(snap, "redis.protocol", defaultRedisProtocol),
		TLS:            snapBool(snap, "redis.tls", false),
		CACert:         snapString(snap, "redis.ca_cert", ""),
		PoolSize:       snapInt(snap, "redis.pool_size", defaultRedisPoolSize),
		MinIdleConn:    snapInt(snap, "redis.min_idle_conn", defaultRedisMinIdleConn),
		ReadTimeoutMs:  snapInt(snap, "redis.read_timeout_ms", defaultRedisReadTimeout),
		WriteTimeoutMs: snapInt(snap, "redis.write_timeout_ms", defaultRedisWriteTimeout),
		DialTimeoutMs:  snapInt(snap, "redis.dial_timeout_ms", defaultRedisDialTimeout),
	}
}

// rabbitMQConfigSnapshot is a snapshot-local RabbitMQ config holder.
type rabbitMQConfigSnapshot struct {
	URI                      string
	Host                     string
	Port                     string
	User                     string
	Password                 string
	VHost                    string
	HealthURL                string
	AllowInsecureHealthCheck bool
}

// extractRabbitMQConfig reads all rabbitmq-related keys from the snapshot.
func (factory *MatcherBundleFactory) extractRabbitMQConfig(snap domain.Snapshot) rabbitMQConfigSnapshot {
	return rabbitMQConfigSnapshot{
		URI:  snapString(snap, "rabbitmq.uri", "amqp"),
		Host: snapString(snap, "rabbitmq.host", "localhost"),
		Port: snapString(snap, "rabbitmq.port", "5672"),
		// Default guest/guest credentials are standard for local development.
		// Production environments must provide explicit credentials via the
		// systemplane store — the config validation layer rejects empty
		// credentials in production mode.
		User:                     snapString(snap, "rabbitmq.user", "guest"),
		Password:                 snapString(snap, "rabbitmq.password", "guest"),
		VHost:                    snapString(snap, "rabbitmq.vhost", "/"),
		HealthURL:                snapString(snap, "rabbitmq.health_url", "http://localhost:15672"),
		AllowInsecureHealthCheck: snapBool(snap, "rabbitmq.allow_insecure_health_check", false),
	}
}

// buildPostgresDSN constructs a PostgreSQL connection string. This is the same
// format used by Config.PrimaryDSN() in config_env.go. The password is
// single-quoted and escaped following libpq quoting rules.
func buildPostgresDSN(host, port, user, password, dbname, sslmode string, connectTimeoutSec int) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password='%s' dbname=%s sslmode=%s connect_timeout=%d",
		host, port, user, escapePGValue(password), dbname, sslmode, connectTimeoutSec,
	)
}

// escapePGValue escapes single quotes and backslashes in a PostgreSQL
// connection string value, following libpq's quoting rules.
func escapePGValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)

	return v
}

// buildLibRedisConfig constructs a libRedis.Config from the snapshot-extracted
// redis config. This mirrors init.go's buildRedisConfig function.
func buildLibRedisConfig(cfg redisConfigSnapshot, envName string, logger libLog.Logger) libRedis.Config {
	redisCfg := libRedis.Config{
		Auth: libRedis.Auth{
			StaticPassword: &libRedis.StaticPasswordAuth{
				Password: cfg.Password,
			},
		},
		Options: libRedis.ConnectionOptions{
			DB:           cfg.DB,
			Protocol:     cfg.Protocol,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConn,
			ReadTimeout:  time.Duration(cfg.ReadTimeoutMs) * time.Millisecond,
			WriteTimeout: time.Duration(cfg.WriteTimeoutMs) * time.Millisecond,
			DialTimeout:  time.Duration(cfg.DialTimeoutMs) * time.Millisecond,
		},
		Logger: logger,
	}

	if cfg.TLS {
		redisCfg.TLS = &libRedis.TLSConfig{
			CACertBase64: cfg.CACert,
		}
	}

	rawAddresses := strings.Split(cfg.Host, ",")
	addresses := make([]string, 0, len(rawAddresses))

	for _, addr := range rawAddresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			addresses = append(addresses, trimmed)
		}
	}

	switch {
	case cfg.MasterName != "":
		redisCfg.Topology = libRedis.Topology{
			Sentinel: &libRedis.SentinelTopology{
				Addresses:  addresses,
				MasterName: cfg.MasterName,
			},
		}
	case len(addresses) > 1:
		redisCfg.Topology = libRedis.Topology{
			Cluster: &libRedis.ClusterTopology{
				Addresses: addresses,
			},
		}
	default:
		addr := strings.TrimSpace(cfg.Host)
		if addr == "" && !IsProductionEnvironment(envName) {
			addr = "localhost:6379"
		}

		redisCfg.Topology = libRedis.Topology{
			Standalone: &libRedis.StandaloneTopology{
				Address: addr,
			},
		}
	}

	return redisCfg
}

// buildRabbitMQDSN constructs an AMQP connection URI from the RabbitMQ config.
// This mirrors Config.RabbitMQDSN() in config_env.go.
func buildRabbitMQDSN(cfg rabbitMQConfigSnapshot) string {
	var userinfo *url.Userinfo
	if cfg.Password == "" {
		userinfo = url.User(cfg.User)
	} else {
		userinfo = url.UserPassword(cfg.User, cfg.Password)
	}

	connURL := url.URL{
		Scheme: cfg.URI,
		User:   userinfo,
		Host:   net.JoinHostPort(cfg.Host, cfg.Port),
	}

	vhostRaw := strings.TrimSpace(cfg.VHost)
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

// snapString extracts a string from the snapshot, or returns the fallback.
func snapString(snap domain.Snapshot, key, fallback string) string {
	v := snap.ConfigValue(key, fallback)
	if s, ok := v.(string); ok {
		return s
	}

	return fmt.Sprintf("%v", v)
}

// snapInt extracts an int from the snapshot, handling the type coercions that
// can arise from JSON deserialization (float64, int64, string).
func snapInt(snap domain.Snapshot, key string, fallback int) int {
	v := snap.ConfigValue(key, fallback)

	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	case string:
		n, err := strconv.Atoi(val)
		if err != nil {
			return fallback
		}

		return n
	default:
		return fallback
	}
}

// snapBool extracts a bool from the snapshot, handling string representations.
func snapBool(snap domain.Snapshot, key string, fallback bool) bool {
	v := snap.ConfigValue(key, fallback)

	switch val := v.(type) {
	case bool:
		return val
	case string:
		return strings.EqualFold(val, "true") || val == "1"
	default:
		return fallback
	}
}

// coalesce returns the first non-empty string from the arguments.
func coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}

	return ""
}
