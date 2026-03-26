// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

	"github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/storageopt"
)

// buildPostgresClient creates a PostgreSQL client from snapshot values.
// It mirrors the DSN construction from Config.PrimaryDSN/ReplicaDSN and
// reuses the same libPostgres.New call pattern as init.go's
// createPostgresConnection.
func (factory *MatcherBundleFactory) buildPostgresClient(
	ctx context.Context,
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

	if resolver, resolveErr := conn.Resolver(ctx); resolveErr == nil {
		applySQLPoolSettings(
			resolver.PrimaryDBs(),
			time.Duration(cfg.ConnMaxLifetimeMins)*time.Minute,
			time.Duration(cfg.ConnMaxIdleTimeMins)*time.Minute,
		)
		applySQLPoolSettings(
			resolver.ReplicaDBs(),
			time.Duration(cfg.ConnMaxLifetimeMins)*time.Minute,
			time.Duration(cfg.ConnMaxIdleTimeMins)*time.Minute,
		)
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
	ctx context.Context,
	snap domain.Snapshot,
	logger libLog.Logger,
) *libRabbitmq.RabbitMQConnection {
	cfg := factory.extractRabbitMQConfig(snap)

	policyCfg := &Config{App: AppConfig{EnvName: factory.bootstrapCfg.EnvName}, RabbitMQ: RabbitMQConfig(cfg)}
	allowInsecure, _ := evaluateInsecureRabbitMQHealthCheckPolicy(policyCfg)

	if !allowInsecure && isInsecureHTTPHealthCheckURL(cfg.HealthURL) && logger != nil {
		logger.Log(ctx, libLog.LevelWarn,
			"RabbitMQ health URL uses HTTP while insecure checks are disabled; set RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK=true only for local/internal non-production environments")
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

var _ sharedPorts.ObjectStorageClient = (*objectStorageCloser)(nil)

// Close is a no-op for the stateless S3 SDK client.
func (c *objectStorageCloser) Close() error { return nil }

// Upload stores an object using the wrapped S3 client.
func (c *objectStorageCloser) Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error) {
	result, err := c.client.Upload(ctx, key, reader, contentType)
	if err != nil {
		return "", fmt.Errorf("upload object to S3 client: %w", err)
	}

	return result, nil
}

// UploadWithOptions stores an object using the wrapped S3 client and upload options.
func (c *objectStorageCloser) UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, opts ...storageopt.UploadOption) (string, error) {
	result, err := c.client.UploadWithOptions(ctx, key, reader, contentType, opts...)
	if err != nil {
		return "", fmt.Errorf("upload object with options to S3 client: %w", err)
	}

	return result, nil
}

// Download retrieves an object using the wrapped S3 client.
func (c *objectStorageCloser) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	reader, err := c.client.Download(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download object from S3 client: %w", err)
	}

	return reader, nil
}

// Delete removes an object using the wrapped S3 client.
func (c *objectStorageCloser) Delete(ctx context.Context, key string) error {
	if err := c.client.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete object from S3 client: %w", err)
	}

	return nil
}

// GeneratePresignedURL creates a download URL using the wrapped S3 client.
func (c *objectStorageCloser) GeneratePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	presignedURL, err := c.client.GeneratePresignedURL(ctx, key, expiry)
	if err != nil {
		return "", fmt.Errorf("generate presigned url from S3 client: %w", err)
	}

	return presignedURL, nil
}

// Exists checks whether an object exists using the wrapped S3 client.
func (c *objectStorageCloser) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := c.client.Exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("check object existence with S3 client: %w", err)
	}

	return exists, nil
}

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
		AllowInsecure:   snapBool(snap, "object_storage.allow_insecure_endpoint", false) && isAllowedInsecureObjectStorageEnvironment(factory.bootstrapCfg.EnvName),
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
		MaxOpenConnections:  snapInt(snap, "postgres.max_open_conns", defaultPGMaxOpenConns),
		MaxIdleConnections:  snapInt(snap, "postgres.max_idle_conns", defaultPGMaxIdleConns),
		ConnMaxLifetimeMins: snapInt(snap, "postgres.conn_max_lifetime_mins", defaultPGConnMaxLifeMins),
		ConnMaxIdleTimeMins: snapInt(snap, "postgres.conn_max_idle_time_mins", defaultPGConnMaxIdleMins),
		ConnectTimeoutSec:   snapInt(snap, "postgres.connect_timeout_sec", defaultPGConnectTimeout),
		QueryTimeoutSec:     snapInt(snap, "postgres.query_timeout_sec", defaultPGQueryTimeout),
	}
}

// redisConfigSnapshot is a snapshot-local Redis config holder, separate from the
// env-based RedisConfig to avoid confusion.
type redisConfigSnapshot struct {
	Host           string
	MasterName     string
	Password       string `json:"-"`
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
		MinIdleConn:    snapInt(snap, "redis.min_idle_conns", defaultRedisMinIdleConn),
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
	Password                 string `json:"-"`
	VHost                    string
	HealthURL                string
	AllowInsecureHealthCheck bool
}

// extractRabbitMQConfig reads all rabbitmq-related keys from the snapshot.
func (factory *MatcherBundleFactory) extractRabbitMQConfig(snap domain.Snapshot) rabbitMQConfigSnapshot {
	return rabbitMQConfigSnapshot{
		URI:  snapString(snap, "rabbitmq.url", "amqp"),
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
	value, ok := resolveSnapshotConfigValue(snap, key)
	if !ok {
		return fallback
	}

	if s, ok := value.(string); ok {
		return s
	}

	return fmt.Sprintf("%v", value)
}

// snapInt extracts an int from the snapshot, handling the type coercions that
// can arise from JSON deserialization (float64, int64, string).
func snapInt(snap domain.Snapshot, key string, fallback int) int {
	v, ok := resolveSnapshotConfigValue(snap, key)
	if !ok {
		return fallback
	}

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
	v, ok := resolveSnapshotConfigValue(snap, key)
	if !ok {
		return fallback
	}

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
