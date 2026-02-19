package adapters

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"
	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for connection manager operations.
var (
	// ErrConnectionManagerClosed is returned when operations are attempted on a closed manager.
	ErrConnectionManagerClosed = errors.New("connection manager is closed")
	// ErrCloseConnections is returned when errors occur during connection cleanup.
	ErrCloseConnections = errors.New("errors closing connections")
	// ErrConfigurationPortRequired is returned when configPort is nil.
	ErrConfigurationPortRequired = errors.New("configuration port is required")
	// ErrNoPrimaryDatabaseConfigured is returned when no primary database is configured.
	ErrNoPrimaryDatabaseConfigured = errors.New(
		"no primary database configured for multi-tenant transaction",
	)
	// errUnexpectedConnectionType is returned when singleflight returns an unexpected type.
	errUnexpectedConnectionType = errors.New("unexpected connection type from singleflight")
	// ErrRedisAddressesRequired is returned when Redis configuration has no usable addresses.
	ErrRedisAddressesRequired = errors.New("redis addresses are required")
)

var _ ports.InfrastructureProvider = (*TenantConnectionManager)(nil)

const defaultConnectionSetupTimeout = 15 * time.Second

type cachedPostgresConnection struct {
	conn *libPostgres.Client
}

type cachedRedisConnection struct {
	conn *libRedis.Client
}

// TenantConnectionManager resolves and caches connections per unique infrastructure configuration.
// It uses ConfigurationPort to get tenant-specific config and caches connections by a stable
// "infra key" derived from connection parameters (not raw tenant ID) to avoid duplicate pools
// when multiple tenants share the same infrastructure.
type TenantConnectionManager struct {
	configPort ports.ConfigurationPort

	postgresClientFactory func(cfg libPostgres.Config) (*libPostgres.Client, error)
	postgresConnector     func(ctx context.Context, client *libPostgres.Client) error
	redisClientFactory    func(ctx context.Context, cfg libRedis.Config) (*libRedis.Client, error)

	mu            sync.RWMutex
	postgresCache map[string]*cachedPostgresConnection
	redisCache    map[string]*cachedRedisConnection
	closed        bool

	postgresSF singleflight.Group
	redisSF    singleflight.Group

	maxOpenConns        int
	maxIdleConns        int
	connMaxLifetimeMins int
	connMaxIdleTimeMins int
}

// NewTenantConnectionManager creates a TenantConnectionManager with the given configuration port.
// Returns (nil, error) if configPort is nil.
func NewTenantConnectionManager(
	configPort ports.ConfigurationPort,
	maxOpenConns, maxIdleConns, connMaxLifetimeMins, connMaxIdleTimeMins int,
) (*TenantConnectionManager, error) {
	if configPort == nil {
		return nil, ErrConfigurationPortRequired
	}

	if maxOpenConns <= 0 {
		maxOpenConns = 25
	}

	if maxIdleConns <= 0 {
		maxIdleConns = 5
	}

	if connMaxLifetimeMins <= 0 {
		connMaxLifetimeMins = 30
	}

	if connMaxIdleTimeMins <= 0 {
		connMaxIdleTimeMins = 5
	}

	return &TenantConnectionManager{
		configPort:            configPort,
		postgresClientFactory: libPostgres.New,
		postgresConnector: func(ctx context.Context, client *libPostgres.Client) error {
			return client.Connect(ctx)
		},
		redisClientFactory:  libRedis.New,
		postgresCache:       make(map[string]*cachedPostgresConnection),
		redisCache:          make(map[string]*cachedRedisConnection),
		maxOpenConns:        maxOpenConns,
		maxIdleConns:        maxIdleConns,
		connMaxLifetimeMins: connMaxLifetimeMins,
		connMaxIdleTimeMins: connMaxIdleTimeMins,
	}, nil
}

// GetPostgresConnection returns a postgres connection for the tenant in context.
// Connections are cached by infrastructure key to avoid duplicate pools.
// Uses singleflight to prevent duplicate connection creation when multiple
// goroutines request the same infrastructure key concurrently.
func (mgr *TenantConnectionManager) GetPostgresConnection(
	ctx context.Context,
) (*libPostgres.Client, error) {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"tenant_connection_manager.get_postgres",
	)

	mgr.mu.RLock()

	if mgr.closed {
		mgr.mu.RUnlock()

		return nil, ErrConnectionManagerClosed
	}

	mgr.mu.RUnlock()

	tenantID := auth.GetTenantID(ctx)

	cfg, err := mgr.configPort.GetTenantConfig(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant config for postgres: %w", err)
	}

	if err := asserter.NotNil(ctx, cfg, "tenant config is nil", "tenant_id", tenantID); err != nil {
		return nil, fmt.Errorf("assert tenant config: %w", err)
	}

	infraKey := postgresInfraKey(cfg)

	mgr.mu.RLock()

	if cached, ok := mgr.postgresCache[infraKey]; ok {
		mgr.mu.RUnlock()

		return cached.conn, nil
	}

	mgr.mu.RUnlock()

	result, err, _ := mgr.postgresSF.Do(infraKey, func() (any, error) {
		setupCtx, cancel := newConnectionSetupContext(ctx)
		defer cancel()

		mgr.mu.Lock()
		defer mgr.mu.Unlock()

		if mgr.closed {
			return nil, ErrConnectionManagerClosed
		}

		if cached, ok := mgr.postgresCache[infraKey]; ok {
			return cached.conn, nil
		}

		conn, createErr := mgr.createPostgresConnection(setupCtx, cfg, infraKey)
		if createErr != nil {
			return nil, createErr
		}

		mgr.postgresCache[infraKey] = &cachedPostgresConnection{
			conn: conn,
		}

		return conn, nil
	})
	if err != nil {
		return nil, fmt.Errorf("singleflight get postgres connection: %w", err)
	}

	conn, ok := result.(*libPostgres.Client)
	if !ok {
		return nil, fmt.Errorf("%w: %T", errUnexpectedConnectionType, result)
	}

	return conn, nil
}

// GetRedisConnection returns a redis connection for the tenant in context.
// Connections are cached by infrastructure key to avoid duplicate pools.
// Uses singleflight to prevent duplicate connection creation when multiple
// goroutines request the same infrastructure key concurrently.
func (mgr *TenantConnectionManager) GetRedisConnection(
	ctx context.Context,
) (*libRedis.Client, error) {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"tenant_connection_manager.get_redis",
	)

	mgr.mu.RLock()

	if mgr.closed {
		mgr.mu.RUnlock()

		return nil, ErrConnectionManagerClosed
	}

	mgr.mu.RUnlock()

	tenantID := auth.GetTenantID(ctx)

	cfg, err := mgr.configPort.GetTenantConfig(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant config for redis: %w", err)
	}

	if err := asserter.NotNil(ctx, cfg, "tenant config is nil", "tenant_id", tenantID); err != nil {
		return nil, fmt.Errorf("assert tenant config: %w", err)
	}

	if _, topologyErr := redisTopology(cfg); topologyErr != nil {
		return nil, fmt.Errorf("invalid redis tenant config: %w", topologyErr)
	}

	infraKey := redisInfraKey(cfg)

	mgr.mu.RLock()

	if cached, ok := mgr.redisCache[infraKey]; ok {
		mgr.mu.RUnlock()

		return cached.conn, nil
	}

	mgr.mu.RUnlock()

	result, err, _ := mgr.redisSF.Do(infraKey, func() (any, error) {
		setupCtx, cancel := newConnectionSetupContext(ctx)
		defer cancel()

		mgr.mu.Lock()
		defer mgr.mu.Unlock()

		if mgr.closed {
			return nil, ErrConnectionManagerClosed
		}

		if cached, ok := mgr.redisCache[infraKey]; ok {
			return cached.conn, nil
		}

		conn, createErr := mgr.createRedisConnection(setupCtx, cfg, infraKey)
		if createErr != nil {
			return nil, createErr
		}

		mgr.redisCache[infraKey] = &cachedRedisConnection{
			conn: conn,
		}

		return conn, nil
	})
	if err != nil {
		return nil, fmt.Errorf("singleflight get redis connection: %w", err)
	}

	conn, ok := result.(*libRedis.Client)
	if !ok {
		return nil, fmt.Errorf("%w: %T", errUnexpectedConnectionType, result)
	}

	return conn, nil
}

// BeginTx starts a tenant-scoped database transaction.
// The caller is responsible for calling Commit() or Rollback() on the returned transaction.
func (mgr *TenantConnectionManager) BeginTx(ctx context.Context) (*sql.Tx, error) {
	conn, err := mgr.GetPostgresConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	db, err := conn.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	primaryDBs := db.PrimaryDBs()
	if len(primaryDBs) == 0 {
		return nil, ErrNoPrimaryDatabaseConfigured
	}

	tx, err := primaryDBs[0].BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
			if logger != nil {
				logger.Log(ctx, libLog.LevelError, fmt.Sprintf(
					"failed to rollback transaction after apply tenant schema error: %v (apply error: %v)",
					rollbackErr,
					err,
				))
			}

			return nil, fmt.Errorf(
				"failed to apply tenant schema: %w",
				errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr)),
			)
		}

		return nil, fmt.Errorf("failed to apply tenant schema: %w", err)
	}

	return tx, nil
}

// GetReplicaDB returns the replica database for read-only queries.
// Falls back to primary if no replica is configured.
//
// WARNING: The returned *sql.DB does NOT have tenant schema isolation applied.
// Callers MUST use pgcommon.WithTenantRead or pgcommon.WithTenantReadQuery
// to ensure tenant-scoped reads, or manually apply the schema via
// SET search_path before executing queries. Direct use without schema scoping
// in multi-tenant mode will cause cross-tenant data leakage.
func (mgr *TenantConnectionManager) GetReplicaDB(ctx context.Context) (*sql.DB, error) {
	conn, err := mgr.GetPostgresConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	db, err := conn.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	replicaDBs := db.ReplicaDBs()
	if len(replicaDBs) > 0 {
		return replicaDBs[0], nil
	}

	primaryDBs := db.PrimaryDBs()
	if len(primaryDBs) == 0 {
		return nil, ErrNoPrimaryDatabaseConfigured
	}

	return primaryDBs[0], nil
}

// Close closes all cached connections. Safe to call multiple times.
func (mgr *TenantConnectionManager) Close() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if mgr.closed {
		return nil
	}

	mgr.closed = true

	var errs []error

	for infraKey, cached := range mgr.postgresCache {
		if cached.conn != nil {
			if err := cached.conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close postgres [%s]: %w", infraKey, err))
			}
		}
	}

	for infraKey, cached := range mgr.redisCache {
		if cached.conn != nil {
			if err := cached.conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close redis [%s]: %w", infraKey, err))
			}
		}
	}

	mgr.postgresCache = nil
	mgr.redisCache = nil

	if len(errs) > 0 {
		allErrs := append([]error{ErrCloseConnections}, errs...)
		return errors.Join(allErrs...)
	}

	return nil
}

//nolint:dogsled // NewTrackingFromContext returns 4 values; we only need logger
func (mgr *TenantConnectionManager) createPostgresConnection(
	ctx context.Context,
	cfg *ports.TenantConfig,
	infraKey string,
) (*libPostgres.Client, error) {
	normalized := normalizePostgresConfig(cfg)

	conn, err := mgr.postgresClientFactory(libPostgres.Config{
		PrimaryDSN:         normalized.primaryDSN,
		ReplicaDSN:         normalized.replicaDSN,
		MaxOpenConnections: mgr.maxOpenConns,
		MaxIdleConnections: mgr.maxIdleConns,
	})
	if err != nil {
		return nil, fmt.Errorf("create postgres client [%s]: %w", infraKey, err)
	}

	if err := mgr.postgresConnector(ctx, conn); err != nil {
		return nil, fmt.Errorf("connect postgres [%s]: %w", infraKey, err)
	}

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)

	resolver, resolverErr := conn.Resolver(ctx)
	if resolverErr != nil {
		if logger != nil {
			logger.With(
				libLog.String("type", "postgres"),
				libLog.String("infra_key", infraKey),
				libLog.String("error", resolverErr.Error()),
			).Log(ctx, libLog.LevelWarn, "unable to apply SQL pool settings from resolver")
		}
	} else {
		maxLifetime := time.Duration(mgr.connMaxLifetimeMins) * time.Minute
		maxIdle := time.Duration(mgr.connMaxIdleTimeMins) * time.Minute
		applySQLPoolSettings(resolver.PrimaryDBs(), maxLifetime, maxIdle)
		applySQLPoolSettings(resolver.ReplicaDBs(), maxLifetime, maxIdle)
	}

	if logger != nil {
		logger.With(
			libLog.String("type", "postgres"),
			libLog.String("infra_key", infraKey),
			libLog.String("primary_db", normalized.primaryDB),
		).Log(ctx, libLog.LevelInfo, "created new postgres connection pool")
	}

	return conn, nil
}

// Default timeout values for Redis connections.
const (
	defaultRedisReadTimeout  = 3 * time.Second
	defaultRedisWriteTimeout = 3 * time.Second
	defaultRedisDialTimeout  = 5 * time.Second
	defaultRedisPoolSize     = 10
	defaultRedisMinIdleConns = 2
)

// redisConnectionOptions builds ConnectionOptions from tenant config, applying defaults.
func redisConnectionOptions(cfg *ports.TenantConfig) libRedis.ConnectionOptions {
	return libRedis.ConnectionOptions{
		DB:           cfg.RedisDB,
		Protocol:     cfg.RedisProtocol,
		PoolSize:     intOrDefault(cfg.RedisPoolSize, defaultRedisPoolSize),
		MinIdleConns: intOrDefault(cfg.RedisMinIdleConns, defaultRedisMinIdleConns),
		ReadTimeout:  durationOrDefault(cfg.RedisReadTimeout, defaultRedisReadTimeout),
		WriteTimeout: durationOrDefault(cfg.RedisWriteTimeout, defaultRedisWriteTimeout),
		DialTimeout:  durationOrDefault(cfg.RedisDialTimeout, defaultRedisDialTimeout),
	}
}

func normalizeRedisAddresses(addresses []string) []string {
	normalized := make([]string, 0, len(addresses))

	for _, address := range addresses {
		trimmed := strings.TrimSpace(address)
		if trimmed == "" {
			continue
		}

		normalized = append(normalized, trimmed)
	}

	return normalized
}

// redisTopology determines the Redis topology from tenant config.
func redisTopology(cfg *ports.TenantConfig) (libRedis.Topology, error) {
	addresses := normalizeRedisAddresses(cfg.RedisAddresses)

	switch {
	case cfg.RedisMasterName != "":
		if len(addresses) == 0 {
			return libRedis.Topology{}, ErrRedisAddressesRequired
		}

		return libRedis.Topology{
			Sentinel: &libRedis.SentinelTopology{
				Addresses:  addresses,
				MasterName: cfg.RedisMasterName,
			},
		}, nil
	case len(addresses) > 1:
		return libRedis.Topology{
			Cluster: &libRedis.ClusterTopology{
				Addresses: addresses,
			},
		}, nil
	default:
		if len(addresses) == 0 {
			return libRedis.Topology{}, ErrRedisAddressesRequired
		}

		return libRedis.Topology{
			Standalone: &libRedis.StandaloneTopology{
				Address: addresses[0],
			},
		}, nil
	}
}

//nolint:dogsled // NewTrackingFromContext returns 4 values; we only need logger
func (mgr *TenantConnectionManager) createRedisConnection(
	ctx context.Context,
	cfg *ports.TenantConfig,
	infraKey string,
) (*libRedis.Client, error) {
	topology, topologyErr := redisTopology(cfg)
	if topologyErr != nil {
		return nil, fmt.Errorf("resolve redis topology [%s]: %w", infraKey, topologyErr)
	}

	redisCfg := libRedis.Config{
		Auth: libRedis.Auth{
			StaticPassword: &libRedis.StaticPasswordAuth{
				Password: cfg.RedisPassword,
			},
		},
		Options:  redisConnectionOptions(cfg),
		Topology: topology,
	}

	if cfg.RedisUseTLS {
		redisCfg.TLS = &libRedis.TLSConfig{
			CACertBase64: cfg.RedisCACert,
		}
	}

	conn, err := mgr.redisClientFactory(ctx, redisCfg)
	if err != nil {
		return nil, fmt.Errorf("connect redis [%s]: %w", infraKey, err)
	}

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
	if logger != nil {
		logger.With(
			libLog.String("type", "redis"),
			libLog.String("infra_key", infraKey),
			libLog.Int("address_count", len(normalizeRedisAddresses(cfg.RedisAddresses))),
		).Log(ctx, libLog.LevelInfo, "created new redis connection pool")
	}

	return conn, nil
}

// postgresInfraKey generates a stable key from postgres config to identify unique infrastructure.
// Empty string fields are valid and contribute to the key; cfg must not be nil.
func postgresInfraKey(cfg *ports.TenantConfig) string {
	normalized := normalizePostgresConfig(cfg)

	data := fmt.Sprintf("pg:%s:%s",
		normalized.primaryDSN,
		normalized.replicaDSN,
	)

	return hashKey(data)
}

// redisInfraKey generates a stable key from redis config to identify unique infrastructure.
// Empty fields and nil RedisAddresses slice are valid and handled gracefully.
// Note: Password is included in the hash to ensure tenants with different credentials
// don't share connections, even if they use the same Redis addresses. The hash output
// does not expose the password since it's a one-way SHA256 hash.
// cfg must not be nil.
func redisInfraKey(cfg *ports.TenantConfig) string {
	addresses := normalizeRedisAddresses(cfg.RedisAddresses)
	sort.Strings(addresses)

	options := redisConnectionOptions(cfg)

	topologyType := "standalone"
	if cfg.RedisMasterName != "" {
		topologyType = "sentinel"
	} else if len(addresses) > 1 {
		topologyType = "cluster"
	}

	data := fmt.Sprintf("redis:%s:%s:%d:%s:%s:%d:%t:%s:%d:%d:%d:%d:%d",
		strings.Join(addresses, ","),
		cfg.RedisPassword,
		options.DB,
		cfg.RedisMasterName,
		topologyType,
		options.Protocol,
		cfg.RedisUseTLS,
		cfg.RedisCACert,
		options.ReadTimeout,
		options.WriteTimeout,
		options.DialTimeout,
		options.PoolSize,
		options.MinIdleConns,
	)

	return hashKey(data)
}

type normalizedPostgres struct {
	primaryDSN string
	replicaDSN string
	primaryDB  string
}

func normalizePostgresConfig(cfg *ports.TenantConfig) normalizedPostgres {
	replicaDSN := cfg.PostgresReplicaDSN
	if replicaDSN == "" {
		replicaDSN = cfg.PostgresPrimaryDSN
	}

	return normalizedPostgres{
		primaryDSN: cfg.PostgresPrimaryDSN,
		replicaDSN: replicaDSN,
		primaryDB:  cfg.PostgresPrimaryDB,
	}
}

func hashKey(data string) string {
	h := sha256.Sum256([]byte(data))

	return hex.EncodeToString(h[:])
}

// intOrDefault returns value if positive, otherwise the default.
func intOrDefault(value, defaultValue int) int {
	if value > 0 {
		return value
	}

	return defaultValue
}

// durationOrDefault returns value if positive, otherwise the default.
func durationOrDefault(value, defaultValue time.Duration) time.Duration {
	if value > 0 {
		return value
	}

	return defaultValue
}

func applySQLPoolSettings(dbs []*sql.DB, maxLifetime, maxIdle time.Duration) {
	for _, db := range dbs {
		if db == nil {
			continue
		}

		db.SetConnMaxLifetime(maxLifetime)
		db.SetConnMaxIdleTime(maxIdle)
	}
}

func newConnectionSetupContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}

	return context.WithTimeout(context.WithoutCancel(parent), defaultConnectionSetupTimeout)
}
