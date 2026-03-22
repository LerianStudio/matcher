// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/redis/go-redis/v9"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
)

// TenantConfig holds infrastructure configuration for a specific tenant.
type TenantConfig struct {
	PostgresPrimaryDSN string `json:"postgresPrimaryDSN"`
	PostgresReplicaDSN string `json:"postgresReplicaDSN"`
	PostgresPrimaryDB  string `json:"postgresPrimaryDB"`
	PostgresReplicaDB  string `json:"postgresReplicaDB"`

	RedisAddresses    []string      `json:"redisAddresses"`
	RedisPassword     string        `json:"redisPassword"`
	RedisDB           int           `json:"redisDB"`
	RedisMasterName   string        `json:"redisMasterName"`
	RedisProtocol     int           `json:"redisProtocol"`
	RedisUseTLS       bool          `json:"redisUseTLS"`
	RedisCACert       string        `json:"redisCACert"`
	RedisReadTimeout  time.Duration `json:"redisReadTimeout"`
	RedisWriteTimeout time.Duration `json:"redisWriteTimeout"`
	RedisDialTimeout  time.Duration `json:"redisDialTimeout"`
	RedisPoolSize     int           `json:"redisPoolSize"`
	RedisMinIdleConns int           `json:"redisMinIdleConns"`
}

// ConfigurationPort resolves infrastructure configuration for a given tenant.
type ConfigurationPort interface {
	GetTenantConfig(ctx context.Context, tenantID string) (*TenantConfig, error)
}

var (
	// ErrPostgresLeaseUnavailable indicates the lease has no postgres connection.
	ErrPostgresLeaseUnavailable = errors.New("postgres connection lease unavailable")
	// ErrRedisLeaseUnavailable indicates the lease has no redis connection.
	ErrRedisLeaseUnavailable = errors.New("redis connection lease unavailable")
)

func noopLeaseRelease() {}

// PostgresConnectionLease protects the lifetime of a postgres connection pool.
// Callers MUST release the lease when finished using the connection.
type PostgresConnectionLease struct {
	conn        *libPostgres.Client
	release     func()
	releaseOnce sync.Once
}

// NewPostgresConnectionLease creates a postgres lease.
func NewPostgresConnectionLease(conn *libPostgres.Client, release func()) *PostgresConnectionLease {
	if conn == nil {
		return nil
	}

	if release == nil {
		release = noopLeaseRelease
	}

	return &PostgresConnectionLease{conn: conn, release: release}
}

// Connection returns the leased postgres connection.
func (lease *PostgresConnectionLease) Connection() *libPostgres.Client {
	if lease == nil {
		return nil
	}

	return lease.conn
}

// Resolver delegates to the underlying postgres client for compatibility with existing call sites.
func (lease *PostgresConnectionLease) Resolver(ctx context.Context) (dbresolver.DB, error) {
	if lease == nil || lease.conn == nil {
		return nil, ErrPostgresLeaseUnavailable
	}

	resolver, err := lease.conn.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres connection from lease: %w", err)
	}

	return resolver, nil
}

// Release releases the lease exactly once.
func (lease *PostgresConnectionLease) Release() {
	if lease == nil {
		return
	}

	lease.releaseOnce.Do(lease.release)
}

// RedisConnectionLease protects the lifetime of a redis connection pool.
// Callers MUST release the lease when finished using the connection.
type RedisConnectionLease struct {
	conn        *libRedis.Client
	release     func()
	releaseOnce sync.Once
}

// NewRedisConnectionLease creates a redis lease.
func NewRedisConnectionLease(conn *libRedis.Client, release func()) *RedisConnectionLease {
	if conn == nil {
		return nil
	}

	if release == nil {
		release = noopLeaseRelease
	}

	return &RedisConnectionLease{conn: conn, release: release}
}

// Connection returns the leased redis connection.
func (lease *RedisConnectionLease) Connection() *libRedis.Client {
	if lease == nil {
		return nil
	}

	return lease.conn
}

// GetClient delegates to the underlying redis client for compatibility with existing call sites.
func (lease *RedisConnectionLease) GetClient(ctx context.Context) (redis.UniversalClient, error) {
	if lease == nil || lease.conn == nil {
		return nil, ErrRedisLeaseUnavailable
	}

	client, err := lease.conn.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("get redis client from lease: %w", err)
	}

	return client, nil
}

// Release releases the lease exactly once.
func (lease *RedisConnectionLease) Release() {
	if lease == nil {
		return
	}

	lease.releaseOnce.Do(lease.release)
}

// ReplicaDBLease protects the lifetime of a replica database handle.
// Callers MUST release the lease when finished using the database handle.
type ReplicaDBLease struct {
	db          *sql.DB
	release     func()
	releaseOnce sync.Once
}

// NewReplicaDBLease creates a replica DB lease.
func NewReplicaDBLease(db *sql.DB, release func()) *ReplicaDBLease {
	if db == nil {
		return nil
	}

	if release == nil {
		release = noopLeaseRelease
	}

	return &ReplicaDBLease{db: db, release: release}
}

// DB returns the leased replica database.
func (lease *ReplicaDBLease) DB() *sql.DB {
	if lease == nil {
		return nil
	}

	return lease.db
}

// Release releases the lease exactly once.
func (lease *ReplicaDBLease) Release() {
	if lease == nil {
		return
	}

	lease.releaseOnce.Do(lease.release)
}

// TxLease protects the lifetime of a transaction's backing connection pool.
// Callers MUST finish the transaction via Commit or Rollback.
type TxLease struct {
	tx          *sql.Tx
	release     func()
	releaseOnce sync.Once
}

// NewTxLease creates a transaction lease.
func NewTxLease(tx *sql.Tx, release func()) *TxLease {
	if tx == nil {
		return nil
	}

	if release == nil {
		release = noopLeaseRelease
	}

	return &TxLease{tx: tx, release: release}
}

// SQLTx returns the leased transaction.
func (lease *TxLease) SQLTx() *sql.Tx {
	if lease == nil {
		return nil
	}

	return lease.tx
}

func (lease *TxLease) finish(err error) error {
	if lease == nil {
		return nil
	}

	lease.releaseOnce.Do(lease.release)

	return err
}

// Commit commits the transaction and releases the lease.
func (lease *TxLease) Commit() error {
	if lease == nil || lease.tx == nil {
		return lease.finish(nil)
	}

	return lease.finish(lease.tx.Commit())
}

// Rollback rolls back the transaction and releases the lease.
func (lease *TxLease) Rollback() error {
	if lease == nil || lease.tx == nil {
		return lease.finish(nil)
	}

	return lease.finish(lease.tx.Rollback())
}

// InfrastructureProvider resolves connections for the tenant in ctx.
// Implementations MUST preserve current behavior:
// - If ctx has no tenant, default tenant must still work.
type InfrastructureProvider interface {
	GetPostgresConnection(ctx context.Context) (*PostgresConnectionLease, error)
	GetRedisConnection(ctx context.Context) (*RedisConnectionLease, error)
	// BeginTx starts a tenant-scoped database transaction.
	// The caller is responsible for calling Commit() or Rollback() on the returned lease.
	BeginTx(ctx context.Context) (*TxLease, error)
	// GetReplicaDB returns the replica database for read-only queries.
	// Falls back to primary if no replica is configured.
	// This enables CQRS read operations to be routed to a read replica.
	GetReplicaDB(ctx context.Context) (*ReplicaDBLease, error)
}
