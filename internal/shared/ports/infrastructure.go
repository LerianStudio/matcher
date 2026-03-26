// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
)

// ErrRedisLeaseUnavailable indicates the lease has no redis connection.
var ErrRedisLeaseUnavailable = errors.New("redis connection lease unavailable")

func noopLeaseRelease() {}

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

// DBLease protects the lifetime of a database handle.
// Callers MUST release the lease when finished using the database handle.
type DBLease struct {
	db          *sql.DB
	release     func()
	releaseOnce sync.Once
}

// NewDBLease creates a database lease.
func NewDBLease(db *sql.DB, release func()) *DBLease {
	if db == nil {
		return nil
	}

	if release == nil {
		release = noopLeaseRelease
	}

	return &DBLease{db: db, release: release}
}

// DB returns the leased database handle.
func (lease *DBLease) DB() *sql.DB {
	if lease == nil {
		return nil
	}

	return lease.db
}

// Release releases the lease exactly once.
func (lease *DBLease) Release() {
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

// InfrastructureProvider resolves tenant-scoped infrastructure from ctx.
// Multi-tenant implementations may fail closed when tenant context is genuinely
// absent; single-tenant/default-tenant behavior is implementation-specific.
type InfrastructureProvider interface {
	GetRedisConnection(ctx context.Context) (*RedisConnectionLease, error)
	// BeginTx starts a tenant-scoped database transaction.
	// The caller is responsible for calling Commit() or Rollback() on the returned lease.
	BeginTx(ctx context.Context) (*TxLease, error)
	// GetReplicaDB returns the replica database for read-only queries.
	// Returns nil when no replica is configured.
	// Callers that want fallback behavior should use GetPrimaryDB explicitly.
	GetReplicaDB(ctx context.Context) (*DBLease, error)
	// GetPrimaryDB returns the primary database handle.
	// This avoids resolver/client indirection for callers that only need *sql.DB.
	GetPrimaryDB(ctx context.Context) (*DBLease, error)
}
