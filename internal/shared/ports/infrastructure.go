// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"database/sql"
	"time"

	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"
	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"
)

// TenantConfig holds infrastructure configuration for a specific tenant.
type TenantConfig struct {
	PostgresPrimaryDSN string
	PostgresReplicaDSN string
	PostgresPrimaryDB  string
	PostgresReplicaDB  string

	RedisAddresses    []string
	RedisPassword     string
	RedisDB           int
	RedisMasterName   string
	RedisProtocol     int
	RedisUseTLS       bool
	RedisCACert       string
	RedisReadTimeout  time.Duration
	RedisWriteTimeout time.Duration
	RedisDialTimeout  time.Duration
	RedisPoolSize     int
	RedisMinIdleConns int
}

// ConfigurationPort resolves infrastructure configuration for a given tenant.
type ConfigurationPort interface {
	GetTenantConfig(ctx context.Context, tenantID string) (*TenantConfig, error)
}

// InfrastructureProvider resolves connections for the tenant in ctx.
// Implementations MUST preserve current behavior:
// - If ctx has no tenant, default tenant must still work.
type InfrastructureProvider interface {
	GetPostgresConnection(ctx context.Context) (*libPostgres.Client, error)
	GetRedisConnection(ctx context.Context) (*libRedis.Client, error)
	// BeginTx starts a tenant-scoped database transaction.
	// The caller is responsible for calling Commit() or Rollback() on the returned transaction.
	BeginTx(ctx context.Context) (*sql.Tx, error)
	// GetReplicaDB returns the replica database for read-only queries.
	// Falls back to primary if no replica is configured.
	// This enables CQRS read operations to be routed to a read replica.
	GetReplicaDB(ctx context.Context) (*sql.DB, error)
}
