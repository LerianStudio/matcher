package adapters

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	// ErrPostgresConnectionNotConfigured is returned when postgres connection was not provided at construction time.
	ErrPostgresConnectionNotConfigured = errors.New("postgres connection not configured")
	// ErrRedisConnectionNotConfigured is returned when redis connection was not provided at construction time.
	ErrRedisConnectionNotConfigured = errors.New("redis connection not configured")
	// ErrNoPrimaryDatabaseForTransaction is returned when no primary database is configured for starting a transaction.
	ErrNoPrimaryDatabaseForTransaction = errors.New(
		"no primary database configured for single-tenant transaction",
	)
	// ErrNoDatabaseForRead is returned when no database is available for read operations.
	ErrNoDatabaseForRead = errors.New("no database configured for read operations")
)

// Compile-time check that SingleTenantInfrastructureProvider implements InfrastructureProvider.
var _ ports.InfrastructureProvider = (*SingleTenantInfrastructureProvider)(nil)

// SingleTenantInfrastructureProvider wraps singleton connections for single-tenant mode.
// This is the default provider that maintains current behavior with no changes.
type SingleTenantInfrastructureProvider struct {
	postgres *libPostgres.Client
	redis    *libRedis.Client
}

// NewSingleTenantInfrastructureProvider creates a provider wrapping existing singleton connections.
func NewSingleTenantInfrastructureProvider(
	postgres *libPostgres.Client,
	redis *libRedis.Client,
) *SingleTenantInfrastructureProvider {
	return &SingleTenantInfrastructureProvider{
		postgres: postgres,
		redis:    redis,
	}
}

// GetPostgresConnection returns the singleton postgres connection.
// Returns ErrPostgresConnectionNotConfigured if no connection was provided at construction time.
func (provider *SingleTenantInfrastructureProvider) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	if provider.postgres == nil {
		return nil, ErrPostgresConnectionNotConfigured
	}

	return provider.postgres, nil
}

// GetRedisConnection returns the singleton redis connection.
// Returns ErrRedisConnectionNotConfigured if no connection was provided at construction time.
func (provider *SingleTenantInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	if provider.redis == nil {
		return nil, ErrRedisConnectionNotConfigured
	}

	return provider.redis, nil
}

// BeginTx starts a tenant-scoped database transaction.
// The caller is responsible for calling Commit() or Rollback() on the returned transaction.
func (provider *SingleTenantInfrastructureProvider) BeginTx(ctx context.Context) (*sql.Tx, error) {
	if provider.postgres == nil {
		return nil, ErrPostgresConnectionNotConfigured
	}

	resolver, err := provider.postgres.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get database connection: %w",
			errors.Join(ErrNoPrimaryDatabaseForTransaction, err),
		)
	}

	// resolver.PrimaryDBs() is expected to return a single entry in single-tenant mode;
	// use primaryDBs[0] as the transaction target, and the error path covers len(primaryDBs) == 0.
	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 {
		return nil, ErrNoPrimaryDatabaseForTransaction
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
func (provider *SingleTenantInfrastructureProvider) GetReplicaDB(ctx context.Context) (*sql.DB, error) {
	if provider.postgres == nil {
		return nil, ErrPostgresConnectionNotConfigured
	}

	resolver, err := provider.postgres.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get database connection: %w",
			errors.Join(ErrNoDatabaseForRead, err),
		)
	}

	replicaDBs := resolver.ReplicaDBs()
	if len(replicaDBs) > 0 {
		return replicaDBs[0], nil
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 {
		return nil, ErrNoDatabaseForRead
	}

	return primaryDBs[0], nil
}
