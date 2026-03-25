// Package common provides shared utilities for postgres adapters.
package common

import (
	"context"
	"database/sql"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// QueryExecutor is an interface for executing read queries.
// Both *sql.Tx and *sql.Conn implement this interface.
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// WithTenantRead executes fn using a read replica connection with tenant schema isolation.
// Falls back to primary if no replica is configured.
// The fn callback receives a *sql.Conn that has the tenant schema applied.
// The connection is automatically returned to the pool after fn completes.
// Tenant schema is applied using SET (session-level) and explicitly reset to 'public'
// before the connection is returned to the pool to prevent schema leakage.
func WithTenantRead[T any](
	ctx context.Context,
	provider ports.InfrastructureProvider,
	fn func(*sql.Conn) (T, error),
) (T, error) {
	var zero T

	if provider == nil {
		return zero, ErrConnectionRequired
	}

	// Apply a default timeout when the context has no deadline.
	// This prevents indefinite hangs when acquiring a read connection
	// from an exhausted pool.
	readCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc

		readCtx, cancel = context.WithTimeout(ctx, defaultTxTimeout)

		defer cancel()
	}

	dbLease, err := getReadDB(readCtx, provider)
	if err != nil {
		return zero, err
	}
	defer dbLease.Release()

	db := dbLease.DB()

	conn, err := db.Conn(readCtx)
	if err != nil {
		return zero, fmt.Errorf("acquire connection: %w", err)
	}

	defer func() {
		// Use the original ctx (not readCtx) for resetSearchPath because readCtx
		// may already be cancelled when the defer runs, which would cause the
		// SET search_path to fail and leave the connection with a tenant schema.
		resetSearchPath(ctx, conn)
		_ = conn.Close()
	}()

	if err := applyTenantSchemaToConn(readCtx, conn); err != nil {
		return zero, fmt.Errorf("apply tenant schema: %w", err)
	}

	return fn(conn)
}

// getReadDB returns the replica DB if available, otherwise falls back to primary.
func getReadDB(ctx context.Context, provider ports.InfrastructureProvider) (*ports.DBLease, error) {
	dbLease, err := provider.GetReplicaDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("get replica db: %w", err)
	}

	if dbLease != nil && dbLease.DB() != nil {
		return dbLease, nil
	}

	if dbLease != nil {
		dbLease.Release()
	}

	return getPrimaryDBFallback(ctx, provider)
}

// getPrimaryDBFallback returns the primary database when replica is not available.
func getPrimaryDBFallback(
	ctx context.Context,
	provider ports.InfrastructureProvider,
) (*ports.DBLease, error) {
	primaryLease, err := provider.GetPrimaryDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("get primary connection as fallback: %w", err)
	}

	if primaryLease == nil {
		return nil, ErrConnectionRequired
	}

	if primaryLease.DB() == nil {
		if primaryLease != nil {
			primaryLease.Release()
		}

		return nil, ErrNoPrimaryDB
	}

	return primaryLease, nil
}

// WithTenantReadQuery executes fn using a read replica connection with tenant schema isolation.
// This variant accepts QueryExecutor interface, making it easier to migrate code that
// previously used *sql.Tx since both *sql.Tx and *sql.Conn satisfy QueryExecutor.
// Falls back to primary if no replica is configured.
func WithTenantReadQuery[T any](
	ctx context.Context,
	provider ports.InfrastructureProvider,
	fn func(QueryExecutor) (T, error),
) (T, error) {
	return WithTenantRead(ctx, provider, func(conn *sql.Conn) (T, error) {
		return fn(conn)
	})
}

// applyTenantSchemaToConn sets the search_path for the tenant on a connection.
// Uses session-level SET for the connection. The caller is responsible for
// resetting the search_path before returning the connection to the pool.
// Validates that tenantID is a valid UUID before using it in SQL to match
// the validation performed by auth.ApplyTenantSchema on the write path.
func applyTenantSchemaToConn(ctx context.Context, conn *sql.Conn) error {
	tenantID := auth.GetTenantID(ctx)
	defaultTenantID := auth.GetDefaultTenantID()

	if tenantID == "" || tenantID == defaultTenantID {
		return nil
	}

	if !libCommons.IsUUID(tenantID) {
		return fmt.Errorf("%w: %s", ErrInvalidTenantID, tenantID)
	}

	quotedID := auth.QuoteIdentifier(tenantID)
	if _, err := conn.ExecContext(ctx, "SET search_path TO "+quotedID); err != nil {
		return fmt.Errorf("set search_path: %w", err)
	}

	return nil
}

// resetSearchPath resets the search_path to 'public' before returning the connection to the pool.
// This prevents tenant schema leakage between pooled connection reuses.
// If reset fails, the connection is closed to prevent cross-tenant data leakage
// and a warning is logged with tenant and error details.
func resetSearchPath(ctx context.Context, conn *sql.Conn) {
	if _, err := conn.ExecContext(ctx, "SET search_path TO public"); err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)

		tenantID := auth.GetTenantID(ctx)

		logger.With(
			libLog.String("tenant_id", tenantID),
			libLog.Err(err),
		).Log(ctx, libLog.LevelWarn, "failed to reset search_path, closing connection to prevent tenant leakage")

		_ = conn.Close()
	}
}
