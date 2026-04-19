// Package common provides shared utilities for postgres adapters.
package common

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// defaultTxTimeout is the fallback timeout applied to database transactions
// when the incoming context has no deadline. This prevents indefinite hangs
// caused by sql.DB connection pool exhaustion.
const defaultTxTimeout = 30 * time.Second

var (
	// ErrConnectionRequired indicates postgres connection is nil.
	ErrConnectionRequired = errors.New("postgres connection is required")
	// ErrNoPrimaryDB indicates no primary database is configured.
	ErrNoPrimaryDB = errors.New("no primary database configured for tenant transaction")
	// ErrNilTxLease indicates the infrastructure provider returned a nil or unusable tx lease.
	ErrNilTxLease = errors.New("tenant transaction lease is required")
	// ErrNilCallback indicates a nil callback function was passed to a transaction wrapper.
	ErrNilCallback = errors.New("pgcommon: callback function must not be nil")
	// ErrInvalidTenantID indicates the tenant ID is not a valid UUID.
	ErrInvalidTenantID = errors.New("invalid tenant ID format")
)

// WithTenantTx executes fn within a new tenant-scoped transaction.
// The fn callback must not be nil.
func WithTenantTx[T any](
	ctx context.Context,
	conn *libPostgres.Client,
	fn func(*sql.Tx) (T, error),
) (T, error) {
	return WithTenantTxOrExisting(ctx, conn, nil, fn)
}

// WithTenantTxOrExisting executes fn within an existing or new tenant-scoped transaction.
// The fn callback must not be nil.
//
// When tx is non-nil, the caller retains ownership of commit/rollback. This function
// only applies SET LOCAL search_path to isolate the tenant schema, then invokes fn.
// The caller must ensure the transaction is eventually committed or rolled back.
//
// When tx is nil, a new transaction is created, fn is executed, and the transaction
// is committed on success or rolled back on error. The caller does not manage the
// transaction lifecycle in this case.
func WithTenantTxOrExisting[Result any](
	ctx context.Context,
	conn *libPostgres.Client,
	tx *sql.Tx,
	fn func(*sql.Tx) (Result, error),
) (Result, error) {
	var zero Result

	if fn == nil {
		return zero, ErrNilCallback
	}

	if conn == nil {
		return zero, ErrConnectionRequired
	}

	if tx != nil {
		if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
			return zero, fmt.Errorf("failed to apply tenant schema: %w", err)
		}

		return fn(tx)
	}

	db, err := conn.Resolver(ctx)
	if err != nil {
		return zero, fmt.Errorf("failed to get database connection: %w", err)
	}

	primaryDBs := db.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return zero, ErrNoPrimaryDB
	}

	// Apply a default timeout when the context has no deadline.
	// This prevents indefinite hangs when the sql.DB connection pool
	// is exhausted and no caller-supplied deadline is present.
	txCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc

		txCtx, cancel = context.WithTimeout(ctx, defaultTxTimeout)

		defer cancel()
	}

	newTx, err := primaryDBs[0].BeginTx(txCtx, nil)
	if err != nil {
		return zero, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		_ = newTx.Rollback()
	}()

	if err := auth.ApplyTenantSchema(txCtx, newTx); err != nil {
		return zero, fmt.Errorf("failed to apply tenant schema: %w", err)
	}

	result, err := fn(newTx)
	if err != nil {
		return zero, err
	}

	if err := newTx.Commit(); err != nil {
		return zero, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}

// WithTenantTxProvider executes fn within a new tenant-scoped transaction using an InfrastructureProvider.
// The fn callback must not be nil.
func WithTenantTxProvider[T any](
	ctx context.Context,
	provider ports.InfrastructureProvider,
	fn func(*sql.Tx) (T, error),
) (T, error) {
	return WithTenantTxOrExistingProvider(ctx, provider, nil, fn)
}

// WithTenantTxOrExistingProvider executes fn within an existing or new tenant-scoped transaction using an InfrastructureProvider.
// The fn callback must not be nil.
func WithTenantTxOrExistingProvider[Result any](
	ctx context.Context,
	provider ports.InfrastructureProvider,
	tx *sql.Tx,
	fn func(*sql.Tx) (Result, error),
) (Result, error) {
	var zero Result

	if provider == nil {
		return zero, ErrConnectionRequired
	}

	if fn == nil {
		return zero, ErrNilCallback
	}

	if ports.IsNilValue(provider) {
		return zero, ErrConnectionRequired
	}

	if tx != nil {
		if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
			return zero, fmt.Errorf("failed to apply tenant schema: %w", err)
		}

		return fn(tx)
	}

	txCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc

		txCtx, cancel = context.WithTimeout(ctx, defaultTxTimeout)
		defer cancel()
	}

	txLease, err := provider.BeginTx(txCtx)
	if err != nil {
		return zero, fmt.Errorf("failed to begin transaction: %w", err)
	}

	if txLease == nil || txLease.SQLTx() == nil {
		return zero, ErrNilTxLease
	}

	defer func() {
		_ = txLease.Rollback()
	}()

	result, err := fn(txLease.SQLTx())
	if err != nil {
		return zero, err
	}

	if err := txLease.Commit(); err != nil {
		return zero, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}
