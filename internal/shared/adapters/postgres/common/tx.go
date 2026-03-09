// Package common provides shared utilities for postgres adapters.
package common

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"

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
	if len(primaryDBs) == 0 {
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

	if isNilInterface(provider) {
		return zero, ErrConnectionRequired
	}

	conn, err := provider.GetPostgresConnection(ctx)
	if err != nil {
		return zero, fmt.Errorf("failed to get postgres connection: %w", err)
	}

	if conn == nil {
		return zero, ErrConnectionRequired
	}

	return WithTenantTxOrExisting(ctx, conn, tx, fn)
}

// BeginTenantTx begins a tenant-scoped transaction that the caller must manage.
// The caller is responsible for calling Commit() or Rollback() on the returned transaction,
// and must call the returned CancelFunc after the transaction is committed or rolled back
// to release the timeout context resources.
//
// If the incoming context already has a deadline, the returned CancelFunc is a no-op.
// If no deadline is present, a default timeout of 30 seconds is applied to prevent
// indefinite hangs from connection pool exhaustion.
func BeginTenantTx(ctx context.Context, provider ports.InfrastructureProvider) (*sql.Tx, context.CancelFunc, error) {
	noop := func() {} // returned when no timeout context was created

	if provider == nil {
		return nil, noop, ErrConnectionRequired
	}

	if isNilInterface(provider) {
		return nil, noop, ErrConnectionRequired
	}

	conn, err := provider.GetPostgresConnection(ctx)
	if err != nil {
		return nil, noop, fmt.Errorf("failed to get postgres connection: %w", err)
	}

	if conn == nil {
		return nil, noop, ErrConnectionRequired
	}

	db, err := conn.Resolver(ctx)
	if err != nil {
		return nil, noop, fmt.Errorf("failed to get database connection: %w", err)
	}

	primaryDBs := db.PrimaryDBs()
	if len(primaryDBs) == 0 {
		return nil, noop, ErrNoPrimaryDB
	}

	// Apply a default timeout when the context has no deadline.
	// The cancel function is returned to the caller instead of deferred,
	// because the transaction outlives this function call. The caller must
	// invoke cancel after commit/rollback to release context resources.
	txCtx := ctx
	cancel := noop

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		txCtx, cancel = context.WithTimeout(ctx, defaultTxTimeout)
	}

	tx, err := primaryDBs[0].BeginTx(txCtx, nil)
	if err != nil {
		cancel()

		return nil, noop, fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := auth.ApplyTenantSchema(txCtx, tx); err != nil {
		_ = tx.Rollback()

		cancel()

		return nil, noop, fmt.Errorf("failed to apply tenant schema: %w", err)
	}

	return tx, cancel, nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}

	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
