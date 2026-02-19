// Package common provides shared utilities for postgres adapters.
package common

import (
	"context"
	"database/sql"
	"fmt"

	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"

	sharedCommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// WithTenantTx executes fn within a new tenant-scoped transaction.
func WithTenantTx[Result any](
	ctx context.Context,
	conn *libPostgres.Client,
	fn func(*sql.Tx) (Result, error),
) (Result, error) {
	if fn == nil {
		var zero Result
		return zero, sharedCommon.ErrNilCallback
	}

	result, err := sharedCommon.WithTenantTx(ctx, conn, func(tx *sql.Tx) (Result, error) {
		return fn(tx)
	})
	if err != nil {
		return result, fmt.Errorf("with tenant tx: %w", err)
	}

	return result, nil
}

// WithTenantTxOrExisting executes fn within an existing or new tenant-scoped transaction.
func WithTenantTxOrExisting[Result any](
	ctx context.Context,
	conn *libPostgres.Client,
	tx *sql.Tx,
	fn func(*sql.Tx) (Result, error),
) (Result, error) {
	if fn == nil {
		var zero Result
		return zero, sharedCommon.ErrNilCallback
	}

	result, err := sharedCommon.WithTenantTxOrExisting(
		ctx,
		conn,
		tx,
		func(execTx *sql.Tx) (Result, error) {
			return fn(execTx)
		},
	)
	if err != nil {
		return result, fmt.Errorf("with tenant tx or existing: %w", err)
	}

	return result, nil
}

// WithTenantTxProvider executes fn within a new tenant-scoped transaction using an InfrastructureProvider.
func WithTenantTxProvider[Result any](
	ctx context.Context,
	provider ports.InfrastructureProvider,
	fn func(*sql.Tx) (Result, error),
) (Result, error) {
	if fn == nil {
		var zero Result
		return zero, sharedCommon.ErrNilCallback
	}

	result, err := sharedCommon.WithTenantTxProvider(ctx, provider, func(tx *sql.Tx) (Result, error) {
		return fn(tx)
	})
	if err != nil {
		return result, fmt.Errorf("with tenant tx provider: %w", err)
	}

	return result, nil
}

// WithTenantTxOrExistingProvider executes fn within an existing or new tenant-scoped transaction using an InfrastructureProvider.
func WithTenantTxOrExistingProvider[Result any](
	ctx context.Context,
	provider ports.InfrastructureProvider,
	tx *sql.Tx,
	fn func(*sql.Tx) (Result, error),
) (Result, error) {
	if fn == nil {
		var zero Result
		return zero, sharedCommon.ErrNilCallback
	}

	result, err := sharedCommon.WithTenantTxOrExistingProvider(
		ctx,
		provider,
		tx,
		func(execTx *sql.Tx) (Result, error) {
			return fn(execTx)
		},
	)
	if err != nil {
		return result, fmt.Errorf("with tenant tx or existing provider: %w", err)
	}

	return result, nil
}

// QueryExecutor is an interface for executing read queries.
// Re-exported from shared common for convenience.
type QueryExecutor = sharedCommon.QueryExecutor

// WithTenantReadQuery executes fn using a read replica connection with tenant schema isolation.
// Falls back to primary if no replica is configured.
func WithTenantReadQuery[Result any](
	ctx context.Context,
	provider ports.InfrastructureProvider,
	fn func(QueryExecutor) (Result, error),
) (Result, error) {
	if fn == nil {
		var zero Result
		return zero, sharedCommon.ErrNilCallback
	}

	result, err := sharedCommon.WithTenantReadQuery(ctx, provider, fn)
	if err != nil {
		return result, fmt.Errorf("with tenant read query: %w", err)
	}

	return result, nil
}
