package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const defaultBeginTxTimeout = 30 * time.Second

var errNoPrimaryDatabase = errors.New("no primary database configured for tenant transaction")

func beginTenantTx(ctx context.Context, provider sharedPorts.InfrastructureProvider) (*sql.Tx, context.CancelFunc, error) {
	noopCancel := func() {}

	if provider == nil {
		return nil, noopCancel, ErrInlineCreateRequiresInfrastructure
	}

	txCtx := ctx
	cancel := noopCancel

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		txCtx, cancel = context.WithTimeout(ctx, defaultBeginTxTimeout)
	}

	conn, err := provider.GetPostgresConnection(txCtx)
	if err != nil {
		cancel()
		return nil, noopCancel, fmt.Errorf("get postgres connection: %w", err)
	}

	if conn == nil {
		cancel()
		return nil, noopCancel, ErrInlineCreateRequiresInfrastructure
	}

	db, err := conn.Resolver(txCtx)
	if err != nil {
		cancel()
		return nil, noopCancel, fmt.Errorf("resolve database connection: %w", err)
	}

	primaryDBs := db.PrimaryDBs()
	if len(primaryDBs) == 0 {
		cancel()
		return nil, noopCancel, errNoPrimaryDatabase
	}

	tx, err := primaryDBs[0].BeginTx(txCtx, nil)
	if err != nil {
		cancel()
		return nil, noopCancel, fmt.Errorf("begin tenant transaction: %w", err)
	}

	if err := auth.ApplyTenantSchema(txCtx, tx); err != nil {
		_ = tx.Rollback()

		cancel()

		return nil, noopCancel, fmt.Errorf("apply tenant schema: %w", err)
	}

	return tx, cancel, nil
}
