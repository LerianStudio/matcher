package command

import (
	"context"
	"database/sql"
	"fmt"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func beginTenantTx(ctx context.Context, provider sharedPorts.InfrastructureProvider) (*sql.Tx, context.CancelFunc, error) {
	noopCancel := func() {}

	if provider == nil {
		return nil, noopCancel, ErrInlineCreateRequiresInfrastructure
	}

	txLease, err := provider.BeginTx(ctx)
	if err != nil {
		return nil, noopCancel, fmt.Errorf("begin tenant transaction: %w", err)
	}

	if txLease == nil || txLease.SQLTx() == nil {
		return nil, noopCancel, ErrInlineCreateRequiresInfrastructure
	}

	return txLease.SQLTx(), func() {
		_ = txLease.Rollback()
	}, nil
}
