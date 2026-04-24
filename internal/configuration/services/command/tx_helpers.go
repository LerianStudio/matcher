// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"

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

// lockSourceContextForShare acquires a PostgreSQL FOR SHARE lock on the source
// reconciliation context row within the given transaction. This prevents
// concurrent modifications to the source context (and cascading child changes)
// while the clone operation reads and copies the data, ensuring a consistent
// snapshot across all reads within the transaction.
func lockSourceContextForShare(ctx context.Context, tx *sql.Tx, sourceContextID uuid.UUID) error {
	const query = `SELECT 1 FROM reconciliation_contexts WHERE id = $1 FOR SHARE`

	if _, err := tx.ExecContext(ctx, query, sourceContextID); err != nil {
		return fmt.Errorf("lock source context for share: %w", err)
	}

	return nil
}
