// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package exception

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Update updates an existing exception.
func (repo *Repository) Update(
	ctx context.Context,
	exception *entities.Exception,
) (*entities.Exception, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if exception == nil {
		return nil, entities.ErrExceptionNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.update")

	defer span.End()

	updated, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.Exception, error) {
			return repo.executeUpdate(ctx, tx, exception)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to update exception: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update exception", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update exception", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return updated, nil
}

// UpdateWithTx updates an existing exception using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	exception *entities.Exception,
) (*entities.Exception, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if exception == nil {
		return nil, entities.ErrExceptionNil
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.update_with_tx")

	defer span.End()

	updated, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.Exception, error) {
			return repo.executeUpdate(ctx, innerTx, exception)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to update exception: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update exception", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update exception", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return updated, nil
}

// executeUpdate performs the actual update operation within a transaction.
func (repo *Repository) executeUpdate(
	ctx context.Context,
	tx *sql.Tx,
	exception *entities.Exception,
) (*entities.Exception, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE exceptions SET
			severity = $2,
			status = $3,
			external_system = $4,
			external_issue_id = $5,
			assigned_to = $6,
			due_at = $7,
			resolution_notes = $8,
			resolution_type = $9,
			resolution_reason = $10,
			reason = $11,
			version = version + 1,
			updated_at = $12
		WHERE id = $1 AND version = $13
	`,
		exception.ID.String(),
		exception.Severity.String(),
		exception.Status.String(),
		pgcommon.StringPtrToNullString(exception.ExternalSystem),
		pgcommon.StringPtrToNullString(exception.ExternalIssueID),
		pgcommon.StringPtrToNullString(exception.AssignedTo),
		pgcommon.TimePtrToNullTime(exception.DueAt),
		pgcommon.StringPtrToNullString(exception.ResolutionNotes),
		pgcommon.StringPtrToNullString(exception.ResolutionType),
		pgcommon.StringPtrToNullString(exception.ResolutionReason),
		pgcommon.StringPtrToNullString(exception.Reason),
		time.Now().UTC(),
		exception.Version,
	)
	if err != nil {
		return nil, fmt.Errorf("update exception: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)", exception.ID.String()).Scan(&exists); err != nil {
			return nil, fmt.Errorf("check exception existence: %w", err)
		}

		if exists {
			return nil, ErrConcurrentModification
		}

		return nil, entities.ErrExceptionNotFound
	}

	row := tx.QueryRowContext(ctx, `
		SELECT id, transaction_id, severity, status, external_system, external_issue_id,
		       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
		       reason, version, created_at, updated_at
		FROM exceptions
		WHERE id = $1
	`, exception.ID.String())

	return scanException(row)
}
