// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dispute

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Create inserts a new dispute.
func (repo *Repository) Create(
	ctx context.Context,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.create")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeCreate(ctx, tx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to create dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new dispute using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.create_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeCreate(ctx, innerTx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to create dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual create operation within a transaction.
func (repo *Repository) executeCreate(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	evidenceJSON, err := json.Marshal(disputeEntity.Evidence)
	if err != nil {
		return nil, fmt.Errorf("marshal evidence: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO disputes (
			id, exception_id, category, state, description,
			opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`,
		disputeEntity.ID.String(),
		disputeEntity.ExceptionID.String(),
		disputeEntity.Category.String(),
		disputeEntity.State.String(),
		disputeEntity.Description,
		disputeEntity.OpenedBy,
		pgcommon.StringPtrToNullString(disputeEntity.Resolution),
		pgcommon.StringPtrToNullString(disputeEntity.ReopenReason),
		evidenceJSON,
		disputeEntity.CreatedAt,
		disputeEntity.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert dispute: %w", err)
	}

	return repo.findByIDExec(ctx, tx, disputeEntity.ID)
}

// Update updates an existing dispute.
func (repo *Repository) Update(
	ctx context.Context,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.update")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeUpdate(ctx, tx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// UpdateWithTx updates an existing dispute using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.update_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeUpdate(ctx, innerTx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// executeUpdate performs the actual update operation within a transaction.
func (repo *Repository) executeUpdate(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	evidenceJSON, err := json.Marshal(disputeEntity.Evidence)
	if err != nil {
		return nil, fmt.Errorf("marshal evidence: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE disputes SET
			category = $2,
			state = $3,
			description = $4,
			opened_by = $5,
			resolution = $6,
			reopen_reason = $7,
			evidence = $8,
			updated_at = $9
		WHERE id = $1
	`,
		disputeEntity.ID.String(),
		disputeEntity.Category.String(),
		disputeEntity.State.String(),
		disputeEntity.Description,
		disputeEntity.OpenedBy,
		pgcommon.StringPtrToNullString(disputeEntity.Resolution),
		pgcommon.StringPtrToNullString(disputeEntity.ReopenReason),
		evidenceJSON,
		disputeEntity.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update dispute: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return nil, ErrDisputeNotFound
	}

	return repo.findByIDExec(ctx, tx, disputeEntity.ID)
}
