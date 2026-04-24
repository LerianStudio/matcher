// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee_schedule

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Delete removes a fee schedule by ID. CASCADE handles items.
func (repo *Repository) Delete(ctx context.Context, id uuid.UUID) error {
	return repo.deleteInternal(ctx, nil, id)
}

// DeleteWithTx removes a fee schedule by ID using the provided transaction.
func (repo *Repository) DeleteWithTx(ctx context.Context, tx matchingRepos.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrInvalidTx
	}

	return repo.deleteInternal(ctx, tx, id)
}

// deleteInternal is the shared implementation for Delete and DeleteWithTx.
func (repo *Repository) deleteInternal(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.delete_fee_schedule")

	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(execTx *sql.Tx) (bool, error) {
		result, execErr := execTx.ExecContext(ctx,
			"DELETE FROM fee_schedules WHERE id = $1",
			id.String(),
		)
		if execErr != nil {
			return false, fmt.Errorf("delete fee schedule: %w", execErr)
		}

		rowsAffected, execErr := result.RowsAffected()
		if execErr != nil {
			return false, fmt.Errorf("get rows affected: %w", execErr)
		}

		if rowsAffected == 0 {
			return false, sql.ErrNoRows
		}

		return true, nil
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fee.ErrFeeScheduleNotFound
		}

		wrappedErr := fmt.Errorf("delete fee schedule: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete fee schedule", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete fee schedule")

		return wrappedErr
	}

	return nil
}
