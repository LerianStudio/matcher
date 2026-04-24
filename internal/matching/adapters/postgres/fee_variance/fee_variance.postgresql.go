// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package fee_variance provides PostgreSQL persistence for fee variance entities.
package fee_variance

import (
	"context"
	"database/sql"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository persists fee variances in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new fee variance repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// CreateBatchWithTx inserts fee variances using the provided transaction.
func (repo *Repository) CreateBatchWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	rows []*matchingEntities.FeeVariance,
) ([]*matchingEntities.FeeVariance, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrInvalidTx
	}

	return repo.createBatch(ctx, tx, rows)
}

func (repo *Repository) createBatch(
	ctx context.Context,
	tx *sql.Tx,
	rows []*matchingEntities.FeeVariance,
) ([]*matchingEntities.FeeVariance, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if len(rows) == 0 {
		return nil, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_fee_variance_batch")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) ([]*matchingEntities.FeeVariance, error) {
			stmt, err := execTx.PrepareContext(
				ctx,
				`INSERT INTO match_fee_variances (id, context_id, run_id, match_group_id, transaction_id, fee_schedule_id, fee_schedule_name_snapshot, currency, expected_fee_amount, actual_fee_amount, delta, tolerance_abs, tolerance_percent, variance_type, created_at, updated_at)
				 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
			)
			if err != nil {
				return nil, fmt.Errorf("prepare insert fee variance: %w", err)
			}

			defer func() { _ = stmt.Close() }()

			for _, row := range rows {
				if row == nil {
					continue
				}

				model, err := NewPostgreSQLModel(row)
				if err != nil {
					return nil, err
				}

				if _, err := stmt.ExecContext(ctx,
					model.ID,
					model.ContextID,
					model.RunID,
					model.MatchGroupID,
					model.TransactionID,
					model.FeeScheduleID,
					model.FeeScheduleNameSnapshot,
					model.Currency,
					model.ExpectedFee,
					model.ActualFee,
					model.Delta,
					model.ToleranceAbs,
					model.TolerancePct,
					model.VarianceType,
					model.CreatedAt,
					model.UpdatedAt,
				); err != nil {
					return nil, fmt.Errorf("insert fee variance: %w", err)
				}
			}

			return rows, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create fee variance batch transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create fee variance batch", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create fee variance batch")

		return nil, wrappedErr
	}

	return result, nil
}

var _ matchingRepos.FeeVarianceRepository = (*Repository)(nil)
