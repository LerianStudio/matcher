// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package transaction

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// MarkMatched marks transactions as matched by their IDs.
func (repo *Repository) MarkMatched(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_matched")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeMarkMatched(ctx, tx, contextID, transactionIDs)
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions matched", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to mark transactions matched")

		return fmt.Errorf("failed to mark transactions matched: %w", err)
	}

	return nil
}

// MarkMatchedWithTx marks transactions as matched within an existing transaction.
func (repo *Repository) MarkMatchedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if tx == nil {
		return errTxRequired
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	return repo.executeMarkMatched(ctx, tx, contextID, transactionIDs)
}

// executeMarkMatched performs the actual mark matched operation within a database transaction.
func (repo *Repository) executeMarkMatched(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	queryBuilder, err := BuildMarkMatchedQuery(contextID, transactionIDs)
	if err != nil {
		return fmt.Errorf("build mark matched query: %w", err)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build SQL: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

// MarkPendingReview marks transactions as pending review by their IDs.
func (repo *Repository) MarkPendingReview(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_pending_review")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeMarkPendingReview(ctx, tx, contextID, transactionIDs)
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions pending review", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to mark transactions pending review")

		return fmt.Errorf("failed to mark transactions pending review: %w", err)
	}

	return nil
}

// MarkPendingReviewWithTx marks transactions as pending review within an existing transaction.
func (repo *Repository) MarkPendingReviewWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if tx == nil {
		return errTxRequired
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	return repo.executeMarkPendingReview(ctx, tx, contextID, transactionIDs)
}

// executeMarkPendingReview performs the actual mark pending review operation within a database transaction.
func (repo *Repository) executeMarkPendingReview(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	queryBuilder, err := BuildMarkPendingReviewQuery(contextID, transactionIDs)
	if err != nil {
		return fmt.Errorf("build mark pending review query: %w", err)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build SQL: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

// MarkUnmatched marks transactions as unmatched by their IDs.
func (repo *Repository) MarkUnmatched(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_unmatched")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeMarkUnmatched(ctx, tx, contextID, transactionIDs)
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions unmatched", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to mark transactions unmatched")

		return fmt.Errorf("failed to mark transactions unmatched: %w", err)
	}

	return nil
}

// MarkUnmatchedWithTx marks transactions as unmatched within an existing transaction.
func (repo *Repository) MarkUnmatchedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if tx == nil {
		return errTxRequired
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	return repo.executeMarkUnmatched(ctx, tx, contextID, transactionIDs)
}

// executeMarkUnmatched performs the actual mark unmatched operation within a database transaction.
func (repo *Repository) executeMarkUnmatched(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	queryBuilder, err := BuildMarkUnmatchedQuery(contextID, transactionIDs)
	if err != nil {
		return fmt.Errorf("build mark unmatched query: %w", err)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build SQL: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

// BuildMarkMatchedQuery delegates to shared infrastructure.
func BuildMarkMatchedQuery(contextID uuid.UUID, transactionIDs []uuid.UUID) (squirrel.UpdateBuilder, error) {
	builder, err := sharedpg.BuildMarkMatchedQuery(contextID, transactionIDs)
	if err != nil {
		return builder, fmt.Errorf("build mark matched query: %w", err)
	}

	return builder, nil
}

// BuildMarkPendingReviewQuery delegates to shared infrastructure.
func BuildMarkPendingReviewQuery(
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) (squirrel.UpdateBuilder, error) {
	builder, err := sharedpg.BuildMarkPendingReviewQuery(contextID, transactionIDs)
	if err != nil {
		return builder, fmt.Errorf("build mark pending review query: %w", err)
	}

	return builder, nil
}

// BuildMarkUnmatchedQuery delegates to shared infrastructure.
func BuildMarkUnmatchedQuery(
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) (squirrel.UpdateBuilder, error) {
	builder, err := sharedpg.BuildMarkUnmatchedQuery(contextID, transactionIDs)
	if err != nil {
		return builder, fmt.Errorf("build mark unmatched query: %w", err)
	}

	return builder, nil
}
