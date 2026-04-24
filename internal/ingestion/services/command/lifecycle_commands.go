// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedObservability "github.com/LerianStudio/matcher/internal/shared/observability"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// cleanupOnFailure clears dedup keys if processing failed.
func (uc *UseCase) cleanupOnFailure(ctx context.Context, state *ingestionState) {
	if state == nil || state.job == nil {
		return
	}

	if state.succeeded || len(state.markedHashes) == 0 {
		return
	}

	if clearErr := uc.dedupe.ClearBatch(ctx, state.job.ContextID, state.markedHashes); clearErr != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(libLog.Err(clearErr)).Log(ctx, libLog.LevelWarn, "failed to clear dedup keys on failure")
	}
}

// completeIngestionJob finalizes the job and publishes the completion event.
func (uc *UseCase) completeIngestionJob(
	ctx context.Context,
	state *ingestionState,
	span trace.Span,
) (*entities.IngestionJob, error) {
	if err := state.job.Complete(ctx, state.totalRows, state.totalErrors); err != nil {
		return nil, fmt.Errorf("failed to complete job: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"ingestion",
			struct {
				RecordsCount   int `json:"recordsCount"`
				RecordsValid   int `json:"recordsValid"`
				RecordsInvalid int `json:"recordsInvalid"`
			}{
				RecordsCount:   state.totalRows,
				RecordsValid:   state.totalRows - state.totalErrors,
				RecordsInvalid: state.totalErrors,
			},
			sharedObservability.NewMatcherRedactor(),
		)
	}

	return uc.persistCompletedJob(ctx, state)
}

// persistCompletedJob updates the job and creates the outbox event in a transaction.
func (uc *UseCase) persistCompletedJob(
	ctx context.Context,
	state *ingestionState,
) (*entities.IngestionJob, error) {
	var updatedJob *entities.IngestionJob

	err := uc.jobTxRunner.WithTx(ctx, func(tx *sql.Tx) error {
		updated, err := uc.jobRepoTx.UpdateWithTx(ctx, tx, state.job)
		if err != nil {
			return fmt.Errorf("failed to update job: %w", err)
		}

		effectiveDateRange := state.dateRange
		if effectiveDateRange == nil {
			effectiveDateRange = &ports.DateRange{Start: updated.StartedAt, End: updated.StartedAt}
		}

		event, err := entities.NewIngestionCompletedEvent(
			ctx,
			updated,
			state.totalInserted,
			effectiveDateRange.Start,
			effectiveDateRange.End,
			state.totalRows,
			state.totalErrors,
		)
		if err != nil {
			return fmt.Errorf("failed to create completed event: %w", err)
		}

		body, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		outboxEvent, err := shared.NewOutboxEvent(ctx, event.EventType, event.JobID, body)
		if err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		if _, err := uc.outboxRepoTx.CreateWithTx(ctx, tx, outboxEvent); err != nil {
			return fmt.Errorf("failed to create outbox entry: %w", err)
		}

		updatedJob = updated

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to complete ingestion: %w", err)
	}

	return updatedJob, nil
}

// IgnoreTransactionInput contains the data required to ignore a transaction.
type IgnoreTransactionInput struct {
	TransactionID uuid.UUID
	ContextID     uuid.UUID
	Reason        string
}

// IgnoreTransaction sets a transaction's status to IGNORED.
func (uc *UseCase) IgnoreTransaction(
	ctx context.Context,
	input IgnoreTransactionInput,
) (*shared.Transaction, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "command.ingestion.ignore_transaction")

	if span != nil {
		defer span.End()

		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"ignore_transaction",
			struct {
				TransactionID string `json:"transactionId"`
				ContextID     string `json:"contextId"`
				Reason        string `json:"reason"`
			}{
				TransactionID: input.TransactionID.String(),
				ContextID:     input.ContextID.String(),
				Reason:        input.Reason,
			},
			sharedObservability.NewMatcherRedactor(),
		)
	}

	if strings.TrimSpace(input.Reason) == "" {
		return nil, ErrReasonRequired
	}

	existing, err := uc.transactionRepo.FindByID(ctx, input.TransactionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTransactionNotFound
		}

		return nil, fmt.Errorf("failed to find transaction: %w", err)
	}

	if existing == nil {
		return nil, ErrTransactionNotFound
	}

	if existing.Status != shared.TransactionStatusUnmatched {
		return nil, ErrTransactionNotIgnorable
	}

	updated, err := uc.transactionRepo.UpdateStatus(
		ctx,
		input.TransactionID,
		input.ContextID,
		shared.TransactionStatusIgnored,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTransactionNotFound
		}

		return nil, fmt.Errorf("failed to update transaction status: %w", err)
	}

	return updated, nil
}

func (uc *UseCase) failJob(
	ctx context.Context, //nolint:contextcheck // cleanup function intentionally uses context.WithoutCancel to outlive parent cancellation
	job *entities.IngestionJob,
	cause error,
	markedHashes []string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	persistCtx := context.WithoutCancel(ctx)

	logger, _, _, _ := libCommons.NewTrackingFromContext(persistCtx) //nolint:dogsled // only logger needed

	// Clean up dedup keys to allow retries
	if job != nil && len(markedHashes) > 0 {
		if clearErr := uc.dedupe.ClearBatch(persistCtx, job.ContextID, markedHashes); clearErr != nil {
			logger.With(libLog.Err(clearErr)).Log(persistCtx, libLog.LevelWarn, "failed to clear dedup keys on job failure")
		}
	}

	if job == nil {
		return cause
	}

	if err := job.Fail(persistCtx, cause.Error()); err != nil {
		return fmt.Errorf("failed to transition job to failed: %w", err)
	}

	err := uc.jobTxRunner.WithTx(persistCtx, func(tx *sql.Tx) error {
		updated, err := uc.jobRepoTx.UpdateWithTx(persistCtx, tx, job)
		if err != nil {
			return fmt.Errorf("failed to update job after error: %w", err)
		}

		event, err := entities.NewIngestionFailedEvent(persistCtx, updated)
		if err != nil {
			return fmt.Errorf("failed to create failed event: %w", err)
		}

		body, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal ingestion failed event: %w", err)
		}

		outboxEvent, err := shared.NewOutboxEvent(persistCtx, event.EventType, event.JobID, body)
		if err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		if _, err := uc.outboxRepoTx.CreateWithTx(persistCtx, tx, outboxEvent); err != nil {
			return fmt.Errorf("failed to enqueue ingestion failure: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to save failed job: %w", err)
	}

	// Cleanup runs in a separate best-effort transaction so a cleanup SQL failure
	// cannot poison the primary transaction that persists FAILED status + outbox event.
	uc.cleanupPartialTransactionsBestEffort(persistCtx, job.ID)

	return cause
}

func (uc *UseCase) cleanupPartialTransactionsBestEffort(ctx context.Context, jobID uuid.UUID) {
	if uc == nil || uc.jobTxRunner == nil || uc.txCleanupRepoTx == nil {
		return
	}

	if ctx == nil {
		ctx = context.Background() //nolint:contextcheck // best-effort cleanup intentionally uses fresh context when nil
	}

	logger, tracer, _, headerID := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.ingestion.cleanup_partial_transactions")
	defer span.End()

	if err := uc.jobTxRunner.WithTx(ctx, func(tx *sql.Tx) error {
		if err := uc.txCleanupRepoTx.CleanupFailedJobTransactionsWithTx(ctx, tx, jobID); err != nil {
			return fmt.Errorf("cleanup failed job transactions: %w", err)
		}

		return nil
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to execute best-effort partial transaction cleanup", err)

		if logger != nil {
			logger.With(
				libLog.String("job_id", jobID.String()),
				libLog.Any("header_id", headerID),
				libLog.Err(err),
			).Log(ctx, libLog.LevelWarn, "failed to execute best-effort partial transaction cleanup")
		}
	}
}

// for the context and triggers an asynchronous match run if so.
// This is fire-and-forget; errors are logged but do not affect ingestion.
func (uc *UseCase) triggerAutoMatchIfEnabled(ctx context.Context, contextID uuid.UUID) {
	if sharedPorts.IsNilValue(uc.contextProvider) || sharedPorts.IsNilValue(uc.matchTrigger) {
		return
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed

	ctx, span := tracer.Start(ctx, "command.ingestion.trigger_auto_match")
	defer span.End()

	enabled, err := uc.contextProvider.IsAutoMatchEnabled(ctx, contextID)
	if err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(
			libLog.String("context_id", contextID.String()),
			libLog.Err(err),
		).Log(ctx, libLog.LevelWarn, "failed to check auto-match status")

		return
	}

	if !enabled {
		return
	}

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(
			libLog.String("tenant_id", tenantIDStr),
			libLog.Err(err),
		).Log(ctx, libLog.LevelWarn, "auto-match skipped: invalid tenant ID")

		return
	}

	uc.matchTrigger.TriggerMatchForContext(ctx, tenantID, contextID)
}
