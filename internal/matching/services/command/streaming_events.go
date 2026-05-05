// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

// MatchRunFailedCode is the stable external error code for match run failures.
// The producer-side error is intentionally discarded at this boundary to
// avoid leaking internal error text into the catalog-defined external schema.
const MatchRunFailedCode = "MATCH_RUN_FAILED"

// transactionMatchStatusPayload is the typed shape for `transaction.matched`
// and `transaction.pending_review` events.
//
// Hot-path note: `transaction.matched` is matcher's single highest-frequency
// emit site — one event per matched transaction, fanned out per match group
// per match run. Avoiding the reflection-heavy map[string]any encoding path
// here is the largest single win in the streaming hot loop. Optional fields
// use omitempty so the on-the-wire payload stays identical to the previous
// map-based encoder.
type transactionMatchStatusPayload struct {
	TransactionID         string `json:"transaction_id"`
	ContextID             string `json:"context_id"`
	MatchRunID            string `json:"match_run_id"`
	PreviousStatus        string `json:"previous_status"`
	Status                string `json:"status"`
	MatchGroupID          string `json:"match_group_id,omitempty"`
	CandidateMatchGroupID string `json:"candidate_match_group_id,omitempty"`
	SourceID              string `json:"source_id,omitempty"`
	MatchedAt             string `json:"matched_at,omitempty"`
	PendingReviewAt       string `json:"pending_review_at,omitempty"`
}

func (uc *UseCase) emitMatchRunCompleted(ctx context.Context, span trace.Span, run *matchingEntities.MatchRun) {
	if run == nil {
		return
	}

	payload := map[string]any{
		"match_run_id": run.ID.String(),
		"context_id":   run.ContextID.String(),
		"mode":         string(run.Mode),
		"status":       string(run.Status),
		"stats":        run.Stats,
		"started_at":   formatMatchingTime(run.StartedAt),
	}
	if run.CompletedAt != nil {
		payload["completed_at"] = formatMatchingTime(*run.CompletedAt)
	}

	uc.emitMatchingPayload(ctx, span, "match_run.completed", run.ID.String(), payload)
}

func (uc *UseCase) emitMatchRunFailed(ctx context.Context, run *matchingEntities.MatchRun) {
	if run == nil {
		return
	}

	failureReason := ""
	if run.FailureReason != nil {
		failureReason = MatchRunFailedCode
	}

	payload := map[string]any{
		"match_run_id":   run.ID.String(),
		"context_id":     run.ContextID.String(),
		"mode":           string(run.Mode),
		"status":         string(run.Status),
		"failure_reason": failureReason,
		"started_at":     formatMatchingTime(run.StartedAt),
	}
	if run.CompletedAt != nil {
		payload["completed_at"] = formatMatchingTime(*run.CompletedAt)
	}

	uc.emitMatchingPayload(ctx, nil, "match_run.failed", run.ID.String(), payload)
}

func (uc *UseCase) emitMatchArtifacts(ctx context.Context, span trace.Span, run *matchingEntities.MatchRun, groups []*matchingEntities.MatchGroup, autoMatchedIDs, pendingReviewIDs []uuid.UUID, feeInput *feeVerificationInput) {
	if run == nil {
		return
	}

	groupByTransaction := map[uuid.UUID]*matchingEntities.MatchGroup{}

	for _, group := range groups {
		if group == nil {
			continue
		}

		if group.Status == matchingVO.MatchGroupStatusConfirmed {
			uc.emitMatchGroupConfirmed(ctx, span, group)
		}

		for _, item := range group.Items {
			if item != nil {
				groupByTransaction[item.TransactionID] = group
			}
		}
	}

	for _, transactionID := range autoMatchedIDs {
		uc.emitTransactionMatchStatus(ctx, span, "transaction.matched", transactionID, run, groupByTransaction[transactionID], feeInput, "MATCHED")
	}

	for _, transactionID := range pendingReviewIDs {
		uc.emitTransactionMatchStatus(ctx, span, "transaction.pending_review", transactionID, run, groupByTransaction[transactionID], feeInput, "PENDING_REVIEW")
	}
}

// emitMatchGroupConfirmed emits the match_group.confirmed event for a freshly
// confirmed group.
//
// REQUIRES: group != nil. The caller (emitMatchArtifacts) walks the slice and
// skips nil entries before reaching this method, so the contract is enforced
// upstream rather than re-checked here. Adding a defensive nil-guard would
// silently mask a caller bug that violates the documented invariant.
func (uc *UseCase) emitMatchGroupConfirmed(ctx context.Context, span trace.Span, group *matchingEntities.MatchGroup) {
	payload := map[string]any{
		"match_group_id":  group.ID.String(),
		"match_run_id":    group.RunID.String(),
		"context_id":      group.ContextID.String(),
		"rule_id":         group.RuleID.String(),
		"transaction_ids": matchItemTransactionIDs(group.Items),
		"confidence":      group.Confidence.Value(),
		"status":          string(group.Status),
	}
	if group.ConfirmedAt != nil {
		payload["confirmed_at"] = formatMatchingTime(*group.ConfirmedAt)
	}

	uc.emitMatchingPayload(ctx, span, "match_group.confirmed", group.ID.String(), payload)
}

// emitTransactionMatchStatus emits transaction.matched or transaction.pending_review
// with a typed payload to avoid map[string]any allocation pressure on the
// per-transaction hot path.
//
// REQUIRES: run != nil; caller-validated by emitMatchArtifacts which skips
// the loop when run is nil. group may be nil for transactions that did not
// land in a confirmed group (pending_review path). Adding a defensive nil-guard
// for run would silently mask a caller bug that violates the documented invariant.
func (uc *UseCase) emitTransactionMatchStatus(ctx context.Context, span trace.Span, definitionKey string, transactionID uuid.UUID, run *matchingEntities.MatchRun, group *matchingEntities.MatchGroup, feeInput *feeVerificationInput, status string) {
	payload := transactionMatchStatusPayload{
		TransactionID:  transactionID.String(),
		ContextID:      run.ContextID.String(),
		MatchRunID:     run.ID.String(),
		PreviousStatus: "UNMATCHED",
		Status:         status,
	}

	if group != nil {
		groupID := group.ID.String()
		payload.MatchGroupID = groupID
		payload.CandidateMatchGroupID = groupID
	}

	if feeInput != nil {
		if tx, ok := feeInput.txByID[transactionID]; ok && tx != nil {
			payload.SourceID = tx.SourceID.String()
		}
	}

	now := formatMatchingTime(time.Now().UTC())

	switch definitionKey {
	case "transaction.matched":
		payload.MatchedAt = now
	case "transaction.pending_review":
		payload.PendingReviewAt = now
	}

	if err := emission.Emit(ctx, uc.streamEmitter, definitionKey, transactionID.String(), payload); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
		} else {
			emitWarnNoSpan(ctx, definitionKey, err)
		}
	}
}

func (uc *UseCase) emitMatchGroupUnmatched(ctx context.Context, span trace.Span, group *matchingEntities.MatchGroup, reason string) {
	if group == nil {
		return
	}

	payload := map[string]any{
		"match_group_id":  group.ID.String(),
		"match_run_id":    group.RunID.String(),
		"context_id":      group.ContextID.String(),
		"rule_id":         group.RuleID.String(),
		"transaction_ids": matchItemTransactionIDs(group.Items),
		"previous_status": "CONFIRMED",
		"status":          string(group.Status),
		"reason":          reason,
		"unmatched_at":    formatMatchingTime(group.UpdatedAt),
	}

	uc.emitMatchingPayload(ctx, span, "match_group.unmatched", group.ID.String(), payload)
}

func (uc *UseCase) emitFeeVariancesCreated(ctx context.Context, span trace.Span, variances []*matchingEntities.FeeVariance) {
	for _, variance := range variances {
		if variance == nil {
			continue
		}

		payload := map[string]any{
			"fee_variance_id":            variance.ID.String(),
			"context_id":                 variance.ContextID.String(),
			"match_run_id":               variance.RunID.String(),
			"match_group_id":             variance.MatchGroupID.String(),
			"transaction_id":             variance.TransactionID.String(),
			"fee_schedule_id":            variance.FeeScheduleID.String(),
			"fee_schedule_name_snapshot": variance.FeeScheduleNameSnapshot,
			"currency":                   variance.Currency,
			"expected_fee":               variance.ExpectedFee.String(),
			"actual_fee":                 variance.ActualFee.String(),
			"delta":                      variance.Delta.String(),
			"variance_type":              variance.VarianceType,
			"created_at":                 formatMatchingTime(variance.CreatedAt),
		}

		uc.emitMatchingPayload(ctx, span, "fee_variance.created", variance.ID.String(), payload)
	}
}

func matchItemTransactionIDs(items []*matchingEntities.MatchItem) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		if item != nil {
			ids = append(ids, item.TransactionID.String())
		}
	}

	return ids
}

func (uc *UseCase) emitMatchingPayload(ctx context.Context, span trace.Span, definitionKey, subject string, payload map[string]any) {
	if err := emission.Emit(ctx, uc.streamEmitter, definitionKey, subject, payload); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
		} else {
			emitWarnNoSpan(ctx, definitionKey, err)
		}
	}
}

// emitWarnNoSpan logs IMPORTANT-tier emission failures when no active span is
// available to attribute the error to. Matcher does not silently drop emit
// failures: the pattern mirrors configuration.emitConfigurationEvent.
func emitWarnNoSpan(ctx context.Context, definitionKey string, err error) {
	// NewTrackingFromContext is the canonical accessor in lib-commons/v5; it
	// returns (logger, tracer, requestID, metricsFactory) and only the logger
	// is needed here. lib-commons itself uses the same nolint at its own
	// call sites (see commons/net/http/ratelimit/middleware.go).
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed; see lib-commons NewTrackingFromContext signature
	if logger == nil {
		return
	}

	logger.With(libLog.Err(err)).Log(ctx, libLog.LevelWarn, "failed to emit streaming event "+definitionKey+" without span")
}

// formatMatchingTime delegates to emission.FormatTime; preserved as a thin
// wrapper for backward compatibility with existing unit tests.
func formatMatchingTime(value time.Time) string {
	return emission.FormatTime(value)
}
