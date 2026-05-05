// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

func (uc *UseCase) emitReconciliationContextCreated(ctx context.Context, span trace.Span, entity *entities.ReconciliationContext) {
	if entity == nil {
		return
	}

	uc.emitConfigurationEvent(ctx, span, "reconciliation_context.created", entity.ID.String(), map[string]any{
		"context_id":           entity.ID.String(),
		"name":                 entity.Name,
		"context_type":         entity.Type.String(),
		"interval":             entity.Interval,
		"status":               string(entity.Status),
		"auto_match_on_upload": entity.AutoMatchOnUpload,
		"created_at":           formatConfigurationTime(entity.CreatedAt),
	})
}

func (uc *UseCase) emitReconciliationContextUpdated(ctx context.Context, span trace.Span, entity *entities.ReconciliationContext) {
	if entity == nil {
		return
	}

	uc.emitConfigurationEvent(ctx, span, "reconciliation_context.updated", entity.ID.String(), map[string]any{
		"context_id":           entity.ID.String(),
		"name":                 entity.Name,
		"context_type":         entity.Type.String(),
		"interval":             entity.Interval,
		"status":               string(entity.Status),
		"auto_match_on_upload": entity.AutoMatchOnUpload,
		"updated_at":           formatConfigurationTime(entity.UpdatedAt),
	})
}

func (uc *UseCase) emitReconciliationContextDeleted(ctx context.Context, span trace.Span, contextID uuid.UUID) {
	uc.emitConfigurationEvent(ctx, span, "reconciliation_context.deleted", contextID.String(), map[string]any{
		"context_id":             contextID.String(),
		"status":                 "DELETED",
		"deleted_at":             formatConfigurationTime(time.Now().UTC()),
		"child_entities_checked": true,
	})
}

func (uc *UseCase) emitReconciliationSourceCreated(ctx context.Context, span trace.Span, entity *entities.ReconciliationSource) {
	if entity == nil {
		return
	}

	uc.emitConfigurationEvent(ctx, span, "reconciliation_source.created", entity.ID.String(), map[string]any{
		"context_id":  entity.ContextID.String(),
		"source_id":   entity.ID.String(),
		"name":        entity.Name,
		"source_type": entity.Type.String(),
		"side":        string(entity.Side),
		"created_at":  formatConfigurationTime(entity.CreatedAt),
	})
}

func (uc *UseCase) emitMatchRuleCreated(ctx context.Context, span trace.Span, entity *entities.MatchRule) {
	if entity == nil {
		return
	}

	uc.emitConfigurationEvent(ctx, span, "match_rule.created", entity.ID.String(), map[string]any{
		"context_id":  entity.ContextID.String(),
		"rule_id":     entity.ID.String(),
		"rule_type":   entity.Type.String(),
		"priority":    entity.Priority,
		"config_hash": hashConfigurationPayload(entity.Config),
		"created_at":  formatConfigurationTime(entity.CreatedAt),
	})
}

func (uc *UseCase) emitMatchRuleReordered(ctx context.Context, span trace.Span, contextID uuid.UUID, ruleIDs []uuid.UUID) {
	orderedRuleIDs := make([]string, len(ruleIDs))
	for i, ruleID := range ruleIDs {
		orderedRuleIDs[i] = ruleID.String()
	}

	uc.emitConfigurationEvent(ctx, span, "match_rule.reordered", contextID.String(), map[string]any{
		"context_id":       contextID.String(),
		"ordered_rule_ids": orderedRuleIDs,
		"priority_version": hashConfigurationPayload(orderedRuleIDs),
		"reordered_at":     formatConfigurationTime(time.Now().UTC()),
	})
}

func (uc *UseCase) emitConfigurationEvent(ctx context.Context, span trace.Span, definitionKey, subject string, payload map[string]any) {
	if emission.IsNilEmitter(uc.streamEmitter) {
		return
	}

	var err error

	payload, err = emission.AddTenantID(ctx, payload)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build streaming event payload "+definitionKey, err)
		return
	}

	if err := emission.Emit(ctx, uc.streamEmitter, definitionKey, subject, payload); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
	}
}

// formatConfigurationTime delegates to emission.FormatTime; preserved as a
// thin wrapper for backward compatibility with existing unit tests.
func formatConfigurationTime(value time.Time) string {
	return emission.FormatTime(value)
}

func hashConfigurationPayload(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(payload)

	return hex.EncodeToString(sum[:])
}
