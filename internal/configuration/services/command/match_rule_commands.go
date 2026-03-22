package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// CreateMatchRule creates a new match rule for a context.
func (uc *UseCase) CreateMatchRule(
	ctx context.Context,
	contextID uuid.UUID,
	input entities.CreateMatchRuleInput,
) (*entities.MatchRule, error) {
	if uc == nil || uc.matchRuleRepo == nil {
		return nil, ErrNilMatchRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.create_match_rule")
	defer span.End()

	if err := uc.ensurePriorityAvailable(ctx, contextID, input.Priority); err != nil {
		if errors.Is(err, entities.ErrRulePriorityConflict) {
			libOpentelemetry.HandleSpanError(span, "match rule priority conflict", err)
			return nil, err
		}

		libOpentelemetry.HandleSpanError(span, "failed to check match rule priority", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to check match rule priority")

		return nil, err
	}

	if err := entities.ValidateMatchRuleConfig(input.Type, input.Config); err != nil {
		if errors.Is(err, entities.ErrRuleConfigRequired) {
			libOpentelemetry.HandleSpanError(span, "rule config is required", err)
			return nil, err
		}

		libOpentelemetry.HandleSpanError(span, "invalid rule config schema", err)

		logger.With(
			libLog.Any("rule.type", string(input.Type)),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelError, "invalid rule config schema")

		return nil, err
	}

	entity, err := entities.NewMatchRule(ctx, contextID, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid match rule input", err)
		return nil, err
	}

	created, err := uc.matchRuleRepo.Create(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create match rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create match rule")

		return nil, fmt.Errorf("creating match rule: %w", err)
	}

	uc.publishAudit(ctx, "match_rule", created.ID, "create", map[string]any{
		"type":       string(created.Type),
		"priority":   created.Priority,
		"context_id": created.ContextID.String(),
	})

	return created, nil
}

// ensurePriorityAvailable ensures no match rule exists with the given priority.
func (uc *UseCase) ensurePriorityAvailable(
	ctx context.Context,
	contextID uuid.UUID,
	priority int,
) error {
	existing, err := uc.matchRuleRepo.FindByPriority(ctx, contextID, priority)
	if err == nil {
		if existing != nil {
			return entities.ErrRulePriorityConflict
		}

		return nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("finding match rule by priority: %w", err)
	}

	return nil
}

// checkPriorityConflict validates that the new priority doesn't conflict with existing rules.
func (uc *UseCase) checkPriorityConflict(
	ctx context.Context,
	contextID, ruleID uuid.UUID,
	newPriority int,
) error {
	existing, err := uc.matchRuleRepo.FindByPriority(ctx, contextID, newPriority)
	if err == nil {
		if existing != nil && existing.ID != ruleID {
			return entities.ErrRulePriorityConflict
		}

		return nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("finding match rule by priority: %w", err)
	}

	return nil
}

// loadMatchRuleForUpdate loads and validates a match rule exists for update.
func (uc *UseCase) loadMatchRuleForUpdate(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	contextID, ruleID uuid.UUID,
) (*entities.MatchRule, error) {
	entity, err := uc.matchRuleRepo.FindByID(ctx, contextID, ruleID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load match rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load match rule")

		return nil, fmt.Errorf("finding match rule: %w", err)
	}

	if entity == nil {
		libOpentelemetry.HandleSpanError(span, "match rule not found", sql.ErrNoRows)

		return nil, sql.ErrNoRows
	}

	return entity, nil
}

// validatePriorityUpdate checks for priority conflicts when updating a rule.
func (uc *UseCase) validatePriorityUpdate(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	contextID, ruleID uuid.UUID,
	entity *entities.MatchRule,
	input entities.UpdateMatchRuleInput,
) error {
	if input.Priority == nil || *input.Priority == entity.Priority {
		return nil
	}

	if err := uc.checkPriorityConflict(ctx, contextID, ruleID, *input.Priority); err != nil {
		libOpentelemetry.HandleSpanError(span, "match rule priority conflict", err)

		if !errors.Is(err, entities.ErrRulePriorityConflict) {
			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to check match rule priority")
		}

		return err
	}

	return nil
}

// validateRuleConfig validates rule configuration against its type.
func (uc *UseCase) validateRuleConfig(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	ruleType entities.RuleType,
	config map[string]any,
) error {
	if err := entities.ValidateMatchRuleConfig(ruleType, config); err != nil {
		if errors.Is(err, entities.ErrRuleConfigRequired) {
			libOpentelemetry.HandleSpanError(span, "rule config is required", err)

			return err
		}

		libOpentelemetry.HandleSpanError(span, "invalid rule config schema", err)

		logger.With(
			libLog.Any("rule.type", string(ruleType)),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelError, "invalid rule config schema")

		return err
	}

	return nil
}

// resolveRuleTypeAndConfig determines the effective rule type and config for validation.
func resolveRuleTypeAndConfig(
	entity *entities.MatchRule,
	input entities.UpdateMatchRuleInput,
) (entities.RuleType, map[string]any) {
	ruleType := entity.Type
	if input.Type != nil {
		ruleType = *input.Type
	}

	configToValidate := entity.Config
	if input.Config != nil {
		configToValidate = input.Config
	}

	return ruleType, configToValidate
}

// UpdateMatchRule modifies an existing match rule.
func (uc *UseCase) UpdateMatchRule(
	ctx context.Context,
	contextID, ruleID uuid.UUID,
	input entities.UpdateMatchRuleInput,
) (*entities.MatchRule, error) {
	if uc == nil || uc.matchRuleRepo == nil {
		return nil, ErrNilMatchRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.update_match_rule")
	defer span.End()

	entity, err := uc.loadMatchRuleForUpdate(ctx, span, logger, contextID, ruleID)
	if err != nil {
		return nil, err
	}

	if err := uc.validatePriorityUpdate(ctx, span, logger, contextID, ruleID, entity, input); err != nil {
		return nil, err
	}

	ruleType, configToValidate := resolveRuleTypeAndConfig(entity, input)

	if err := uc.validateRuleConfig(ctx, span, logger, ruleType, configToValidate); err != nil {
		return nil, err
	}

	if err := entity.Update(ctx, input); err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid match rule update", err)

		return nil, fmt.Errorf("applying match rule update: %w", err)
	}

	updated, err := uc.matchRuleRepo.Update(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update match rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update match rule")

		return nil, fmt.Errorf("updating match rule: %w", err)
	}

	uc.publishAudit(ctx, "match_rule", updated.ID, "update", map[string]any{
		"type":       string(updated.Type),
		"priority":   updated.Priority,
		"context_id": updated.ContextID.String(),
	})

	return updated, nil
}

// DeleteMatchRule removes a match rule.
func (uc *UseCase) DeleteMatchRule(ctx context.Context, contextID, ruleID uuid.UUID) error {
	if uc == nil || uc.matchRuleRepo == nil {
		return ErrNilMatchRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.delete_match_rule")
	defer span.End()

	if err := uc.matchRuleRepo.Delete(ctx, contextID, ruleID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete match rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to delete match rule")

		return fmt.Errorf("deleting match rule: %w", err)
	}

	uc.publishAudit(ctx, "match_rule", ruleID, "delete", nil)

	return nil
}

// ReorderMatchRulePriorities changes the priority order of match rules.
func (uc *UseCase) ReorderMatchRulePriorities(ctx context.Context, contextID uuid.UUID, ruleIDs []uuid.UUID) error {
	if uc == nil || uc.matchRuleRepo == nil {
		return ErrNilMatchRuleRepository
	}

	if len(ruleIDs) == 0 {
		return ErrRuleIDsRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.reorder_match_rules")
	defer span.End()

	if err := uc.matchRuleRepo.ReorderPriorities(ctx, contextID, ruleIDs); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to reorder match rules", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to reorder match rules")

		return fmt.Errorf("reordering match rules: %w", err)
	}

	// NOTE: Audit event omitted for reorder operation as it represents a bulk priority change
	// that doesn't modify individual rule content. Priority changes are tracked via the
	// updated_at timestamp on affected rules.

	return nil
}
