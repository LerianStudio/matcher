package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Sentinel errors for fee rule operations.
var (
	// ErrNilFeeRuleRepository is returned when the fee rule repository is nil.
	ErrNilFeeRuleRepository = errors.New("fee rule repository is required")
	// ErrDuplicateFeeRulePriority is returned when a fee rule with the same priority already exists.
	ErrDuplicateFeeRulePriority = errors.New("a fee rule with this priority already exists in the context")
	// ErrDuplicateFeeRuleName is returned when a fee rule with the same name already exists.
	ErrDuplicateFeeRuleName = errors.New("a fee rule with this name already exists in the context")
)

// PostgreSQL unique constraint names for fee rules.
const (
	constraintFeeRulePriority = "uq_fee_rules_context_priority"
	constraintFeeRuleName     = "uq_fee_rules_context_name"
	constraintFeeRuleSchedule = "fk_fee_rules_fee_schedule"
)

// WithFeeRuleRepository sets the fee rule repository for the use case.
func WithFeeRuleRepository(repo repositories.FeeRuleRepository) UseCaseOption {
	return func(uc *UseCase) {
		if repo != nil {
			uc.feeRuleRepo = repo
		}
	}
}

// CreateFeeRule creates a new fee rule.
func (uc *UseCase) CreateFeeRule(
	ctx context.Context,
	contextID uuid.UUID,
	side string,
	feeScheduleID uuid.UUID,
	name string,
	priority int,
	predicates []fee.FieldPredicate,
) (*fee.FeeRule, error) {
	if uc == nil || uc.feeRuleRepo == nil {
		return nil, ErrNilFeeRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.create_fee_rule")
	defer span.End()

	// Soft limit: the count check is application-level and not atomic under concurrent creates.
	// The unique (context_id, priority) constraint naturally serializes same-priority attempts,
	// limiting the practical race window. Exceeding the limit by 1 under extreme concurrency is
	// acceptable — the runtime also enforces the cap at match time (loadFeeRulesAndSchedules).
	existingRules, err := uc.feeRuleRepo.FindByContextID(ctx, contextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load fee rules for limit check", err)
		return nil, fmt.Errorf("create fee rule: loading existing fee rules: %w", err)
	}

	if len(existingRules) >= fee.MaxFeeRulesPerContext {
		limitErr := fmt.Errorf("create fee rule: %w", fee.ErrFeeRuleCountLimitExceeded)
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "fee rule count limit exceeded", limitErr)

		return nil, limitErr
	}

	entity, err := fee.NewFeeRule(ctx, contextID, feeScheduleID, fee.MatchingSide(side), name, priority, predicates)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid fee rule input", err)
		return nil, fmt.Errorf("create fee rule: %w", err)
	}

	if err := uc.feeRuleRepo.Create(ctx, entity); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create fee rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create fee rule")

		if mappedErr := mapFeeRuleConstraintError(err); mappedErr != nil {
			return nil, mappedErr
		}

		return nil, fmt.Errorf("creating fee rule: %w", err)
	}

	uc.publishAudit(ctx, "fee_rule", entity.ID, "create", map[string]any{
		"name":       entity.Name,
		"context_id": entity.ContextID.String(),
		"side":       string(entity.Side),
		"priority":   entity.Priority,
	})

	return entity, nil
}

// UpdateFeeRule modifies an existing fee rule.
func (uc *UseCase) UpdateFeeRule(
	ctx context.Context,
	contextID uuid.UUID,
	feeRuleID uuid.UUID,
	side *string,
	feeScheduleID *string,
	name *string,
	priority *int,
	predicates *[]fee.FieldPredicate,
) (*fee.FeeRule, error) {
	if uc == nil || uc.feeRuleRepo == nil {
		return nil, ErrNilFeeRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.update_fee_rule")
	defer span.End()

	entity, err := uc.findFeeRuleInContext(ctx, contextID, feeRuleID)
	if err != nil {
		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return nil, fee.ErrFeeRuleNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to load fee rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee rule")

		return nil, fmt.Errorf("finding fee rule: %w", err)
	}

	if entity == nil {
		return nil, fee.ErrFeeRuleNotFound
	}

	if err := entity.Update(ctx, fee.UpdateFeeRuleInput{
		Side:          side,
		FeeScheduleID: feeScheduleID,
		Name:          name,
		Priority:      priority,
		Predicates:    predicates,
	}); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid fee rule update", err)
		return nil, fmt.Errorf("update fee rule: %w", err)
	}

	if err := uc.feeRuleRepo.Update(ctx, entity); err != nil {
		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return nil, fee.ErrFeeRuleNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to update fee rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update fee rule")

		if mappedErr := mapFeeRuleConstraintError(err); mappedErr != nil {
			return nil, mappedErr
		}

		return nil, fmt.Errorf("updating fee rule: %w", err)
	}

	uc.publishAudit(ctx, "fee_rule", entity.ID, "update", map[string]any{
		"name": entity.Name,
	})

	return entity, nil
}

// DeleteFeeRuleInContext removes a fee rule by ID after verifying it belongs to the provided context.
func (uc *UseCase) DeleteFeeRuleInContext(ctx context.Context, contextID, feeRuleID uuid.UUID) error {
	return uc.deleteFeeRule(ctx, contextID, feeRuleID)
}

func (uc *UseCase) deleteFeeRule(ctx context.Context, contextID, feeRuleID uuid.UUID) error {
	if uc == nil || uc.feeRuleRepo == nil {
		return ErrNilFeeRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.delete_fee_rule")
	defer span.End()

	existing, err := uc.findFeeRuleInContext(ctx, contextID, feeRuleID)
	if err != nil {
		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return fee.ErrFeeRuleNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to load fee rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee rule")

		return fmt.Errorf("finding fee rule: %w", err)
	}

	if existing == nil {
		return fee.ErrFeeRuleNotFound
	}

	if err := uc.feeRuleRepo.Delete(ctx, contextID, feeRuleID); err != nil {
		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return fee.ErrFeeRuleNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to delete fee rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to delete fee rule")

		return fmt.Errorf("deleting fee rule: %w", err)
	}

	uc.publishAudit(ctx, "fee_rule", feeRuleID, "delete", nil)

	return nil
}

func (uc *UseCase) findFeeRuleInContext(
	ctx context.Context,
	contextID uuid.UUID,
	feeRuleID uuid.UUID,
) (*fee.FeeRule, error) {
	if contextID == uuid.Nil {
		return uc.feeRuleRepo.FindByID(ctx, feeRuleID)
	}

	rules, err := uc.feeRuleRepo.FindByContextID(ctx, contextID)
	if err != nil {
		return nil, fmt.Errorf("find fee rules by context: %w", err)
	}

	for _, rule := range rules {
		if rule != nil && rule.ID == feeRuleID {
			return rule, nil
		}
	}

	return nil, fee.ErrFeeRuleNotFound
}

// mapFeeRuleConstraintError maps PostgreSQL unique constraint violations to domain errors.
func mapFeeRuleConstraintError(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return nil
	}

	// PostgreSQL error code 23505 = unique_violation.
	switch pgErr.Code {
	case "23505":
		switch pgErr.ConstraintName {
		case constraintFeeRulePriority:
			return ErrDuplicateFeeRulePriority
		case constraintFeeRuleName:
			return ErrDuplicateFeeRuleName
		default:
			return nil
		}
	case "23503":
		if pgErr.ConstraintName == constraintFeeRuleSchedule {
			return fee.ErrFeeScheduleNotFound
		}

		return nil
	default:
		return nil
	}
}
