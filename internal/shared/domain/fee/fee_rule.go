// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// maxFeeRuleNameLength is the maximum allowed length for a fee rule name.
const maxFeeRuleNameLength = 100

// maxFeeRulePredicates is the maximum number of predicates a fee rule may contain.
const maxFeeRulePredicates = 50

// MaxFeeRulesPerContext is the maximum number of fee rules allowed for a context.
const MaxFeeRulesPerContext = constants.MaximumPaginationLimit

// FeeRule defines a conditional mapping from transaction metadata to a fee schedule.
// Rules are evaluated in priority order (lower = higher precedence); first match wins.
type FeeRule struct {
	ID            uuid.UUID
	ContextID     uuid.UUID
	Side          MatchingSide
	FeeScheduleID uuid.UUID
	Name          string
	Priority      int
	Predicates    []FieldPredicate
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewFeeRule creates and validates a new FeeRule.
func NewFeeRule(
	ctx context.Context,
	contextID, feeScheduleID uuid.UUID,
	side MatchingSide,
	name string,
	priority int,
	predicates []FieldPredicate,
) (*FeeRule, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "fee.rule.new")
	trimmedName := strings.TrimSpace(name)

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, fmt.Errorf("fee rule context id: %w", ErrFeeRuleContextIDRequired)
	}

	if err := asserter.That(ctx, feeScheduleID != uuid.Nil, "fee schedule id is required"); err != nil {
		return nil, fmt.Errorf("fee rule schedule id: %w", ErrFeeRuleScheduleIDRequired)
	}

	if err := asserter.That(ctx, side.IsValid(), "invalid matching side", "side", string(side)); err != nil {
		return nil, fmt.Errorf("fee rule side: %w", ErrInvalidMatchingSide)
	}

	if err := asserter.NotEmpty(ctx, trimmedName, ErrFeeRuleNameRequired.Error()); err != nil {
		return nil, fmt.Errorf("fee rule name: %w", ErrFeeRuleNameRequired)
	}

	if err := asserter.That(ctx, len(trimmedName) <= maxFeeRuleNameLength, ErrFeeRuleNameTooLong.Error(), "length", len(trimmedName)); err != nil {
		return nil, fmt.Errorf("fee rule name: %w", ErrFeeRuleNameTooLong)
	}

	if err := asserter.That(ctx, priority >= 0, "priority must be non-negative", "priority", priority); err != nil {
		return nil, fmt.Errorf("fee rule priority: %w", ErrFeeRulePriorityNegative)
	}

	if err := asserter.That(ctx, len(predicates) <= maxFeeRulePredicates, ErrFeeRuleTooManyPredicates.Error(), "count", len(predicates)); err != nil {
		return nil, fmt.Errorf("fee rule predicates: %w", ErrFeeRuleTooManyPredicates)
	}

	normalizedPredicates := make([]FieldPredicate, len(predicates))
	copy(normalizedPredicates, predicates)

	for i, pred := range normalizedPredicates {
		pred.Field = strings.TrimSpace(pred.Field)
		normalizedPredicates[i] = pred

		if err := pred.Validate(ctx); err != nil {
			return nil, fmt.Errorf("fee rule predicate[%d]: %w", i, err)
		}
	}

	now := time.Now().UTC()

	return &FeeRule{
		ID:            uuid.New(),
		ContextID:     contextID,
		Side:          side,
		FeeScheduleID: feeScheduleID,
		Name:          trimmedName,
		Priority:      priority,
		Predicates:    normalizedPredicates,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

// UpdateFeeRuleInput holds the optional fields for a partial fee rule update.
type UpdateFeeRuleInput struct {
	Side          *string
	FeeScheduleID *string
	Name          *string
	Priority      *int
	Predicates    *[]FieldPredicate
}

// Update applies partial changes to a FeeRule, re-validating every changed field.
func (fr *FeeRule) Update(ctx context.Context, input UpdateFeeRuleInput) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "fee.rule.update")

	if err := asserter.NotNil(ctx, fr, "fee rule is required"); err != nil {
		return ErrFeeRuleNotFound
	}

	if input.Side != nil {
		s := MatchingSide(*input.Side)
		if err := asserter.That(ctx, s.IsValid(), "invalid matching side", "side", *input.Side); err != nil {
			return fmt.Errorf("fee rule side: %w", ErrInvalidMatchingSide)
		}

		fr.Side = s
	}

	if input.Name != nil {
		trimmedName := strings.TrimSpace(*input.Name)

		if err := asserter.NotEmpty(ctx, trimmedName, ErrFeeRuleNameRequired.Error()); err != nil {
			return fmt.Errorf("fee rule name: %w", ErrFeeRuleNameRequired)
		}

		if err := asserter.That(ctx, len(trimmedName) <= maxFeeRuleNameLength, ErrFeeRuleNameTooLong.Error(), "length", len(trimmedName)); err != nil {
			return fmt.Errorf("fee rule name: %w", ErrFeeRuleNameTooLong)
		}

		fr.Name = trimmedName
	}

	if input.Priority != nil {
		if err := asserter.That(ctx, *input.Priority >= 0, "priority must be non-negative", "priority", *input.Priority); err != nil {
			return fmt.Errorf("fee rule priority: %w", ErrFeeRulePriorityNegative)
		}

		fr.Priority = *input.Priority
	}

	if input.FeeScheduleID != nil {
		parsed, err := parseFeeScheduleID(ctx, asserter, *input.FeeScheduleID)
		if err != nil {
			return err
		}

		fr.FeeScheduleID = parsed
	}

	if input.Predicates != nil {
		preds := make([]FieldPredicate, len(*input.Predicates))
		copy(preds, *input.Predicates)

		if err := validatePredicates(ctx, asserter, preds); err != nil {
			return err
		}

		fr.Predicates = preds
	}

	fr.UpdatedAt = time.Now().UTC()

	return nil
}

// parseFeeScheduleID parses and validates a fee schedule ID string.
func parseFeeScheduleID(ctx context.Context, asserter *assert.Asserter, raw string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("fee rule fee schedule id: %w", ErrFeeRuleScheduleIDRequired)
	}

	if err := asserter.That(ctx, parsed != uuid.Nil, "fee schedule id is required"); err != nil {
		return uuid.Nil, fmt.Errorf("fee rule fee schedule id: %w", ErrFeeRuleScheduleIDRequired)
	}

	return parsed, nil
}

// validatePredicates checks count and individual predicate validity.
func validatePredicates(ctx context.Context, asserter *assert.Asserter, preds []FieldPredicate) error {
	if err := asserter.That(ctx, len(preds) <= maxFeeRulePredicates, ErrFeeRuleTooManyPredicates.Error(), "count", len(preds)); err != nil {
		return fmt.Errorf("fee rule predicates: %w", ErrFeeRuleTooManyPredicates)
	}

	for i, pred := range preds {
		pred.Field = strings.TrimSpace(pred.Field)
		preds[i] = pred

		if err := pred.Validate(ctx); err != nil {
			return fmt.Errorf("fee rule predicate[%d]: %w", i, err)
		}
	}

	return nil
}
