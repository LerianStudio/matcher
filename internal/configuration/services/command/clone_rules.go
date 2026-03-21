package command

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func (uc *UseCase) cloneMatchRules(ctx context.Context, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneMatchRulesInternal(ctx, nil, sourceContextID, newContextID)
}

func (uc *UseCase) cloneMatchRulesWithTx(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneMatchRulesInternal(ctx, tx, sourceContextID, newContextID)
}

func (uc *UseCase) cloneFeeRules(ctx context.Context, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneFeeRulesInternal(ctx, nil, sourceContextID, newContextID)
}

func (uc *UseCase) cloneFeeRulesWithTx(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneFeeRulesInternal(ctx, tx, sourceContextID, newContextID)
}

func (uc *UseCase) cloneMatchRulesInternal(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	rules, err := uc.fetchAllRules(ctx, sourceContextID)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC()
	cloned := 0

	for _, rule := range rules {
		newRule := &entities.MatchRule{
			ID:        uuid.New(),
			ContextID: newContextID,
			Priority:  rule.Priority,
			Type:      rule.Type,
			Config:    cloneMap(ctx, rule.Config),
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err := uc.createMatchRuleWithOptionalTx(ctx, tx, newRule); err != nil {
			return cloned, fmt.Errorf("creating cloned rule (priority %d): %w", rule.Priority, err)
		}

		cloned++
	}

	return cloned, nil
}

func (uc *UseCase) createMatchRuleWithOptionalTx(ctx context.Context, tx *sql.Tx, rule *entities.MatchRule) error {
	if tx != nil {
		txCreator, ok := uc.matchRuleRepo.(matchRuleTxCreator)
		if !ok {
			return fmt.Errorf("match rule repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		_, err := txCreator.CreateWithTx(ctx, tx, rule)

		return err
	}

	_, err := uc.matchRuleRepo.Create(ctx, rule)

	return err
}

func (uc *UseCase) cloneFeeRulesInternal(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	rules, err := uc.feeRuleRepo.FindByContextID(ctx, sourceContextID)
	if err != nil {
		return 0, fmt.Errorf("fetching fee rules: %w", err)
	}

	now := time.Now().UTC()
	cloned := 0

	for _, rule := range rules {
		if rule == nil {
			continue
		}

		newRule := &fee.FeeRule{
			ID:            uuid.New(),
			ContextID:     newContextID,
			Side:          rule.Side,
			FeeScheduleID: rule.FeeScheduleID,
			Name:          rule.Name,
			Priority:      rule.Priority,
			Predicates:    clonePredicates(rule.Predicates),
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		if err := uc.createFeeRuleWithOptionalTx(ctx, tx, newRule); err != nil {
			return cloned, fmt.Errorf("creating cloned fee rule %q: %w", rule.Name, err)
		}

		cloned++
	}

	return cloned, nil
}

func (uc *UseCase) createFeeRuleWithOptionalTx(ctx context.Context, tx *sql.Tx, rule *fee.FeeRule) error {
	if tx != nil {
		txCreator, ok := uc.feeRuleRepo.(feeRuleTxCreator)
		if !ok {
			return fmt.Errorf("fee rule repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		return txCreator.CreateWithTx(ctx, tx, rule)
	}

	return uc.feeRuleRepo.Create(ctx, rule)
}

func (uc *UseCase) fetchAllRules(ctx context.Context, contextID uuid.UUID) (entities.MatchRules, error) {
	var allRules entities.MatchRules

	cursor := ""
	for {
		rules, pagination, err := uc.matchRuleRepo.FindByContextID(ctx, contextID, cursor, maxClonePaginationLimit)
		if err != nil {
			return nil, fmt.Errorf("fetching rules page: %w", err)
		}

		allRules = append(allRules, rules...)

		if pagination.Next == "" {
			break
		}

		cursor = pagination.Next
	}

	return allRules, nil
}

func clonePredicates(predicates []fee.FieldPredicate) []fee.FieldPredicate {
	if len(predicates) == 0 {
		return nil
	}

	cloned := make([]fee.FieldPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		copyPredicate := predicate
		if len(predicate.Values) > 0 {
			copyPredicate.Values = append([]string(nil), predicate.Values...)
		}

		cloned = append(cloned, copyPredicate)
	}

	return cloned
}
