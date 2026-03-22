//go:build unit

package repositories_test

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Compile-time verification that the interface is defined correctly.
var _ repositories.FeeRuleRepository = (*mockFeeRuleRepository)(nil)

type mockFeeRuleRepository struct {
	rules map[uuid.UUID]*fee.FeeRule
}

func (m *mockFeeRuleRepository) Create(_ context.Context, rule *fee.FeeRule) error {
	if m.rules == nil {
		m.rules = make(map[uuid.UUID]*fee.FeeRule)
	}

	m.rules[rule.ID] = rule

	return nil
}

func (m *mockFeeRuleRepository) CreateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	if m.rules == nil {
		m.rules = make(map[uuid.UUID]*fee.FeeRule)
	}

	m.rules[rule.ID] = rule

	return nil
}

func (m *mockFeeRuleRepository) FindByID(_ context.Context, id uuid.UUID) (*fee.FeeRule, error) {
	if rule, ok := m.rules[id]; ok {
		return rule, nil
	}

	return nil, nil
}

func (m *mockFeeRuleRepository) FindByContextID(
	_ context.Context,
	contextID uuid.UUID,
) ([]*fee.FeeRule, error) {
	result := make([]*fee.FeeRule, 0, len(m.rules))

	for _, rule := range m.rules {
		if rule.ContextID == contextID {
			result = append(result, rule)
		}
	}

	return result, nil
}

func (m *mockFeeRuleRepository) Update(_ context.Context, rule *fee.FeeRule) error {
	if m.rules == nil {
		m.rules = make(map[uuid.UUID]*fee.FeeRule)
	}

	m.rules[rule.ID] = rule

	return nil
}

func (m *mockFeeRuleRepository) UpdateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	if m.rules == nil {
		m.rules = make(map[uuid.UUID]*fee.FeeRule)
	}

	m.rules[rule.ID] = rule

	return nil
}

func (m *mockFeeRuleRepository) Delete(_ context.Context, _ uuid.UUID, id uuid.UUID) error {
	delete(m.rules, id)
	return nil
}

func (m *mockFeeRuleRepository) DeleteWithTx(_ context.Context, _ *sql.Tx, _ uuid.UUID, id uuid.UUID) error {
	delete(m.rules, id)
	return nil
}
