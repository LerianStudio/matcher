// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// scheduleRepository is an in-memory stub for schedule persistence in tests.
type scheduleRepository struct {
	items map[uuid.UUID]*entities.ReconciliationSchedule
}

func newScheduleRepository() *scheduleRepository {
	return &scheduleRepository{items: make(map[uuid.UUID]*entities.ReconciliationSchedule)}
}

func (repo *scheduleRepository) Create(
	_ context.Context,
	schedule *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	repo.items[schedule.ID] = schedule
	return schedule, nil
}

func (repo *scheduleRepository) FindByID(
	_ context.Context,
	id uuid.UUID,
) (*entities.ReconciliationSchedule, error) {
	schedule, ok := repo.items[id]
	if !ok {
		return nil, sql.ErrNoRows
	}

	return schedule, nil
}

func (repo *scheduleRepository) FindByContextID(
	_ context.Context,
	contextID uuid.UUID,
) ([]*entities.ReconciliationSchedule, error) {
	results := make([]*entities.ReconciliationSchedule, 0)

	for _, schedule := range repo.items {
		if schedule.ContextID == contextID {
			results = append(results, schedule)
		}
	}

	return results, nil
}

func (repo *scheduleRepository) FindDueSchedules(
	_ context.Context,
	now time.Time,
) ([]*entities.ReconciliationSchedule, error) {
	results := make([]*entities.ReconciliationSchedule, 0)

	for _, schedule := range repo.items {
		if schedule.Enabled && schedule.NextRunAt != nil && !schedule.NextRunAt.After(now) {
			results = append(results, schedule)
		}
	}

	return results, nil
}

func (repo *scheduleRepository) Update(
	_ context.Context,
	schedule *entities.ReconciliationSchedule,
) (*entities.ReconciliationSchedule, error) {
	if _, ok := repo.items[schedule.ID]; !ok {
		return nil, sql.ErrNoRows
	}

	repo.items[schedule.ID] = schedule

	return schedule, nil
}

func (repo *scheduleRepository) Delete(_ context.Context, id uuid.UUID) error {
	if _, ok := repo.items[id]; !ok {
		return sql.ErrNoRows
	}

	delete(repo.items, id)

	return nil
}

// newQueryUseCaseForTest creates a query use case without optional dependencies.
func newQueryUseCaseForTest(
	contextRepo repositories.ContextRepository,
	sourceRepo repositories.SourceRepository,
	fieldMapRepo repositories.FieldMapRepository,
	matchRuleRepo repositories.MatchRuleRepository,
) (*query.UseCase, error) {
	return query.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
}

// feeScheduleRepository is an in-memory stub for fee schedule persistence in tests.
type feeScheduleRepository struct {
	items     map[uuid.UUID]*fee.FeeSchedule
	deleteErr error
}

func newFeeScheduleRepository() *feeScheduleRepository {
	return &feeScheduleRepository{items: make(map[uuid.UUID]*fee.FeeSchedule)}
}

func (repo *feeScheduleRepository) Create(
	_ context.Context,
	schedule *fee.FeeSchedule,
) (*fee.FeeSchedule, error) {
	repo.items[schedule.ID] = schedule
	return schedule, nil
}

func (repo *feeScheduleRepository) GetByID(
	_ context.Context,
	id uuid.UUID,
) (*fee.FeeSchedule, error) {
	schedule, ok := repo.items[id]
	if !ok {
		return nil, fee.ErrFeeScheduleNotFound
	}

	return schedule, nil
}

func (repo *feeScheduleRepository) Update(
	_ context.Context,
	schedule *fee.FeeSchedule,
) (*fee.FeeSchedule, error) {
	if _, ok := repo.items[schedule.ID]; !ok {
		return nil, fee.ErrFeeScheduleNotFound
	}

	repo.items[schedule.ID] = schedule

	return schedule, nil
}

func (repo *feeScheduleRepository) Delete(_ context.Context, id uuid.UUID) error {
	if repo.deleteErr != nil {
		return repo.deleteErr
	}

	if _, ok := repo.items[id]; !ok {
		return fee.ErrFeeScheduleNotFound
	}

	delete(repo.items, id)

	return nil
}

func (repo *feeScheduleRepository) List(
	_ context.Context,
	limit int,
) ([]*fee.FeeSchedule, error) {
	results := make([]*fee.FeeSchedule, 0, len(repo.items))

	for _, s := range repo.items {
		results = append(results, s)

		if len(results) >= limit {
			break
		}
	}

	return results, nil
}

func (repo *feeScheduleRepository) GetByIDs(
	_ context.Context,
	ids []uuid.UUID,
) (map[uuid.UUID]*fee.FeeSchedule, error) {
	result := make(map[uuid.UUID]*fee.FeeSchedule, len(ids))

	for _, id := range ids {
		if s, ok := repo.items[id]; ok {
			result[id] = s
		}
	}

	return result, nil
}

// Compile-time interface checks.
var (
	_ configPorts.ScheduleRepository    = (*scheduleRepository)(nil)
	_ sharedPorts.FeeScheduleRepository = (*feeScheduleRepository)(nil)
	_ repositories.FeeRuleRepository    = (*feeRuleRepository)(nil)
)

// feeRuleRepository is an in-memory stub for fee rule persistence in tests.
type feeRuleRepository struct {
	items            map[uuid.UUID]*fee.FeeRule
	findByIDOverride func(ctx context.Context, id uuid.UUID) (*fee.FeeRule, error)
}

func newFeeRuleRepository() *feeRuleRepository {
	return &feeRuleRepository{items: make(map[uuid.UUID]*fee.FeeRule)}
}

func (repo *feeRuleRepository) Create(_ context.Context, rule *fee.FeeRule) error {
	repo.items[rule.ID] = rule
	return nil
}

func (repo *feeRuleRepository) CreateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	repo.items[rule.ID] = rule
	return nil
}

func (repo *feeRuleRepository) FindByID(ctx context.Context, id uuid.UUID) (*fee.FeeRule, error) {
	if repo.findByIDOverride != nil {
		return repo.findByIDOverride(ctx, id)
	}

	rule, ok := repo.items[id]
	if !ok {
		return nil, fee.ErrFeeRuleNotFound
	}

	return rule, nil
}

func (repo *feeRuleRepository) FindByContextID(_ context.Context, contextID uuid.UUID) ([]*fee.FeeRule, error) {
	result := make([]*fee.FeeRule, 0)
	for _, rule := range repo.items {
		if rule.ContextID == contextID {
			result = append(result, rule)
		}
	}

	return result, nil
}

func (repo *feeRuleRepository) Update(_ context.Context, rule *fee.FeeRule) error {
	if _, ok := repo.items[rule.ID]; !ok {
		return fee.ErrFeeRuleNotFound
	}

	repo.items[rule.ID] = rule
	return nil
}

func (repo *feeRuleRepository) UpdateWithTx(_ context.Context, _ *sql.Tx, rule *fee.FeeRule) error {
	return repo.Update(context.Background(), rule)
}

func (repo *feeRuleRepository) Delete(_ context.Context, _ uuid.UUID, id uuid.UUID) error {
	if _, ok := repo.items[id]; !ok {
		return fee.ErrFeeRuleNotFound
	}

	delete(repo.items, id)
	return nil
}

func (repo *feeRuleRepository) DeleteWithTx(_ context.Context, _ *sql.Tx, _ uuid.UUID, id uuid.UUID) error {
	return repo.Delete(context.Background(), uuid.Nil, id)
}
