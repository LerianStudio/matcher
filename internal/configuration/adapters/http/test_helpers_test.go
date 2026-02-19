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
	items map[uuid.UUID]*fee.FeeSchedule
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
	_ configPorts.FeeScheduleRepository = (*feeScheduleRepository)(nil)
)
