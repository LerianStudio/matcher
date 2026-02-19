//go:build unit

package ports

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// Compile-time interface satisfaction check.
var _ ScheduleRepository = (*mockScheduleRepository)(nil)

// mockScheduleRepository is a manual stub that satisfies the ScheduleRepository interface.
type mockScheduleRepository struct {
	createFn           func(ctx context.Context, schedule *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	findByIDFn         func(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error)
	findByContextIDFn  func(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error)
	findDueSchedulesFn func(ctx context.Context, now time.Time) ([]*entities.ReconciliationSchedule, error)
	updateFn           func(ctx context.Context, schedule *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	deleteFn           func(ctx context.Context, id uuid.UUID) error
}

func (m *mockScheduleRepository) Create(ctx context.Context, schedule *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	if m.createFn != nil {
		return m.createFn(ctx, schedule)
	}

	return schedule, nil
}

func (m *mockScheduleRepository) FindByID(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	return &entities.ReconciliationSchedule{ID: id}, nil
}

func (m *mockScheduleRepository) FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error) {
	if m.findByContextIDFn != nil {
		return m.findByContextIDFn(ctx, contextID)
	}

	return []*entities.ReconciliationSchedule{}, nil
}

func (m *mockScheduleRepository) FindDueSchedules(ctx context.Context, now time.Time) ([]*entities.ReconciliationSchedule, error) {
	if m.findDueSchedulesFn != nil {
		return m.findDueSchedulesFn(ctx, now)
	}

	return []*entities.ReconciliationSchedule{}, nil
}

func (m *mockScheduleRepository) Update(ctx context.Context, schedule *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, schedule)
	}

	return schedule, nil
}

func (m *mockScheduleRepository) Delete(ctx context.Context, id uuid.UUID) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}

	return nil
}

func TestScheduleRepositoryInterfaceSatisfaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mock := &mockScheduleRepository{}

	// Verify Create returns the schedule.
	schedule := &entities.ReconciliationSchedule{ID: testutil.DeterministicUUID("schedule")}
	created, err := mock.Create(ctx, schedule)
	require.NoError(t, err)
	assert.Equal(t, schedule.ID, created.ID)

	// Verify FindByID returns entity with correct ID.
	targetID := testutil.DeterministicUUID("target")
	found, err := mock.FindByID(ctx, targetID)
	require.NoError(t, err)
	assert.Equal(t, targetID, found.ID)

	// Verify FindByContextID returns empty slice.
	list, err := mock.FindByContextID(ctx, testutil.DeterministicUUID("context"))
	require.NoError(t, err)
	assert.Empty(t, list)

	// Verify FindDueSchedules returns empty slice.
	due, err := mock.FindDueSchedules(ctx, testutil.FixedTime())
	require.NoError(t, err)
	assert.Empty(t, due)

	// Verify Update returns the schedule.
	updated, err := mock.Update(ctx, schedule)
	require.NoError(t, err)
	assert.Equal(t, schedule.ID, updated.ID)

	// Verify Delete succeeds.
	err = mock.Delete(ctx, testutil.DeterministicUUID("delete-target"))
	require.NoError(t, err)
}

func TestScheduleRepositoryMock_CustomBehavior(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expectedErr := assert.AnError

	mock := &mockScheduleRepository{
		createFn: func(_ context.Context, _ *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			return nil, expectedErr
		},
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return nil, expectedErr
		},
		deleteFn: func(_ context.Context, _ uuid.UUID) error {
			return expectedErr
		},
	}

	_, err := mock.Create(ctx, &entities.ReconciliationSchedule{})
	assert.ErrorIs(t, err, expectedErr)

	_, err = mock.FindByID(ctx, uuid.New())
	assert.ErrorIs(t, err, expectedErr)

	err = mock.Delete(ctx, uuid.New())
	assert.ErrorIs(t, err, expectedErr)
}
