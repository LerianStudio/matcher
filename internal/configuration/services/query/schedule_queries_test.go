//go:build unit

package query

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
)

// mockScheduleRepo implements configPorts.ScheduleRepository for query tests.
type mockScheduleRepo struct {
	findByIDFn        func(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error)
	findByContextIDFn func(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error)
}

var _ configPorts.ScheduleRepository = (*mockScheduleRepo)(nil)

func (m *mockScheduleRepo) Create(_ context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	return s, nil
}

func (m *mockScheduleRepo) FindByID(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	return nil, sql.ErrNoRows
}

func (m *mockScheduleRepo) FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error) {
	if m.findByContextIDFn != nil {
		return m.findByContextIDFn(ctx, contextID)
	}

	return nil, nil
}

func (m *mockScheduleRepo) FindDueSchedules(_ context.Context, _ time.Time) ([]*entities.ReconciliationSchedule, error) {
	return nil, nil
}

func (m *mockScheduleRepo) Update(_ context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	return s, nil
}

func (m *mockScheduleRepo) Delete(_ context.Context, _ uuid.UUID) error {
	return nil
}

func newQueryUseCaseWithScheduleRepo(t *testing.T, scheduleRepo configPorts.ScheduleRepository) *UseCase {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(func() { ctrl.Finish() })

	uc, err := NewUseCase(
		mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
		WithScheduleRepository(scheduleRepo),
	)
	require.NoError(t, err)

	return uc
}

// --- WithScheduleRepository tests ---

func TestWithScheduleRepository_SetsRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := &mockScheduleRepo{}

	uc, err := NewUseCase(
		mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
		WithScheduleRepository(repo),
	)

	require.NoError(t, err)
	assert.NotNil(t, uc.scheduleRepo)
}

func TestWithScheduleRepository_NilIgnored(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uc, err := NewUseCase(
		mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
		WithScheduleRepository(nil),
	)

	require.NoError(t, err)
	assert.Nil(t, uc.scheduleRepo)
}

// --- GetSchedule tests ---

func TestGetSchedule_NilRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uc, err := NewUseCase(
		mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
	)
	require.NoError(t, err)

	_, err = uc.GetSchedule(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrNilScheduleRepository)
}

func TestGetSchedule_NotFound(t *testing.T) {
	t.Parallel()

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return nil, sql.ErrNoRows
		},
	}

	uc := newQueryUseCaseWithScheduleRepo(t, scheduleRepo)

	_, err := uc.GetSchedule(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrScheduleNotFound)
}

func TestGetSchedule_Success(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	expected := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      uuid.New(),
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error) {
			assert.Equal(t, scheduleID, id)
			return expected, nil
		},
	}

	uc := newQueryUseCaseWithScheduleRepo(t, scheduleRepo)

	result, err := uc.GetSchedule(context.Background(), scheduleID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, expected, result)
}

func TestGetSchedule_Error(t *testing.T) {
	t.Parallel()

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return nil, errDBError
		},
	}

	uc := newQueryUseCaseWithScheduleRepo(t, scheduleRepo)

	_, err := uc.GetSchedule(context.Background(), uuid.New())

	require.ErrorIs(t, err, errDBError)
}

// --- Sentinel errors ---

func TestScheduleQuerySentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrNilScheduleRepository,
		ErrScheduleNotFound,
	}

	for i := range errs {
		for j := i + 1; j < len(errs); j++ {
			assert.NotEqual(t, errs[i].Error(), errs[j].Error(),
				"sentinel errors must have distinct messages: %q vs %q", errs[i], errs[j])
		}
	}
}
