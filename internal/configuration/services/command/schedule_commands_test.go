//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// --- stub mocks ---

// mockScheduleRepo implements configPorts.ScheduleRepository.
type mockScheduleRepo struct {
	createFn          func(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	findByIDFn        func(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error)
	findByContextIDFn func(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error)
	findDueScheduleFn func(ctx context.Context, now time.Time) ([]*entities.ReconciliationSchedule, error)
	updateFn          func(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	deleteFn          func(ctx context.Context, id uuid.UUID) error
}

var _ configPorts.ScheduleRepository = (*mockScheduleRepo)(nil)

func (m *mockScheduleRepo) Create(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	if m.createFn != nil {
		return m.createFn(ctx, s)
	}

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

func (m *mockScheduleRepo) FindDueSchedules(ctx context.Context, now time.Time) ([]*entities.ReconciliationSchedule, error) {
	if m.findDueScheduleFn != nil {
		return m.findDueScheduleFn(ctx, now)
	}

	return nil, nil
}

func (m *mockScheduleRepo) Update(ctx context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, s)
	}

	return s, nil
}

func (m *mockScheduleRepo) Delete(ctx context.Context, id uuid.UUID) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}

	return nil
}

// mockCtxRepo implements repositories.ContextRepository for schedule tests.
type mockCtxRepo struct {
	findByIDFn func(ctx context.Context, id uuid.UUID) (*entities.ReconciliationContext, error)
}

var _ repositories.ContextRepository = (*mockCtxRepo)(nil)

func (m *mockCtxRepo) Create(_ context.Context, e *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
	return e, nil
}

func (m *mockCtxRepo) FindByID(ctx context.Context, id uuid.UUID) (*entities.ReconciliationContext, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	return nil, sql.ErrNoRows
}

func (m *mockCtxRepo) FindByName(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
	return nil, sql.ErrNoRows
}

func (m *mockCtxRepo) FindAll(
	_ context.Context,
	_ string,
	_ int,
	_ *shared.ContextType,
	_ *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockCtxRepo) Update(_ context.Context, e *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
	return e, nil
}

func (m *mockCtxRepo) Delete(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (m *mockCtxRepo) Count(_ context.Context) (int64, error) {
	return 0, nil
}

// stubSourceRepo implements repositories.SourceRepository.
type stubSourceRepo struct{}

var _ repositories.SourceRepository = (*stubSourceRepo)(nil)

func (s *stubSourceRepo) Create(_ context.Context, e *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
	return e, nil
}

func (s *stubSourceRepo) FindByID(_ context.Context, _, _ uuid.UUID) (*entities.ReconciliationSource, error) {
	return nil, sql.ErrNoRows
}

func (s *stubSourceRepo) FindByContextID(_ context.Context, _ uuid.UUID, _ string, _ int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubSourceRepo) FindByContextIDAndType(_ context.Context, _ uuid.UUID, _ value_objects.SourceType, _ string, _ int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubSourceRepo) Update(_ context.Context, e *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
	return e, nil
}

func (s *stubSourceRepo) Delete(_ context.Context, _, _ uuid.UUID) error {
	return nil
}

// stubFieldMapRepo implements repositories.FieldMapRepository.
type stubFieldMapRepo struct{}

var _ repositories.FieldMapRepository = (*stubFieldMapRepo)(nil)

func (s *stubFieldMapRepo) Create(_ context.Context, e *shared.FieldMap) (*shared.FieldMap, error) {
	return e, nil
}

func (s *stubFieldMapRepo) FindByID(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
	return nil, sql.ErrNoRows
}

func (s *stubFieldMapRepo) FindBySourceID(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
	return nil, sql.ErrNoRows
}

func (s *stubFieldMapRepo) ExistsBySourceIDs(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]bool, error) {
	return nil, nil
}

func (s *stubFieldMapRepo) Update(_ context.Context, e *shared.FieldMap) (*shared.FieldMap, error) {
	return e, nil
}

func (s *stubFieldMapRepo) Delete(_ context.Context, _ uuid.UUID) error {
	return nil
}

// stubMatchRuleRepo implements repositories.MatchRuleRepository.
type stubMatchRuleRepo struct{}

var _ repositories.MatchRuleRepository = (*stubMatchRuleRepo)(nil)

func (s *stubMatchRuleRepo) Create(_ context.Context, e *entities.MatchRule) (*entities.MatchRule, error) {
	return e, nil
}

func (s *stubMatchRuleRepo) FindByID(_ context.Context, _, _ uuid.UUID) (*entities.MatchRule, error) {
	return nil, sql.ErrNoRows
}

func (s *stubMatchRuleRepo) FindByContextID(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubMatchRuleRepo) FindByContextIDAndType(_ context.Context, _ uuid.UUID, _ shared.RuleType, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubMatchRuleRepo) FindByPriority(_ context.Context, _ uuid.UUID, _ int) (*entities.MatchRule, error) {
	return nil, sql.ErrNoRows
}

func (s *stubMatchRuleRepo) Update(_ context.Context, e *entities.MatchRule) (*entities.MatchRule, error) {
	return e, nil
}

func (s *stubMatchRuleRepo) Delete(_ context.Context, _, _ uuid.UUID) error {
	return nil
}

func (s *stubMatchRuleRepo) ReorderPriorities(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return nil
}

// --- helpers ---

// newUseCaseWithScheduleRepo creates a UseCase wired with a mockCtxRepo and a mockScheduleRepo.
// The context repository is configured to return a valid context for any FindByID call.
func newUseCaseWithScheduleRepo(scheduleRepo configPorts.ScheduleRepository) *UseCase {
	ctxRepo := &mockCtxRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return &entities.ReconciliationContext{
				ID:       testutil.DeterministicUUID("test-context-id"),
				TenantID: testutil.DeterministicUUID("test-tenant-id"),
				Name:     "Test Context",
				Type:     shared.ContextType("1:1"),
				Interval: "daily",
				Status:   value_objects.ContextStatusActive,
			}, nil
		},
	}

	uc, _ := NewUseCase(
		ctxRepo,
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
		WithScheduleRepository(scheduleRepo),
	)

	return uc
}

// newUseCaseWithCtxAndSchedule creates a UseCase with customizable context and schedule repos.
func newUseCaseWithCtxAndSchedule(
	ctxRepo repositories.ContextRepository,
	scheduleRepo configPorts.ScheduleRepository,
) *UseCase {
	uc, _ := NewUseCase(
		ctxRepo,
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
		WithScheduleRepository(scheduleRepo),
	)

	return uc
}

// validCronExpr is a valid 5-field cron expression for tests (every midnight).
const validCronExpr = "0 0 * * *"

// --- WithScheduleRepository tests ---

func TestWithScheduleRepository_SetsRepo(t *testing.T) {
	t.Parallel()

	repo := &mockScheduleRepo{}

	uc, err := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
		WithScheduleRepository(repo),
	)

	require.NoError(t, err)
	assert.Equal(t, repo, uc.scheduleRepo)
}

func TestWithScheduleRepository_NilIgnored(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockCtxRepo{},
		&stubSourceRepo{},
		&stubFieldMapRepo{},
		&stubMatchRuleRepo{},
		WithScheduleRepository(nil),
	)

	require.NoError(t, err)
	assert.Nil(t, uc.scheduleRepo)
}

// --- CreateSchedule tests ---

func TestCreateSchedule_NilRepository(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: &mockCtxRepo{},
	}

	_, err := uc.CreateSchedule(context.Background(), uuid.New(), entities.CreateScheduleInput{
		CronExpression: validCronExpr,
	})

	require.ErrorIs(t, err, ErrNilScheduleRepository)
}

func TestCreateSchedule_Success(t *testing.T) {
	t.Parallel()

	var persisted *entities.ReconciliationSchedule

	scheduleRepo := &mockScheduleRepo{
		createFn: func(_ context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			persisted = s
			return s, nil
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)
	contextID := uuid.New()

	result, err := uc.CreateSchedule(context.Background(), contextID, entities.CreateScheduleInput{
		CronExpression: validCronExpr,
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, validCronExpr, result.CronExpression)
	assert.True(t, result.Enabled)
	assert.NotNil(t, result.NextRunAt)
	assert.Equal(t, persisted, result)
}

func TestCreateSchedule_ContextNotFound(t *testing.T) {
	t.Parallel()

	ctxRepo := &mockCtxRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return nil, sql.ErrNoRows
		},
	}

	scheduleRepo := &mockScheduleRepo{}
	uc := newUseCaseWithCtxAndSchedule(ctxRepo, scheduleRepo)

	_, err := uc.CreateSchedule(context.Background(), uuid.New(), entities.CreateScheduleInput{
		CronExpression: validCronExpr,
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
	assert.Contains(t, err.Error(), "context not found")
}

func TestCreateSchedule_InvalidCron(t *testing.T) {
	t.Parallel()

	uc := newUseCaseWithScheduleRepo(&mockScheduleRepo{})

	_, err := uc.CreateSchedule(context.Background(), uuid.New(), entities.CreateScheduleInput{
		CronExpression: "not-a-cron",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrScheduleCronExpressionInvalid)
}

// --- UpdateSchedule tests ---

func TestUpdateSchedule_NilRepository(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, err := uc.UpdateSchedule(context.Background(), uuid.New(), uuid.New(), entities.UpdateScheduleInput{})

	require.ErrorIs(t, err, ErrNilScheduleRepository)
}

func TestUpdateSchedule_NotFound(t *testing.T) {
	t.Parallel()

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return nil, sql.ErrNoRows
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	_, err := uc.UpdateSchedule(context.Background(), uuid.New(), uuid.New(), entities.UpdateScheduleInput{})

	require.ErrorIs(t, err, ErrScheduleNotFound)
}

func TestUpdateSchedule_ContextMismatch(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	existing := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      uuid.New(),
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      time.Now().UTC().Add(-time.Hour),
		UpdatedAt:      time.Now().UTC().Add(-time.Hour),
	}

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return existing, nil
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	wrongContextID := uuid.New()
	_, err := uc.UpdateSchedule(context.Background(), wrongContextID, scheduleID, entities.UpdateScheduleInput{})

	require.ErrorIs(t, err, ErrScheduleContextMismatch)
}

func TestUpdateSchedule_Success(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	contextID := uuid.New()
	existing := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      contextID,
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      time.Now().UTC().Add(-time.Hour),
		UpdatedAt:      time.Now().UTC().Add(-time.Hour),
	}

	var updated *entities.ReconciliationSchedule

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error) {
			assert.Equal(t, scheduleID, id)
			return existing, nil
		},
		updateFn: func(_ context.Context, s *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			updated = s
			return s, nil
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	newCron := "0 6 * * *"
	result, err := uc.UpdateSchedule(context.Background(), contextID, scheduleID, entities.UpdateScheduleInput{
		CronExpression: &newCron,
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "0 6 * * *", result.CronExpression)
	assert.Equal(t, updated, result)
}

// --- DeleteSchedule tests ---

func TestDeleteSchedule_NilRepository(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	err := uc.DeleteSchedule(context.Background(), uuid.New(), uuid.New())

	require.ErrorIs(t, err, ErrNilScheduleRepository)
}

func TestDeleteSchedule_NotFound(t *testing.T) {
	t.Parallel()

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return nil, sql.ErrNoRows
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	err := uc.DeleteSchedule(context.Background(), uuid.New(), uuid.New())

	require.ErrorIs(t, err, ErrScheduleNotFound)
}

func TestDeleteSchedule_ContextMismatch(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	existing := &entities.ReconciliationSchedule{
		ID:        scheduleID,
		ContextID: uuid.New(),
	}

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return existing, nil
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	wrongContextID := uuid.New()
	err := uc.DeleteSchedule(context.Background(), wrongContextID, scheduleID)

	require.ErrorIs(t, err, ErrScheduleContextMismatch)
}

func TestDeleteSchedule_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	targetID := uuid.New()
	deletedID := uuid.Nil

	existing := &entities.ReconciliationSchedule{
		ID:        targetID,
		ContextID: contextID,
	}

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return existing, nil
		},
		deleteFn: func(_ context.Context, id uuid.UUID) error {
			deletedID = id
			return nil
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	err := uc.DeleteSchedule(context.Background(), contextID, targetID)

	require.NoError(t, err)
	assert.Equal(t, targetID, deletedID)
}

// --- Sentinel errors ---

func TestScheduleSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrNilScheduleRepository,
		ErrScheduleNotFound,
		ErrScheduleContextMismatch,
	}

	for i := range errs {
		for j := i + 1; j < len(errs); j++ {
			assert.NotEqual(t, errs[i].Error(), errs[j].Error(),
				"sentinel errors must have distinct messages: %q vs %q", errs[i], errs[j])
		}
	}
}
