//go:build unit

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

type feeScheduleRepoStub struct {
	createFn   func(context.Context, *fee.FeeSchedule) (*fee.FeeSchedule, error)
	getByIDFn  func(context.Context, uuid.UUID) (*fee.FeeSchedule, error)
	updateFn   func(context.Context, *fee.FeeSchedule) (*fee.FeeSchedule, error)
	deleteFn   func(context.Context, uuid.UUID) error
	listFn     func(context.Context, int) ([]*fee.FeeSchedule, error)
	getByIDsFn func(context.Context, []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error)
}

func (stub *feeScheduleRepoStub) Create(ctx context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, s)
	}

	return nil, errors.New("create not implemented")
}

func (stub *feeScheduleRepoStub) GetByID(ctx context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
	if stub.getByIDFn != nil {
		return stub.getByIDFn(ctx, id)
	}

	return nil, errors.New("getByID not implemented")
}

func (stub *feeScheduleRepoStub) Update(ctx context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, s)
	}

	return nil, errors.New("update not implemented")
}

func (stub *feeScheduleRepoStub) Delete(ctx context.Context, id uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, id)
	}

	return errors.New("delete not implemented")
}

func (stub *feeScheduleRepoStub) List(ctx context.Context, limit int) ([]*fee.FeeSchedule, error) {
	if stub.listFn != nil {
		return stub.listFn(ctx, limit)
	}

	return nil, errors.New("list not implemented")
}

func (stub *feeScheduleRepoStub) GetByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
	if stub.getByIDsFn != nil {
		return stub.getByIDsFn(ctx, ids)
	}

	return nil, errors.New("getByIDs not implemented")
}

type contextRepoStubQ struct{}

func (stub *contextRepoStubQ) Create(_ context.Context, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
	return nil, errors.New("not implemented")
}

func (stub *contextRepoStubQ) FindByID(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
	return nil, errors.New("not implemented")
}

func (stub *contextRepoStubQ) FindByName(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
	return nil, errors.New("not implemented")
}

func (stub *contextRepoStubQ) FindAll(_ context.Context, _ string, _ int, _ *shared.ContextType, _ *value_objects.ContextStatus) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errors.New("not implemented")
}

func (stub *contextRepoStubQ) Update(_ context.Context, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
	return nil, errors.New("not implemented")
}

func (stub *contextRepoStubQ) Delete(_ context.Context, _ uuid.UUID) error {
	return errors.New("not implemented")
}

func (stub *contextRepoStubQ) Count(_ context.Context) (int64, error) {
	return 0, errors.New("not implemented")
}

type sourceRepoStubQ struct{}

func (stub *sourceRepoStubQ) Create(_ context.Context, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
	return nil, errors.New("not implemented")
}

func (stub *sourceRepoStubQ) FindByID(_ context.Context, _, _ uuid.UUID) (*entities.ReconciliationSource, error) {
	return nil, errors.New("not implemented")
}

func (stub *sourceRepoStubQ) FindByContextID(_ context.Context, _ uuid.UUID, _ string, _ int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errors.New("not implemented")
}

func (stub *sourceRepoStubQ) FindByContextIDAndType(_ context.Context, _ uuid.UUID, _ value_objects.SourceType, _ string, _ int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errors.New("not implemented")
}

func (stub *sourceRepoStubQ) Update(_ context.Context, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
	return nil, errors.New("not implemented")
}

func (stub *sourceRepoStubQ) Delete(_ context.Context, _, _ uuid.UUID) error {
	return errors.New("not implemented")
}

type fieldMapRepoStubQ struct{}

func (stub *fieldMapRepoStubQ) Create(_ context.Context, _ *entities.FieldMap) (*entities.FieldMap, error) {
	return nil, errors.New("not implemented")
}

func (stub *fieldMapRepoStubQ) FindByID(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
	return nil, errors.New("not implemented")
}

func (stub *fieldMapRepoStubQ) FindBySourceID(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
	return nil, errors.New("not implemented")
}

func (stub *fieldMapRepoStubQ) Update(_ context.Context, _ *entities.FieldMap) (*entities.FieldMap, error) {
	return nil, errors.New("not implemented")
}

func (stub *fieldMapRepoStubQ) Delete(_ context.Context, _ uuid.UUID) error {
	return errors.New("not implemented")
}

func (stub *fieldMapRepoStubQ) ExistsBySourceIDs(_ context.Context, sourceIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	return make(map[uuid.UUID]bool, len(sourceIDs)), nil
}

type matchRuleRepoStubQ struct{}

func (stub *matchRuleRepoStubQ) Create(_ context.Context, _ *entities.MatchRule) (*entities.MatchRule, error) {
	return nil, errors.New("not implemented")
}

func (stub *matchRuleRepoStubQ) FindByID(_ context.Context, _, _ uuid.UUID) (*entities.MatchRule, error) {
	return nil, errors.New("not implemented")
}

func (stub *matchRuleRepoStubQ) FindByContextID(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errors.New("not implemented")
}

func (stub *matchRuleRepoStubQ) FindByContextIDAndType(_ context.Context, _ uuid.UUID, _ shared.RuleType, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errors.New("not implemented")
}

func (stub *matchRuleRepoStubQ) FindByPriority(_ context.Context, _ uuid.UUID, _ int) (*entities.MatchRule, error) {
	return nil, errors.New("not implemented")
}

func (stub *matchRuleRepoStubQ) Update(_ context.Context, _ *entities.MatchRule) (*entities.MatchRule, error) {
	return nil, errors.New("not implemented")
}

func (stub *matchRuleRepoStubQ) Delete(_ context.Context, _, _ uuid.UUID) error {
	return errors.New("not implemented")
}

func (stub *matchRuleRepoStubQ) ReorderPriorities(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return errors.New("not implemented")
}

func newQueryUseCaseWithFeeSchedule(t *testing.T, feeRepo *feeScheduleRepoStub) *UseCase {
	t.Helper()

	uc, err := NewUseCase(
		&contextRepoStubQ{},
		&sourceRepoStubQ{},
		&fieldMapRepoStubQ{},
		&matchRuleRepoStubQ{},
		WithFeeScheduleRepository(feeRepo),
	)
	require.NoError(t, err)

	return uc
}

func TestGetFeeSchedule_Success(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	expected := &fee.FeeSchedule{
		ID:   scheduleID,
		Name: "Test Schedule",
	}

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
			if id == scheduleID {
				return expected, nil
			}

			return nil, errors.New("not found")
		},
	}

	uc := newQueryUseCaseWithFeeSchedule(t, repo)

	result, err := uc.GetFeeSchedule(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, "Test Schedule", result.Name)
}

func TestGetFeeSchedule_NilRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&contextRepoStubQ{},
		&sourceRepoStubQ{},
		&fieldMapRepoStubQ{},
		&matchRuleRepoStubQ{},
	)
	require.NoError(t, err)

	_, err = uc.GetFeeSchedule(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

func TestGetFeeSchedule_NotFound(t *testing.T) {
	t.Parallel()

	findErr := errors.New("not found")
	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return nil, findErr
		},
	}

	uc := newQueryUseCaseWithFeeSchedule(t, repo)

	_, err := uc.GetFeeSchedule(context.Background(), uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, findErr)
}

func TestListFeeSchedules_Success(t *testing.T) {
	t.Parallel()

	expected := []*fee.FeeSchedule{
		{ID: uuid.New(), Name: "Schedule 1"},
		{ID: uuid.New(), Name: "Schedule 2"},
	}

	repo := &feeScheduleRepoStub{
		listFn: func(_ context.Context, _ int) ([]*fee.FeeSchedule, error) {
			return expected, nil
		},
	}

	uc := newQueryUseCaseWithFeeSchedule(t, repo)

	result, err := uc.ListFeeSchedules(context.Background(), 100)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestListFeeSchedules_NilRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&contextRepoStubQ{},
		&sourceRepoStubQ{},
		&fieldMapRepoStubQ{},
		&matchRuleRepoStubQ{},
	)
	require.NoError(t, err)

	_, err = uc.ListFeeSchedules(context.Background(), 100)
	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

func TestListFeeSchedules_RepoError(t *testing.T) {
	t.Parallel()

	repoErr := errors.New("db error")
	repo := &feeScheduleRepoStub{
		listFn: func(_ context.Context, _ int) ([]*fee.FeeSchedule, error) {
			return nil, repoErr
		},
	}

	uc := newQueryUseCaseWithFeeSchedule(t, repo)

	_, err := uc.ListFeeSchedules(context.Background(), 100)
	require.Error(t, err)
	require.ErrorIs(t, err, repoErr)
}
