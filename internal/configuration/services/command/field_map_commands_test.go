//go:build unit

package command

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	repoMocks "github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// errFieldMapNotFound is a sentinel error for field map not found in tests.
var errFieldMapNotFound = errors.New("field map not found")

func TestUpdateFieldMap_CommandValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	createFieldMap := func(t *testing.T) *shared.FieldMap {
		t.Helper()

		fieldMap, err := shared.NewFieldMap(
			ctx,
			contextID,
			sourceID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{"amount": "txn_amount"},
			},
		)
		require.NoError(t, err)

		return fieldMap
	}

	tests := []struct {
		name       string
		mapping    map[string]any
		findByErr  error
		wantErr    error
		wantMapped bool
	}{
		{
			name:    "empty mapping",
			mapping: map[string]any{},
			wantErr: shared.ErrFieldMapMappingRequired,
		},
		{
			name:    "nil mapping",
			mapping: nil,
			wantErr: shared.ErrFieldMapMappingRequired,
		},
		{
			name:       "valid mapping",
			mapping:    map[string]any{"amount": "new_amount"},
			wantMapped: true,
		},
		{
			name:      "not found",
			mapping:   map[string]any{"amount": "new_amount"},
			findByErr: errFieldMapNotFound,
			wantErr:   errFieldMapNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fieldMap := createFieldMap(t)
			repo := &fieldMapRepoStub{
				findByIDFn: func(ctx context.Context, identifier uuid.UUID) (*shared.FieldMap, error) {
					if tt.findByErr != nil {
						return nil, tt.findByErr
					}

					return fieldMap, nil
				},
				updateFn: func(ctx context.Context, entity *shared.FieldMap) (*shared.FieldMap, error) {
					return entity, nil
				},
			}
			useCase, err := NewUseCase(
				&contextRepoStub{},
				&sourceRepoStub{},
				repo,
				&matchRuleRepoStub{},
			)
			require.NoError(t, err)

			updated, err := useCase.UpdateFieldMap(
				context.Background(),
				fieldMap.ID,
				shared.UpdateFieldMapInput{Mapping: tt.mapping},
			)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr, "expected error %v, got %v", tt.wantErr, err)

				return
			}

			require.NoError(t, err)

			if tt.wantMapped {
				assert.Equal(t, tt.mapping, updated.Mapping)
			}
		})
	}
}

func TestCreateFieldMap_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.CreateFieldMap(
		context.Background(),
		uuid.New(),
		uuid.New(),
		shared.CreateFieldMapInput{},
	)
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestCreateFieldMap_NilFieldMapRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  nil,
		matchRuleRepo: &matchRuleRepoStub{},
	}

	_, err := uc.CreateFieldMap(
		context.Background(),
		uuid.New(),
		uuid.New(),
		shared.CreateFieldMapInput{},
	)
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestCreateFieldMap_InvalidInput_NilContextID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	_, err = uc.CreateFieldMap(
		context.Background(),
		uuid.Nil,
		uuid.New(),
		shared.CreateFieldMapInput{
			Mapping: map[string]any{"amount": "txn_amount"},
		},
	)
	require.ErrorIs(t, err, shared.ErrFieldMapContextRequired)
}

func TestCreateFieldMap_InvalidInput_NilSourceID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	_, err = uc.CreateFieldMap(
		context.Background(),
		uuid.New(),
		uuid.Nil,
		shared.CreateFieldMapInput{
			Mapping: map[string]any{"amount": "txn_amount"},
		},
	)
	require.ErrorIs(t, err, shared.ErrFieldMapSourceRequired)
}

func TestCreateFieldMap_InvalidInput_EmptyMapping(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	_, err = uc.CreateFieldMap(
		context.Background(),
		uuid.New(),
		uuid.New(),
		shared.CreateFieldMapInput{
			Mapping: map[string]any{},
		},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "mapping")
}

func TestCreateFieldMap_RepositoryError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()
	createErr := errors.New("database error")

	mockFmRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		Return(nil, createErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	_, err = uc.CreateFieldMap(
		context.Background(),
		contextID,
		sourceID,
		shared.CreateFieldMapInput{
			Mapping: map[string]any{"amount": "txn_amount"},
		},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "creating field map")
}

func TestCreateFieldMap_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()
	input := shared.CreateFieldMapInput{
		Mapping: map[string]any{"amount": "txn_amount"},
	}

	mockFmRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *shared.FieldMap) (*shared.FieldMap, error) {
			return entity, nil
		})

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	result, err := uc.CreateFieldMap(context.Background(), contextID, sourceID, input)
	require.NoError(t, err)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, sourceID, result.SourceID)
	assert.Equal(t, input.Mapping, result.Mapping)
}

func TestCreateFieldMap_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()
	input := shared.CreateFieldMapInput{
		Mapping: map[string]any{"amount": "txn_amount"},
	}

	mockFmRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *shared.FieldMap) (*shared.FieldMap, error) {
			return entity, nil
		})

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	result, err := uc.CreateFieldMap(context.Background(), contextID, sourceID, input)
	require.NoError(t, err)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, sourceID, result.SourceID)
}

func TestUpdateFieldMap_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.UpdateFieldMap(context.Background(), uuid.New(), shared.UpdateFieldMapInput{})
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestUpdateFieldMap_NilFieldMapRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  nil,
		matchRuleRepo: &matchRuleRepoStub{},
	}

	_, err := uc.UpdateFieldMap(context.Background(), uuid.New(), shared.UpdateFieldMapInput{})
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestUpdateFieldMap_RepositoryUpdateError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()
	existing, err := shared.NewFieldMap(
		context.Background(),
		contextID,
		sourceID,
		shared.CreateFieldMapInput{
			Mapping: map[string]any{"amount": "txn_amount"},
		},
	)
	require.NoError(t, err)

	updateErr := errors.New("update failed")

	mockFmRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockFmRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil, updateErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	_, err = uc.UpdateFieldMap(context.Background(), existing.ID, shared.UpdateFieldMapInput{
		Mapping: map[string]any{"amount": "new_amount"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "updating field map")
}

func TestUpdateFieldMap_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()
	existing, err := shared.NewFieldMap(
		context.Background(),
		contextID,
		sourceID,
		shared.CreateFieldMapInput{
			Mapping: map[string]any{"amount": "txn_amount"},
		},
	)
	require.NoError(t, err)

	mockFmRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockFmRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *shared.FieldMap) (*shared.FieldMap, error) {
			return entity, nil
		})

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	result, err := uc.UpdateFieldMap(
		context.Background(),
		existing.ID,
		shared.UpdateFieldMapInput{
			Mapping: map[string]any{"amount": "new_amount"},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"amount": "new_amount"}, result.Mapping)
}

func TestDeleteFieldMap_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	err := uc.DeleteFieldMap(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestDeleteFieldMap_NilFieldMapRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  nil,
		matchRuleRepo: &matchRuleRepoStub{},
	}

	err := uc.DeleteFieldMap(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestDeleteFieldMap_DeleteError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	fieldMapID := uuid.New()
	deleteErr := errors.New("delete failed")

	mockFmRepo.EXPECT().
		Delete(gomock.Any(), fieldMapID).
		Return(deleteErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteFieldMap(context.Background(), fieldMapID)
	require.Error(t, err)
	require.ErrorContains(t, err, "deleting field map")
}

func TestDeleteFieldMap_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	fieldMapID := uuid.New()

	mockFmRepo.EXPECT().
		Delete(gomock.Any(), fieldMapID).
		Return(nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteFieldMap(context.Background(), fieldMapID)
	require.NoError(t, err)
}

func TestDeleteFieldMap_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	fieldMapID := uuid.New()

	mockFmRepo.EXPECT().
		Delete(gomock.Any(), fieldMapID).
		Return(nil)

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	err = uc.DeleteFieldMap(context.Background(), fieldMapID)
	require.NoError(t, err)
}
