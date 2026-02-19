//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
)

// errDatabaseError is a sentinel error for database errors in tests.
var errDatabaseError = errors.New("database error")

func TestCreateSource_Command(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	tests := []struct {
		name      string
		input     entities.CreateReconciliationSourceInput
		createErr error
		wantErr   error
	}{
		{
			name: "valid input",
			input: entities.CreateReconciliationSourceInput{
				Name: "Bank",
				Type: value_objects.SourceTypeBank,
				Config: map[string]any{
					"account": "123",
				},
			},
		},
		{
			name: "repository fails",
			input: entities.CreateReconciliationSourceInput{
				Name: "Bank",
				Type: value_objects.SourceTypeBank,
			},
			createErr: errDatabaseError,
			wantErr:   errDatabaseError,
		},
		{
			name: "empty name",
			input: entities.CreateReconciliationSourceInput{
				Name: "",
				Type: value_objects.SourceTypeBank,
			},
			wantErr: entities.ErrSourceNameRequired,
		},
		{
			name: "invalid type",
			input: entities.CreateReconciliationSourceInput{
				Name: "Bank",
				Type: value_objects.SourceType("INVALID"),
			},
			wantErr: entities.ErrSourceTypeInvalid,
		},
		{
			name: "name too long",
			input: entities.CreateReconciliationSourceInput{
				Name: strings.Repeat("a", 51),
				Type: value_objects.SourceTypeBank,
			},
			wantErr: entities.ErrSourceNameTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &sourceRepoStub{
				createFn: func(ctx context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
					if tt.createErr != nil {
						return nil, tt.createErr
					}

					return entity, nil
				},
			}
			useCase, err := NewUseCase(
				&contextRepoStub{},
				repo,
				&fieldMapRepoStub{},
				&matchRuleRepoStub{},
			)
			require.NoError(t, err)

			source, err := useCase.CreateSource(context.Background(), contextID, tt.input)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr, "expected error %v, got %v", tt.wantErr, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, contextID, source.ContextID)
			assert.Equal(t, tt.input.Type, source.Type)
		})
	}
}

func TestUpdateSource_CommandValidation(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ctx := context.Background()
	existing, err := entities.NewReconciliationSource(
		ctx,
		contextID,
		entities.CreateReconciliationSourceInput{
			Name: "Gateway",
			Type: value_objects.SourceTypeGateway,
		},
	)
	require.NoError(t, err)

	repo := &sourceRepoStub{
		findByIDFn: func(ctx context.Context, contextIDValue, identifier uuid.UUID) (*entities.ReconciliationSource, error) {
			return existing, nil
		},
		updateFn: func(ctx context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(&contextRepoStub{}, repo, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	invalidType := value_objects.SourceType("INVALID")
	_, err = useCase.UpdateSource(
		context.Background(),
		contextID,
		existing.ID,
		entities.UpdateReconciliationSourceInput{Type: &invalidType},
	)
	require.Error(t, err)
	assert.Equal(t, entities.ErrSourceTypeInvalid, err)
}

func TestCreateSource_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.CreateSource(
		context.Background(),
		uuid.New(),
		entities.CreateReconciliationSourceInput{},
	)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestCreateSource_NilSourceRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:  &contextRepoStub{},
		sourceRepo:   nil,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	_, err := uc.CreateSource(
		context.Background(),
		uuid.New(),
		entities.CreateReconciliationSourceInput{},
	)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestCreateSource_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	contextID := uuid.New()
	input := entities.CreateReconciliationSourceInput{
		Name: "Bank",
		Type: value_objects.SourceTypeBank,
	}

	mockSrcRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
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

	result, err := uc.CreateSource(context.Background(), contextID, input)
	require.NoError(t, err)
	assert.Equal(t, input.Name, result.Name)
	assert.Equal(t, input.Type, result.Type)
}

func TestUpdateSource_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.UpdateSource(
		context.Background(),
		uuid.New(),
		uuid.New(),
		entities.UpdateReconciliationSourceInput{},
	)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestUpdateSource_NilSourceRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:  &contextRepoStub{},
		sourceRepo:   nil,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	_, err := uc.UpdateSource(
		context.Background(),
		uuid.New(),
		uuid.New(),
		entities.UpdateReconciliationSourceInput{},
	)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestUpdateSource_FindByIDError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()
	findErr := errors.New("source not found")

	mockSrcRepo.EXPECT().
		FindByID(gomock.Any(), contextID, sourceID).
		Return(nil, findErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	_, err = uc.UpdateSource(
		context.Background(),
		contextID,
		sourceID,
		entities.UpdateReconciliationSourceInput{},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "finding reconciliation source")
}

func TestUpdateSource_RepositoryUpdateError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	existing, err := entities.NewReconciliationSource(
		context.Background(),
		contextID,
		entities.CreateReconciliationSourceInput{
			Name: "Original",
			Type: value_objects.SourceTypeBank,
		},
	)
	require.NoError(t, err)

	updateErr := errors.New("update failed")

	mockSrcRepo.EXPECT().
		FindByID(gomock.Any(), contextID, existing.ID).
		Return(existing, nil)

	mockSrcRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil, updateErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	newName := "Updated Name"
	_, err = uc.UpdateSource(
		context.Background(),
		contextID,
		existing.ID,
		entities.UpdateReconciliationSourceInput{Name: &newName},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "updating reconciliation source")
}

func TestUpdateSource_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	existing, err := entities.NewReconciliationSource(
		context.Background(),
		contextID,
		entities.CreateReconciliationSourceInput{
			Name: "Original",
			Type: value_objects.SourceTypeBank,
		},
	)
	require.NoError(t, err)

	mockSrcRepo.EXPECT().
		FindByID(gomock.Any(), contextID, existing.ID).
		Return(existing, nil)

	mockSrcRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		})

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	newName := "Updated Name"
	result, err := uc.UpdateSource(
		context.Background(),
		contextID,
		existing.ID,
		entities.UpdateReconciliationSourceInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, result.Name)
}

func TestUpdateSource_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	contextID := uuid.New()
	existing, err := entities.NewReconciliationSource(
		context.Background(),
		contextID,
		entities.CreateReconciliationSourceInput{
			Name: "Original",
			Type: value_objects.SourceTypeBank,
		},
	)
	require.NoError(t, err)

	mockSrcRepo.EXPECT().
		FindByID(gomock.Any(), contextID, existing.ID).
		Return(existing, nil)

	mockSrcRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
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

	newName := "Updated Name"
	result, err := uc.UpdateSource(
		context.Background(),
		contextID,
		existing.ID,
		entities.UpdateReconciliationSourceInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, result.Name)
}

func TestDeleteSource_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	err := uc.DeleteSource(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestDeleteSource_NilSourceRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:  &contextRepoStub{},
		sourceRepo:   nil,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	err := uc.DeleteSource(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestDeleteSource_DeleteError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()
	deleteErr := errors.New("delete failed")

	mockFmRepo.EXPECT().
		FindBySourceID(gomock.Any(), sourceID).
		Return(nil, sql.ErrNoRows)

	mockSrcRepo.EXPECT().
		Delete(gomock.Any(), contextID, sourceID).
		Return(deleteErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteSource(context.Background(), contextID, sourceID)
	require.Error(t, err)
	require.ErrorContains(t, err, "deleting reconciliation source")
}

func TestDeleteSource_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()

	mockFmRepo.EXPECT().
		FindBySourceID(gomock.Any(), sourceID).
		Return(nil, sql.ErrNoRows)

	mockSrcRepo.EXPECT().
		Delete(gomock.Any(), contextID, sourceID).
		Return(nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteSource(context.Background(), contextID, sourceID)
	require.NoError(t, err)
}

func TestDeleteSource_WithAuditPublisher(t *testing.T) {
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

	mockFmRepo.EXPECT().
		FindBySourceID(gomock.Any(), sourceID).
		Return(nil, sql.ErrNoRows)

	mockSrcRepo.EXPECT().
		Delete(gomock.Any(), contextID, sourceID).
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

	err = uc.DeleteSource(context.Background(), contextID, sourceID)
	require.NoError(t, err)
}

func TestDeleteSource_BlockedByFieldMap(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()

	existingFieldMap := &entities.FieldMap{ID: uuid.New()}
	mockFmRepo.EXPECT().
		FindBySourceID(gomock.Any(), sourceID).
		Return(existingFieldMap, nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteSource(context.Background(), contextID, sourceID)
	require.ErrorIs(t, err, ErrSourceHasFieldMap)
}

func TestDeleteSource_FieldMapCheckError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	sourceID := uuid.New()

	checkErr := errors.New("field map repo unavailable")
	mockFmRepo.EXPECT().
		FindBySourceID(gomock.Any(), sourceID).
		Return(nil, checkErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteSource(context.Background(), contextID, sourceID)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking source field map")
	require.ErrorIs(t, err, checkErr)
}
