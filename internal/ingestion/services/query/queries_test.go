//go:build unit

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestNewUseCaseRequiresRepos(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)

	_, err := NewUseCase(nil, txRepo)
	require.ErrorIs(t, err, ErrNilJobRepository)

	_, err = NewUseCase(jobRepo, nil)
	require.ErrorIs(t, err, ErrNilTransactionRepository)
}

func TestQueryUseCasePaths(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()

	job := &entities.IngestionJob{ID: jobID, ContextID: contextID}
	jobRepo.EXPECT().FindByID(gomock.Any(), jobID).Return(job, nil)
	result, err := uc.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, job, result)

	jobRepo.EXPECT().FindByID(gomock.Any(), jobID).Return(job, nil)
	result, err = uc.GetJobByContext(ctx, contextID, jobID)
	require.NoError(t, err)
	require.Equal(t, job, result)

	wrongContext := uuid.New()

	jobRepo.EXPECT().FindByID(gomock.Any(), jobID).Return(job, nil)
	_, err = uc.GetJobByContext(ctx, wrongContext, jobID)
	require.ErrorIs(t, err, ErrJobNotFound)

	filter := repositories.CursorFilter{Limit: 10}
	jobRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, filter).
		Return([]*entities.IngestionJob{job}, libHTTP.CursorPagination{}, nil)
	jobs, _, err := uc.ListJobsByContext(ctx, contextID, filter)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	tx := &shared.Transaction{ID: uuid.New()}
	txRepo.EXPECT().FindByID(gomock.Any(), tx.ID).Return(tx, nil)
	resultTx, err := uc.GetTransaction(ctx, tx.ID)
	require.NoError(t, err)
	require.Equal(t, tx, resultTx)

	filterTx := repositories.CursorFilter{Limit: 5}
	txRepo.EXPECT().
		FindByJobID(gomock.Any(), jobID, filterTx).
		Return([]*shared.Transaction{tx}, libHTTP.CursorPagination{}, nil)
	list, _, err := uc.ListTransactionsByJob(ctx, jobID, filterTx)
	require.NoError(t, err)
	require.Len(t, list, 1)

	txRepo.EXPECT().
		FindByJobAndContextID(gomock.Any(), jobID, contextID, filterTx).
		Return([]*shared.Transaction{tx}, libHTTP.CursorPagination{}, nil)
	list, _, err = uc.ListTransactionsByJobContext(ctx, jobID, contextID, filterTx)
	require.NoError(t, err)
	require.Len(t, list, 1)
}

func TestGetJobByContextNilJobReturnsNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()

	jobRepo.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, nil)
	_, err = uc.GetJobByContext(ctx, contextID, jobID)
	require.ErrorIs(t, err, ErrJobNotFound)
}

// errRepoFind is a sentinel error for repository find failures.
var errRepoFind = errors.New("repository find failed")

func TestGetJob_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.GetJob(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestGetJob_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()

	jobRepo.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, nil)
	_, err = uc.GetJob(ctx, jobID)
	require.ErrorIs(t, err, ErrJobNotFound)
}

func TestGetJob_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()

	jobRepo.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, errRepoFind)
	_, err = uc.GetJob(ctx, jobID)
	require.Error(t, err)
	require.ErrorIs(t, err, errRepoFind)
	require.Contains(t, err.Error(), "finding job")
}

func TestGetJobByContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.GetJobByContext(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestGetJobByContext_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()

	jobRepo.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, errRepoFind)
	_, err = uc.GetJobByContext(ctx, contextID, jobID)
	require.Error(t, err)
	require.ErrorIs(t, err, errRepoFind)
	require.Contains(t, err.Error(), "finding job")
}

func TestListJobsByContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, _, err := uc.ListJobsByContext(context.Background(), uuid.New(), repositories.CursorFilter{})
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestListJobsByContext_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	filter := repositories.CursorFilter{Limit: 10}

	jobRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, filter).
		Return(nil, libHTTP.CursorPagination{}, errRepoFind)
	_, _, err = uc.ListJobsByContext(ctx, contextID, filter)
	require.Error(t, err)
	require.ErrorIs(t, err, errRepoFind)
	require.Contains(t, err.Error(), "finding jobs by context")
}

func TestListJobsByContext_EmptyResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	filter := repositories.CursorFilter{Limit: 10}

	jobRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, filter).
		Return([]*entities.IngestionJob{}, libHTTP.CursorPagination{}, nil)
	jobs, pagination, err := uc.ListJobsByContext(ctx, contextID, filter)
	require.NoError(t, err)
	require.Empty(t, jobs)
	require.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestGetTransaction_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.GetTransaction(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestGetTransaction_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	txID := uuid.New()

	txRepo.EXPECT().FindByID(gomock.Any(), txID).Return(nil, errRepoFind)
	_, err = uc.GetTransaction(ctx, txID)
	require.Error(t, err)
	require.ErrorIs(t, err, errRepoFind)
	require.Contains(t, err.Error(), "finding transaction")
}

func TestGetTransaction_NilResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	txID := uuid.New()

	txRepo.EXPECT().FindByID(gomock.Any(), txID).Return(nil, nil)
	_, err = uc.GetTransaction(ctx, txID)
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

func TestListTransactionsByJob_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, _, err := uc.ListTransactionsByJob(
		context.Background(),
		uuid.New(),
		repositories.CursorFilter{},
	)
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestListTransactionsByJob_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()
	filter := repositories.CursorFilter{Limit: 5}

	txRepo.EXPECT().
		FindByJobID(gomock.Any(), jobID, filter).
		Return(nil, libHTTP.CursorPagination{}, errRepoFind)
	_, _, err = uc.ListTransactionsByJob(ctx, jobID, filter)
	require.Error(t, err)
	require.ErrorIs(t, err, errRepoFind)
	require.Contains(t, err.Error(), "finding transactions by job")
}

func TestListTransactionsByJob_EmptyResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()
	filter := repositories.CursorFilter{Limit: 5}

	txRepo.EXPECT().
		FindByJobID(gomock.Any(), jobID, filter).
		Return([]*shared.Transaction{}, libHTTP.CursorPagination{}, nil)
	txs, pagination, err := uc.ListTransactionsByJob(ctx, jobID, filter)
	require.NoError(t, err)
	require.Empty(t, txs)
	require.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestListTransactionsByJobContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, _, err := uc.ListTransactionsByJobContext(
		context.Background(),
		uuid.New(),
		uuid.New(),
		repositories.CursorFilter{},
	)
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestListTransactionsByJobContext_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()
	filter := repositories.CursorFilter{Limit: 5}

	txRepo.EXPECT().
		FindByJobAndContextID(gomock.Any(), jobID, contextID, filter).
		Return(nil, libHTTP.CursorPagination{}, errRepoFind)
	_, _, err = uc.ListTransactionsByJobContext(ctx, jobID, contextID, filter)
	require.Error(t, err)
	require.ErrorIs(t, err, errRepoFind)
	require.Contains(t, err.Error(), "finding transactions by job and context")
}

func TestListTransactionsByJobContext_EmptyResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)
	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()
	filter := repositories.CursorFilter{Limit: 5}

	txRepo.EXPECT().
		FindByJobAndContextID(gomock.Any(), jobID, contextID, filter).
		Return([]*shared.Transaction{}, libHTTP.CursorPagination{}, nil)
	txs, pagination, err := uc.ListTransactionsByJobContext(ctx, jobID, contextID, filter)
	require.NoError(t, err)
	require.Empty(t, txs)
	require.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestNewUseCaseSuccess(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobRepo := mocks.NewMockJobRepository(ctrl)
	txRepo := mocks.NewMockTransactionRepository(ctrl)

	uc, err := NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)
	require.NotNil(t, uc)
}
