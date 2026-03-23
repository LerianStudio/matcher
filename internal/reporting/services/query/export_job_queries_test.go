//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	repomocks "github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
)

var errTestDatabaseError = errors.New("test error")

func TestNewExportJobQueryService(t *testing.T) {
	t.Parallel()

	t.Run("creates service with valid repository", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		svc, err := NewExportJobQueryService(repo)

		require.NoError(t, err)
		assert.NotNil(t, svc)
	})

	t.Run("returns error with nil repository", func(t *testing.T) {
		t.Parallel()

		svc, err := NewExportJobQueryService(nil)

		require.Error(t, err)
		assert.Nil(t, svc)
		require.ErrorIs(t, err, ErrNilExportJobRepository)
	})
}

func TestExportJobQueryService_GetByID(t *testing.T) {
	t.Parallel()

	t.Run("returns job successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		jobID := uuid.New()
		expected := &entities.ExportJob{
			ID:         jobID,
			TenantID:   uuid.New(),
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusQueued,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(expected, nil).Times(1)

		svc, err := NewExportJobQueryService(repo)
		require.NoError(t, err)

		job, err := svc.GetByID(context.Background(), jobID)

		require.NoError(t, err)
		assert.Equal(t, expected.ID, job.ID)
		assert.Equal(t, expected.ReportType, job.ReportType)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		repoErr := errTestDatabaseError
		repo.EXPECT().GetByID(gomock.Any(), gomock.Any()).Return(nil, repoErr).Times(1)

		svc, err := NewExportJobQueryService(repo)
		require.NoError(t, err)

		job, err := svc.GetByID(context.Background(), uuid.New())

		require.Error(t, err)
		assert.Nil(t, job)
	})

	t.Run("wraps sentinel ErrExportJobNotFound from repository", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		repo.EXPECT().GetByID(gomock.Any(), gomock.Any()).
			Return(nil, repositories.ErrExportJobNotFound).
			Times(1)

		svc, err := NewExportJobQueryService(repo)
		require.NoError(t, err)

		job, err := svc.GetByID(context.Background(), uuid.New())

		require.Error(t, err)
		assert.Nil(t, job)
		require.ErrorIs(t, err, repositories.ErrExportJobNotFound)
	})
}

func TestExportJobQueryService_List(t *testing.T) {
	t.Parallel()

	t.Run("returns jobs successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		expected := []*entities.ExportJob{
			{
				ID:         uuid.New(),
				TenantID:   uuid.New(),
				ContextID:  uuid.New(),
				ReportType: "MATCHED",
				Format:     "CSV",
				Status:     entities.ExportJobStatusQueued,
				CreatedAt:  time.Now().UTC(),
				ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
				UpdatedAt:  time.Now().UTC(),
			},
		}

		repo.EXPECT().
			List(gomock.Any(), gomock.Nil(), (*libHTTP.TimestampCursor)(nil), 10).
			Return(expected, libHTTP.CursorPagination{}, nil).
			Times(1)

		svc, err := NewExportJobQueryService(repo)
		require.NoError(t, err)

		jobs, _, err := svc.List(context.Background(), ListExportJobsInput{
			Status: nil,
			Cursor: nil,
			Limit:  10,
		})

		require.NoError(t, err)
		assert.Len(t, jobs, 1)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		repoErr := errTestDatabaseError
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, libHTTP.CursorPagination{}, repoErr).
			Times(1)

		svc, err := NewExportJobQueryService(repo)
		require.NoError(t, err)

		jobs, _, err := svc.List(context.Background(), ListExportJobsInput{
			Status: nil,
			Cursor: nil,
			Limit:  10,
		})

		require.Error(t, err)
		assert.Nil(t, jobs)
	})
}

func TestExportJobQueryService_ListByContext(t *testing.T) {
	t.Parallel()

	t.Run("returns jobs successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		contextID := uuid.New()
		expected := []*entities.ExportJob{
			{
				ID:         uuid.New(),
				TenantID:   uuid.New(),
				ContextID:  contextID,
				ReportType: "MATCHED",
				Format:     "CSV",
				Status:     entities.ExportJobStatusQueued,
				CreatedAt:  time.Now().UTC(),
				ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
				UpdatedAt:  time.Now().UTC(),
			},
		}

		repo.EXPECT().ListByContext(gomock.Any(), contextID, 10).Return(expected, nil).Times(1)

		svc, err := NewExportJobQueryService(repo)
		require.NoError(t, err)

		jobs, err := svc.ListByContext(context.Background(), contextID, 10)

		require.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, contextID, jobs[0].ContextID)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repomocks.NewMockExportJobRepository(ctrl)

		repoErr := errTestDatabaseError
		repo.EXPECT().
			ListByContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, repoErr).
			Times(1)

		svc, err := NewExportJobQueryService(repo)
		require.NoError(t, err)

		jobs, err := svc.ListByContext(context.Background(), uuid.New(), 10)

		require.Error(t, err)
		assert.Nil(t, jobs)
	})
}
