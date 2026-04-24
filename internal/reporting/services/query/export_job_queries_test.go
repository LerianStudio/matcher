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
