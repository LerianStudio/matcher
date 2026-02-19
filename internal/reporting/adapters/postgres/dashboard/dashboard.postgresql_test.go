//go:build unit

package dashboard

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
	}

	_, err := repo.GetVolumeStats(ctx, filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetSLAStats(ctx, filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetSummaryMetrics(ctx, filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetTrendMetrics(ctx, filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetBreakdownMetrics(ctx, filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetSourceBreakdown(ctx, filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetCashImpactSummary(ctx, filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}
