//go:build integration

package reporting

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	exportJobRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/export_job"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/tests/integration"
)

// testCtx returns a context with tenant ID and slug set for repository operations.
func testCtx(t *testing.T, h *integration.TestHarness) context.Context {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

	return ctx
}

// newExportJobRepo creates an ExportJobRepository from the test harness.
func newExportJobRepo(h *integration.TestHarness) *exportJobRepo.Repository {
	return exportJobRepo.NewRepository(h.Provider())
}

// createTestExportJob creates and persists an export job for testing.
func createTestExportJob(
	t *testing.T,
	ctx context.Context,
	repo *exportJobRepo.Repository,
	tenantID, contextID uuid.UUID,
	reportType, format string,
) *entities.ExportJob {
	t.Helper()

	filter := entities.ExportJobFilter{
		DateFrom: time.Now().UTC().AddDate(0, -1, 0),
		DateTo:   time.Now().UTC(),
	}

	job, err := entities.NewExportJob(ctx, tenantID, contextID, reportType, format, filter)
	require.NoError(t, err)

	err = repo.Create(ctx, job)
	require.NoError(t, err)

	return job
}
