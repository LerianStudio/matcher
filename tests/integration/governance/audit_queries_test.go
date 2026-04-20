//go:build integration

package governance

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	pkghttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	auditRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/tests/integration"
)

// createAuditLog is a test helper that creates and persists an audit log entry.
// It reduces boilerplate across query tests by handling entity construction and insertion.
func createAuditLog(
	t *testing.T,
	repo *auditRepo.Repository,
	h *integration.TestHarness,
	entityType string,
	entityID uuid.UUID,
	action string,
	actorID *string,
) *entities.AuditLog {
	t.Helper()

	ctx := h.Ctx()
	changes := []byte(fmt.Sprintf(`{"entity_type":%q,"action":%q}`, entityType, action))

	auditLog, err := entities.NewAuditLog(
		ctx,
		h.Seed.TenantID,
		entityType,
		entityID,
		action,
		actorID,
		changes,
	)
	require.NoError(t, err)

	created, err := repo.Create(ctx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, created)

	return created
}

func TestAuditQuery_FilterByEntityType(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		// Insert logs for three distinct entity types
		contextEntityID := uuid.New()
		sourceEntityID := uuid.New()
		ruleEntityID := uuid.New()

		createAuditLog(t, repo, h, "context", contextEntityID, "CREATE", nil)
		createAuditLog(t, repo, h, "context", contextEntityID, "UPDATE", nil)
		createAuditLog(t, repo, h, "source", sourceEntityID, "CREATE", nil)
		createAuditLog(t, repo, h, "rule", ruleEntityID, "CREATE", nil)

		// Filter by entity_type = "context"
		filter := entities.AuditLogFilter{
			EntityType: strPtr("context"),
		}

		logs, _, err := repo.List(ctx, filter, nil, 50)
		require.NoError(t, err)
		require.Len(t, logs, 2, "should return only context-typed logs")

		for _, log := range logs {
			require.Equal(t, "context", log.EntityType)
		}
	})
}

func TestAuditQuery_FilterByAction(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entityID := uuid.New()

		createAuditLog(t, repo, h, "transaction", entityID, "CREATE", nil)
		createAuditLog(t, repo, h, "transaction", entityID, "UPDATE", nil)
		createAuditLog(t, repo, h, "transaction", entityID, "UPDATE", nil)
		createAuditLog(t, repo, h, "transaction", entityID, "DELETE", nil)

		// Filter by action = "UPDATE"
		filter := entities.AuditLogFilter{
			Action: strPtr("UPDATE"),
		}

		logs, _, err := repo.List(ctx, filter, nil, 50)
		require.NoError(t, err)
		require.Len(t, logs, 2, "should return only UPDATE logs")

		for _, log := range logs {
			require.Equal(t, "UPDATE", log.Action)
		}
	})
}

func TestAuditQuery_FilterByDateRange(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entityID := uuid.New()

		// Create several logs — they'll all have created_at ≈ now.
		// We insert 4 logs, then use a date range that captures all of them,
		// and a narrow range that should capture none (in the past).
		for i := 0; i < 4; i++ {
			createAuditLog(t, repo, h, "date_range_test", entityID, fmt.Sprintf("ACTION_%d", i), nil)
		}

		now := time.Now().UTC()

		// Wide range: should include all logs created "just now"
		wideFrom := now.Add(-1 * time.Hour)
		wideTo := now.Add(1 * time.Hour)

		wideFilter := entities.AuditLogFilter{
			EntityType: strPtr("date_range_test"),
			DateFrom:   &wideFrom,
			DateTo:     &wideTo,
		}

		logs, _, err := repo.List(ctx, wideFilter, nil, 50)
		require.NoError(t, err)
		require.Len(t, logs, 4, "wide date range should include all recently created logs")

		// Narrow range in the past: should return zero
		pastFrom := now.Add(-48 * time.Hour)
		pastTo := now.Add(-24 * time.Hour)

		narrowFilter := entities.AuditLogFilter{
			EntityType: strPtr("date_range_test"),
			DateFrom:   &pastFrom,
			DateTo:     &pastTo,
		}

		logsNarrow, _, err := repo.List(ctx, narrowFilter, nil, 50)
		require.NoError(t, err)
		require.Empty(t, logsNarrow, "past date range should return no logs")
	})
}

func TestAuditQuery_FilterByActor(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entityID := uuid.New()
		actorAlice := "alice@example.com"
		actorBob := "bob@example.com"

		createAuditLog(t, repo, h, "actor_test", entityID, "CREATE", &actorAlice)
		createAuditLog(t, repo, h, "actor_test", entityID, "UPDATE", &actorAlice)
		createAuditLog(t, repo, h, "actor_test", entityID, "UPDATE", &actorBob)
		createAuditLog(t, repo, h, "actor_test", entityID, "DELETE", nil)

		// Filter by actor = alice
		filter := entities.AuditLogFilter{
			Actor:      &actorAlice,
			EntityType: strPtr("actor_test"),
		}

		logs, _, err := repo.List(ctx, filter, nil, 50)
		require.NoError(t, err)
		require.Len(t, logs, 2, "should return only alice's logs")

		for _, log := range logs {
			require.NotNil(t, log.ActorID)
			require.Equal(t, actorAlice, *log.ActorID)
		}

		// Filter by actor = bob
		bobFilter := entities.AuditLogFilter{
			Actor:      &actorBob,
			EntityType: strPtr("actor_test"),
		}

		bobLogs, _, err := repo.List(ctx, bobFilter, nil, 50)
		require.NoError(t, err)
		require.Len(t, bobLogs, 1, "should return only bob's single log")
		require.Equal(t, actorBob, *bobLogs[0].ActorID)
	})
}

func TestAuditQuery_PaginationByEntity(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entityID := uuid.New()

		// Insert 5 audit logs for the same entity
		for i := 0; i < 5; i++ {
			createAuditLog(t, repo, h, "pagination_test", entityID, fmt.Sprintf("ACTION_%d", i), nil)
		}

		// Page 1: limit=2
		page1, nextCursor1, err := repo.ListByEntity(ctx, "pagination_test", entityID, nil, 2)
		require.NoError(t, err)
		require.Len(t, page1, 2)
		require.NotEmpty(t, nextCursor1, "should have a next cursor when more pages exist")

		// Page 2: limit=2
		cursor1, err := pkghttp.DecodeTimestampCursor(nextCursor1)
		require.NoError(t, err)

		page2, nextCursor2, err := repo.ListByEntity(ctx, "pagination_test", entityID, cursor1, 2)
		require.NoError(t, err)
		require.Len(t, page2, 2)
		require.NotEmpty(t, nextCursor2, "should have a next cursor for the last page")

		// Page 3: should contain the remaining 1 record
		cursor2, err := pkghttp.DecodeTimestampCursor(nextCursor2)
		require.NoError(t, err)

		page3, nextCursor3, err := repo.ListByEntity(ctx, "pagination_test", entityID, cursor2, 2)
		require.NoError(t, err)
		require.Len(t, page3, 1, "last page should have exactly 1 remaining log")
		require.Empty(t, nextCursor3, "no more pages should exist")

		// Verify no ID overlap across pages
		allIDs := make(map[uuid.UUID]bool, 5)
		for _, log := range page1 {
			allIDs[log.ID] = true
		}

		for _, log := range page2 {
			require.False(t, allIDs[log.ID], "page 2 should not overlap with page 1")
			allIDs[log.ID] = true
		}

		for _, log := range page3 {
			require.False(t, allIDs[log.ID], "page 3 should not overlap with previous pages")
			allIDs[log.ID] = true
		}

		require.Len(t, allIDs, 5, "all 5 logs should be accounted for across pages")
	})
}

func TestAuditQuery_EmptyResult(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		// Query for an entity type that was never inserted
		filter := entities.AuditLogFilter{
			EntityType: strPtr("nonexistent_entity_type_xyz"),
		}

		logs, nextCursor, err := repo.List(ctx, filter, nil, 50)
		require.NoError(t, err)
		require.Empty(t, logs, "should return empty slice for non-existent entity type")
		require.Empty(t, nextCursor, "should return empty cursor for empty result")
	})
}

// strPtr is a helper that returns a pointer to the given string.
func strPtr(s string) *string {
	return &s
}
