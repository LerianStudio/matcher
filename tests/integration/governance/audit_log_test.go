//go:build integration

package governance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	pkghttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	auditRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegrationAuditLog_CreateAndGetByID(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		changes := map[string]any{"field": "value", "count": 42}
		changesJSON, err := json.Marshal(changes)
		require.NoError(t, err)

		actorID := "user-123"
		auditLog, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"transaction",
			uuid.New(),
			"CREATE",
			&actorID,
			changesJSON,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, auditLog)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, h.Seed.TenantID, created.TenantID)
		require.Equal(t, "transaction", created.EntityType)
		require.Equal(t, "CREATE", created.Action)
		require.Equal(t, &actorID, created.ActorID)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.TenantID, fetched.TenantID)
		require.Equal(t, created.EntityType, fetched.EntityType)
		require.Equal(t, created.EntityID, fetched.EntityID)
		require.Equal(t, created.Action, fetched.Action)
		require.Equal(t, created.ActorID, fetched.ActorID)
		require.JSONEq(t, string(changesJSON), string(fetched.Changes))
	})
}

func TestIntegrationAuditLog_TenantIsolation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		tenant1ID := h.Seed.TenantID
		tenant2ID := uuid.New()
		entityID := uuid.New()

		changes := []byte(`{"operation": "update"}`)

		// Create audit log for tenant1 (uses tenant1 context -> writes to default schema)
		auditLog1, err := entities.NewAuditLog(
			ctx,
			tenant1ID,
			"source",
			entityID,
			"UPDATE",
			nil,
			changes,
		)
		require.NoError(t, err)
		created1, err := repo.Create(ctx, auditLog1)
		require.NoError(t, err)

		// Create audit log for tenant2 using a tenant2 context.
		// The repository derives tenant ID from context (security pattern),
		// so we must use a context carrying tenant2's ID.
		ctx2 := context.WithValue(context.Background(), auth.TenantIDKey, tenant2ID.String())

		auditLog2, err := entities.NewAuditLog(
			ctx2,
			tenant2ID,
			"source",
			entityID,
			"DELETE",
			nil,
			changes,
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx2, auditLog2)
		require.NoError(t, err)

		// Verify tenant1 context only sees tenant1's logs (tenant isolation via tenant_id filtering)
		logs1, _, err := repo.ListByEntity(ctx, "source", entityID, nil, 10)
		require.NoError(t, err)
		require.Len(t, logs1, 1, "Tenant1 should only see its own audit log")
		require.Equal(t, created1.ID, logs1[0].ID)
		require.Equal(t, "UPDATE", logs1[0].Action)
		require.Equal(t, tenant1ID, logs1[0].TenantID, "Returned log should belong to tenant1")
	})
}

func TestIntegrationAuditLog_ListByEntityOrdering(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entityID := uuid.New()
		changes := []byte(`{"order": "test"}`)

		auditLog1, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"match_group",
			entityID,
			"FIRST",
			nil,
			changes,
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, auditLog1)
		require.NoError(t, err)

		auditLog2, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"match_group",
			entityID,
			"SECOND",
			nil,
			changes,
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, auditLog2)
		require.NoError(t, err)

		auditLog3, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"match_group",
			entityID,
			"THIRD",
			nil,
			changes,
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, auditLog3)
		require.NoError(t, err)

		logs, _, err := repo.ListByEntity(ctx, "match_group", entityID, nil, 10)
		require.NoError(t, err)
		require.Len(t, logs, 3)
		require.Equal(t, "THIRD", logs[0].Action)
		require.Equal(t, "SECOND", logs[1].Action)
		require.Equal(t, "FIRST", logs[2].Action)
	})
}

func TestIntegrationAuditLog_ListByEntityPagination(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entityID := uuid.New()
		changes := []byte(`{"page": "test"}`)

		for i := 0; i < 5; i++ {
			auditLog, err := entities.NewAuditLog(
				ctx,
				h.Seed.TenantID,
				"paginated",
				entityID,
				"ACTION",
				nil,
				changes,
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, auditLog)
			require.NoError(t, err)
		}

		page1, nextCursor1, err := repo.ListByEntity(ctx, "paginated", entityID, nil, 2)
		require.NoError(t, err)
		require.Len(t, page1, 2)
		require.NotEmpty(t, nextCursor1)

		cursor1, err := pkghttp.DecodeTimestampCursor(nextCursor1)
		require.NoError(t, err)
		page2, nextCursor2, err := repo.ListByEntity(ctx, "paginated", entityID, cursor1, 2)
		require.NoError(t, err)
		require.Len(t, page2, 2)
		require.NotEmpty(t, nextCursor2)

		require.NotEqual(t, page1[0].ID, page2[0].ID)
		require.NotEqual(t, page1[1].ID, page2[1].ID)

		cursor2, err := pkghttp.DecodeTimestampCursor(nextCursor2)
		require.NoError(t, err)
		page3, nextCursor3, err := repo.ListByEntity(ctx, "paginated", entityID, cursor2, 2)
		require.NoError(t, err)
		require.Len(t, page3, 1)
		require.Empty(t, nextCursor3)
	})
}

func TestIntegrationAuditLog_GetByIDNotFound(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.GetByID(ctx, uuid.New())
		require.ErrorIs(t, err, auditRepo.ErrAuditLogNotFound)
	})
}

func TestIntegrationAuditLog_ListByEntityEmpty(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		logs, _, err := repo.ListByEntity(ctx, "nonexistent", uuid.New(), nil, 10)
		require.NoError(t, err)
		require.Empty(t, logs)
	})
}

func TestIntegrationAuditLog_MultipleEntitiesSameType(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity1ID := uuid.New()
		entity2ID := uuid.New()
		changes := []byte(`{"multi": "entity"}`)

		auditLog1, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"rule",
			entity1ID,
			"CREATE",
			nil,
			changes,
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, auditLog1)
		require.NoError(t, err)

		auditLog2, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"rule",
			entity1ID,
			"UPDATE",
			nil,
			changes,
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, auditLog2)
		require.NoError(t, err)

		auditLog3, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"rule",
			entity2ID,
			"CREATE",
			nil,
			changes,
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, auditLog3)
		require.NoError(t, err)

		logs1, _, err := repo.ListByEntity(ctx, "rule", entity1ID, nil, 10)
		require.NoError(t, err)
		require.Len(t, logs1, 2)

		logs2, _, err := repo.ListByEntity(ctx, "rule", entity2ID, nil, 10)
		require.NoError(t, err)
		require.Len(t, logs2, 1)
	})
}

func TestIntegrationAuditLog_NilActorID(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		changes := []byte(`{"system": "auto"}`)

		auditLog, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"job",
			uuid.New(),
			"COMPLETE",
			nil,
			changes,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, auditLog)
		require.NoError(t, err)
		require.Nil(t, created.ActorID)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Nil(t, fetched.ActorID)
	})
}

func TestIntegrationAuditLog_ContextCancellation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())

		ctx, cancel := context.WithCancel(h.Ctx())
		cancel()

		_, err := repo.GetByID(ctx, uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestIntegrationAuditLog_ComplexChangesJSON(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		changes := map[string]any{
			"before": map[string]any{
				"status": "UNMATCHED",
				"amount": 100.50,
			},
			"after": map[string]any{
				"status": "MATCHED",
				"amount": 100.50,
			},
			"metadata": map[string]any{
				"rule_id":    uuid.New().String(),
				"confidence": 95,
				"tags":       []string{"auto", "batch"},
			},
		}
		changesJSON, err := json.Marshal(changes)
		require.NoError(t, err)

		actorID := "system-matcher"
		auditLog, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"transaction",
			uuid.New(),
			"MATCH",
			&actorID,
			changesJSON,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, auditLog)
		require.NoError(t, err)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)

		var storedChanges map[string]any
		require.NoError(t, json.Unmarshal(fetched.Changes, &storedChanges))

		beforeMap, ok := storedChanges["before"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "UNMATCHED", beforeMap["status"])

		afterMap, ok := storedChanges["after"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "MATCHED", afterMap["status"])
	})
}

func TestIntegrationAuditLog_CreateWithTx_CommitAndRollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		db, err := h.Connection.Resolver(ctx)
		require.NoError(t, err)

		primaryDBs := db.PrimaryDBs()
		require.NotEmpty(t, primaryDBs)

		tx, err := primaryDBs[0].BeginTx(ctx, nil)
		require.NoError(t, err)

		auditLog, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"tx_commit_test",
			uuid.New(),
			"CREATE",
			nil,
			[]byte(`{"tx":"commit"}`),
		)
		require.NoError(t, err)

		created, err := repo.CreateWithTx(ctx, tx, auditLog)
		require.NoError(t, err)
		require.NotNil(t, created)
		require.NoError(t, tx.Commit())

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, "tx_commit_test", fetched.EntityType)

		txRollback, err := primaryDBs[0].BeginTx(ctx, nil)
		require.NoError(t, err)

		auditLogRollback, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"tx_rollback_test",
			uuid.New(),
			"DELETE",
			nil,
			[]byte(`{"tx":"rollback"}`),
		)
		require.NoError(t, err)

		createdRollback, err := repo.CreateWithTx(ctx, txRollback, auditLogRollback)
		require.NoError(t, err)
		require.NotNil(t, createdRollback)
		require.NoError(t, txRollback.Rollback())

		fetchedRollback, err := repo.GetByID(ctx, createdRollback.ID)
		require.Error(t, err)
		require.Nil(t, fetchedRollback)
		require.ErrorIs(t, err, auditRepo.ErrAuditLogNotFound)
	})
}

func TestIntegrationAuditLog_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()
		entityID := uuid.New()

		const numConcurrent = 10
		var wg sync.WaitGroup

		results := make(chan error, numConcurrent)

		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)

			go func(idx int) {
				defer wg.Done()

				auditLog, err := entities.NewAuditLog(
					ctx,
					h.Seed.TenantID,
					"concurrent_test",
					entityID,
					"CONCURRENT_WRITE",
					nil,
					[]byte(fmt.Sprintf(`{"idx":%d}`, idx)),
				)
				if err != nil {
					results <- err
					return
				}

				_, err = repo.Create(ctx, auditLog)
				results <- err
			}(i)
		}

		wg.Wait()
		close(results)

		for err := range results {
			require.NoError(t, err)
		}

		logs, _, err := repo.ListByEntity(ctx, "concurrent_test", entityID, nil, 20)
		require.NoError(t, err)
		require.Len(t, logs, numConcurrent, "All concurrent writes should succeed")
	})
}

func TestIntegrationAuditLog_TransactionAtomicityWithBusinessOperation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := auditRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		db, err := h.Connection.Resolver(ctx)
		require.NoError(t, err)

		primaryDBs := db.PrimaryDBs()
		require.NotEmpty(t, primaryDBs)

		tx, err := primaryDBs[0].BeginTx(ctx, nil)
		require.NoError(t, err)

		entityID := uuid.New()
		auditLog1, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"atomic_test",
			entityID,
			"STEP_1",
			nil,
			[]byte(`{"step":1}`),
		)
		require.NoError(t, err)

		created1, err := repo.CreateWithTx(ctx, tx, auditLog1)
		require.NoError(t, err)
		require.NotNil(t, created1)

		auditLog2, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"atomic_test",
			entityID,
			"STEP_2",
			nil,
			[]byte(`{"step":2}`),
		)
		require.NoError(t, err)

		created2, err := repo.CreateWithTx(ctx, tx, auditLog2)
		require.NoError(t, err)
		require.NotNil(t, created2)

		require.NoError(t, tx.Rollback())

		logs, _, err := repo.ListByEntity(ctx, "atomic_test", entityID, nil, 10)
		require.NoError(t, err)
		require.Empty(t, logs, "Both audit logs should be rolled back atomically")
	})
}
