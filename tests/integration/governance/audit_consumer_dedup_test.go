//go:build integration

package governance

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	governanceAudit "github.com/LerianStudio/matcher/internal/governance/adapters/audit"
	governancePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// dedupTestCtx returns a context with both tenant ID and tenant slug set,
// suitable for audit consumer operations that require full tenant context.
func dedupTestCtx(t *testing.T, h *integration.TestHarness) context.Context {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

	return ctx
}

// newDedupEvent creates an AuditLogCreatedEvent with the given entity+action parameters.
// Each call generates a fresh UniqueID to simulate distinct outbox deliveries.
func newDedupEvent(
	tenantID uuid.UUID,
	entityType string,
	entityID uuid.UUID,
	action string,
) *shared.AuditLogCreatedEvent {
	return &shared.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		TenantID:   tenantID,
		EntityType: entityType,
		EntityID:   entityID,
		Action:     action,
		OccurredAt: time.Now().UTC(),
	}
}

// countAuditLogs returns the number of audit_logs rows matching the given entity_type
// and entity_id within the tenant-scoped schema.
func countAuditLogs(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	entityType string,
	entityID uuid.UUID,
) int {
	t.Helper()

	count, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (int, error) {
		var n int
		err := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM audit_logs WHERE entity_type=$1 AND entity_id=$2`,
			entityType, entityID.String(),
		).Scan(&n)

		return n, err
	})
	require.NoError(t, err)

	return count
}

func latestAuditLogCreatedAt(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	entityType string,
	entityID uuid.UUID,
) time.Time {
	t.Helper()

	createdAt, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (time.Time, error) {
		var ts time.Time
		err := tx.QueryRowContext(ctx,
			`SELECT created_at FROM audit_logs WHERE entity_type=$1 AND entity_id=$2 ORDER BY created_at DESC LIMIT 1`,
			entityType, entityID.String(),
		).Scan(&ts)

		return ts, err
	})
	require.NoError(t, err)

	return createdAt
}

func TestAuditConsumerDedup_FirstEventPersisted(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := dedupTestCtx(t, h)
		repo := governancePostgres.NewRepository(h.Provider())

		const dedupWindow = 100 * time.Millisecond

		consumer, err := governanceAudit.NewConsumer(repo, governanceAudit.ConsumerConfig{
			DedupWindow: dedupWindow,
		})
		require.NoError(t, err)

		entityType := "reconciliation_context"
		entityID := uuid.New()

		event := newDedupEvent(h.Seed.TenantID, entityType, entityID, "CREATED")
		err = consumer.PublishAuditLogCreated(ctx, event)
		require.NoError(t, err)

		got := countAuditLogs(t, ctx, h, entityType, entityID)
		require.Equal(t, 1, got, "first event should be persisted")
	})
}

func TestAuditConsumerDedup_DuplicateWithinWindow(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := dedupTestCtx(t, h)
		repo := governancePostgres.NewRepository(h.Provider())

		consumer, err := governanceAudit.NewConsumer(repo, governanceAudit.ConsumerConfig{
			DedupWindow: 100 * time.Millisecond,
		})
		require.NoError(t, err)

		entityType := "reconciliation_context"
		entityID := uuid.New()
		action := "CREATED"

		// First delivery — should persist.
		event1 := newDedupEvent(h.Seed.TenantID, entityType, entityID, action)
		err = consumer.PublishAuditLogCreated(ctx, event1)
		require.NoError(t, err)

		// Second delivery — same entity+action within the 5s dedup window — should be dropped.
		event2 := newDedupEvent(h.Seed.TenantID, entityType, entityID, action)
		err = consumer.PublishAuditLogCreated(ctx, event2)
		require.NoError(t, err)

		got := countAuditLogs(t, ctx, h, entityType, entityID)
		require.Equal(t, 1, got, "duplicate within dedup window should be silently dropped")
	})
}

func TestAuditConsumerDedup_DifferentActionNotDeduped(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := dedupTestCtx(t, h)
		repo := governancePostgres.NewRepository(h.Provider())

		const dedupWindow = 100 * time.Millisecond

		consumer, err := governanceAudit.NewConsumer(repo, governanceAudit.ConsumerConfig{
			DedupWindow: dedupWindow,
		})
		require.NoError(t, err)

		entityType := "reconciliation_context"
		entityID := uuid.New()

		// First event: CREATED action.
		event1 := newDedupEvent(h.Seed.TenantID, entityType, entityID, "CREATED")
		err = consumer.PublishAuditLogCreated(ctx, event1)
		require.NoError(t, err)

		// Second event: UPDATED action on the same entity — different action is not a duplicate.
		event2 := newDedupEvent(h.Seed.TenantID, entityType, entityID, "UPDATED")
		err = consumer.PublishAuditLogCreated(ctx, event2)
		require.NoError(t, err)

		got := countAuditLogs(t, ctx, h, entityType, entityID)
		require.Equal(t, 2, got, "different actions on the same entity must not be deduped")
	})
}

func TestAuditConsumerDedup_DifferentEntityNotDeduped(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := dedupTestCtx(t, h)
		repo := governancePostgres.NewRepository(h.Provider())

		const dedupWindow = 100 * time.Millisecond

		consumer, err := governanceAudit.NewConsumer(repo, governanceAudit.ConsumerConfig{
			DedupWindow: dedupWindow,
		})
		require.NoError(t, err)

		entityType := "reconciliation_context"
		entity1ID := uuid.New()
		entity2ID := uuid.New()
		action := "CREATED"

		// Publish the same action for two distinct entities.
		event1 := newDedupEvent(h.Seed.TenantID, entityType, entity1ID, action)
		err = consumer.PublishAuditLogCreated(ctx, event1)
		require.NoError(t, err)

		event2 := newDedupEvent(h.Seed.TenantID, entityType, entity2ID, action)
		err = consumer.PublishAuditLogCreated(ctx, event2)
		require.NoError(t, err)

		got1 := countAuditLogs(t, ctx, h, entityType, entity1ID)
		require.Equal(t, 1, got1, "entity1 should have exactly 1 audit log")

		got2 := countAuditLogs(t, ctx, h, entityType, entity2ID)
		require.Equal(t, 1, got2, "entity2 should have exactly 1 audit log")
	})
}

func TestAuditConsumerDedup_AfterWindowExpiry(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := dedupTestCtx(t, h)
		repo := governancePostgres.NewRepository(h.Provider())

		const dedupWindow = 100 * time.Millisecond

		consumer, err := governanceAudit.NewConsumer(repo, governanceAudit.ConsumerConfig{
			DedupWindow: dedupWindow,
		})
		require.NoError(t, err)

		entityType := "reconciliation_context"
		entityID := uuid.New()
		action := "CREATED"

		// First delivery — persisted.
		event1 := newDedupEvent(h.Seed.TenantID, entityType, entityID, action)
		err = consumer.PublishAuditLogCreated(ctx, event1)
		require.NoError(t, err)

		firstCreatedAt := latestAuditLogCreatedAt(t, ctx, h, entityType, entityID)

		// Wait beyond the configured test dedup window relative to the persisted row
		// timestamp to avoid flakes from app/database clock skew.
		target := firstCreatedAt.Add(dedupWindow + 25*time.Millisecond)
		if wait := time.Until(target); wait > 0 {
			time.Sleep(wait)
		}

		// Second delivery — same entity+action, but outside the dedup window — should persist.
		event2 := newDedupEvent(h.Seed.TenantID, entityType, entityID, action)
		err = consumer.PublishAuditLogCreated(ctx, event2)
		require.NoError(t, err)

		got := countAuditLogs(t, ctx, h, entityType, entityID)
		require.Equal(t, 2, got, "event after dedup window expiry should be persisted")
	})
}
