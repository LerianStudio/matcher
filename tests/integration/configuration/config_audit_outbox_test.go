//go:build integration

package configuration

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	outboxEntities "github.com/LerianStudio/lib-commons/v5/commons/outbox"
	configAudit "github.com/LerianStudio/matcher/internal/configuration/adapters/audit"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// newAuditPublisher wires a real OutboxPublisher backed by the test harness's database.
func newAuditPublisher(
	t *testing.T,
	h *integration.TestHarness,
) *configAudit.OutboxPublisher {
	t.Helper()

	repo := integration.NewTestOutboxRepository(t, h.Connection)

	pub, err := configAudit.NewOutboxPublisher(repo)
	require.NoError(t, err)

	return pub
}

// countOutboxByAggregateAndType counts outbox events matching aggregate_id and event_type.
func countOutboxByAggregateAndType(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	aggregateID uuid.UUID,
	eventType string,
) int {
	t.Helper()

	n, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (int, error) {
		var count int
		if scanErr := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type=$2`,
			aggregateID.String(), eventType,
		).Scan(&count); scanErr != nil {
			return 0, scanErr
		}

		return count, nil
	})
	require.NoError(t, err)

	return n
}

// readOutboxPayload reads the most recent outbox event payload for a given aggregate_id.
func readOutboxPayload(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	aggregateID uuid.UUID,
) []byte {
	t.Helper()

	payload, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) ([]byte, error) {
		var p []byte
		if scanErr := tx.QueryRowContext(ctx,
			`SELECT payload FROM outbox_events WHERE aggregate_id=$1 ORDER BY created_at DESC LIMIT 1`,
			aggregateID.String(),
		).Scan(&p); scanErr != nil {
			return nil, scanErr
		}

		return p, nil
	})
	require.NoError(t, err)

	return payload
}

// readOutboxStatus reads the status of the most recent outbox event for a given aggregate_id.
func readOutboxStatus(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	aggregateID uuid.UUID,
) string {
	t.Helper()

	status, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (string, error) {
		var s string
		if scanErr := tx.QueryRowContext(ctx,
			`SELECT status FROM outbox_events WHERE aggregate_id=$1 ORDER BY created_at DESC LIMIT 1`,
			aggregateID.String(),
		).Scan(&s); scanErr != nil {
			return "", scanErr
		}

		return s, nil
	})
	require.NoError(t, err)

	return status
}

// countOutboxByAggregate counts all outbox events matching a given aggregate_id.
func countOutboxByAggregate(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	aggregateID uuid.UUID,
) int {
	t.Helper()

	n, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (int, error) {
		var count int
		if scanErr := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM outbox_events WHERE aggregate_id=$1`,
			aggregateID.String(),
		).Scan(&count); scanErr != nil {
			return 0, scanErr
		}

		return count, nil
	})
	require.NoError(t, err)

	return n
}

func TestConfigAuditOutbox_PublishCreatesOutboxEvent(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		pub := newAuditPublisher(t, h)

		entityID := uuid.New()

		event := configPorts.AuditEvent{
			EntityType: "reconciliation_context",
			EntityID:   entityID,
			Action:     "CREATED",
			Actor:      "system",
			OccurredAt: time.Now().UTC(),
			Changes: map[string]any{
				"name": "Test Context",
			},
		}

		err := pub.Publish(ctx, event)
		require.NoError(t, err)

		count := countOutboxByAggregateAndType(t, ctx, h, entityID, shared.EventTypeAuditLogCreated)
		require.Equal(t, 1, count, "expected exactly 1 outbox event for the published audit event")
	})
}

func TestConfigAuditOutbox_PayloadContainsAuditFields(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		pub := newAuditPublisher(t, h)

		entityID := uuid.New()

		event := configPorts.AuditEvent{
			EntityType: "reconciliation_source",
			EntityID:   entityID,
			Action:     "UPDATED",
			Actor:      "admin-user",
			OccurredAt: time.Now().UTC(),
			Changes: map[string]any{
				"name":     "Updated Source",
				"interval": "0 */2 * * *",
			},
		}

		err := pub.Publish(ctx, event)
		require.NoError(t, err)

		payload := readOutboxPayload(t, ctx, h, entityID)
		require.NotEmpty(t, payload, "outbox event payload must not be empty")

		var decoded map[string]any
		err = json.Unmarshal(payload, &decoded)
		require.NoError(t, err, "payload must be valid JSON")

		require.Equal(t, "reconciliation_source", decoded["entityType"],
			"payload must contain entity_type matching the published event")
		require.Equal(t, entityID.String(), decoded["entityId"],
			"payload must contain entity_id matching the published event")
		require.Equal(t, "UPDATED", decoded["action"],
			"payload must contain action matching the published event")
		require.Equal(t, shared.EventTypeAuditLogCreated, decoded["eventType"],
			"payload must contain event_type for audit log creation")
	})
}

func TestConfigAuditOutbox_MultipleEventsForDifferentEntities(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		pub := newAuditPublisher(t, h)

		entityIDs := [3]uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
		actions := [3]string{"CREATED", "UPDATED", "DELETED"}

		for i := range entityIDs {
			event := configPorts.AuditEvent{
				EntityType: "match_rule",
				EntityID:   entityIDs[i],
				Action:     actions[i],
				Actor:      "system",
				OccurredAt: time.Now().UTC(),
				Changes:    map[string]any{"index": i},
			}

			err := pub.Publish(ctx, event)
			require.NoError(t, err, "publish must succeed for event %d", i)
		}

		// Verify each entity got exactly one outbox event with the correct aggregate_id.
		for i, entityID := range entityIDs {
			count := countOutboxByAggregate(t, ctx, h, entityID)
			require.Equal(t, 1, count,
				"entity %d (%s) must have exactly 1 outbox event, got %d",
				i, entityID.String(), count)
		}
	})
}

func TestConfigAuditOutbox_EventStatusIsPending(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		pub := newAuditPublisher(t, h)

		entityID := uuid.New()

		event := configPorts.AuditEvent{
			EntityType: "fee_schedule",
			EntityID:   entityID,
			Action:     "CREATED",
			Actor:      "system",
			OccurredAt: time.Now().UTC(),
			Changes:    map[string]any{"rate": "0.015"},
		}

		err := pub.Publish(ctx, event)
		require.NoError(t, err)

		status := readOutboxStatus(t, ctx, h, entityID)
		require.Equal(t, string(outboxEntities.OutboxStatusPending), status,
			"newly created outbox event must be in PENDING status for dispatcher pickup")
	})
}

func TestConfigAuditOutbox_ActorIDPropagated(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		pub := newAuditPublisher(t, h)

		entityID := uuid.New()
		actorID := "user-" + uuid.New().String()[:8]

		event := configPorts.AuditEvent{
			EntityType: "reconciliation_context",
			EntityID:   entityID,
			Action:     "UPDATED",
			Actor:      actorID,
			OccurredAt: time.Now().UTC(),
			Changes: map[string]any{
				"status": "active",
			},
		}

		err := pub.Publish(ctx, event)
		require.NoError(t, err)

		payload := readOutboxPayload(t, ctx, h, entityID)
		require.NotEmpty(t, payload)

		var decoded map[string]any
		err = json.Unmarshal(payload, &decoded)
		require.NoError(t, err, "payload must be valid JSON")

		require.Equal(t, actorID, decoded["actor"],
			"actor_id must be propagated into the outbox event payload")
	})
}
