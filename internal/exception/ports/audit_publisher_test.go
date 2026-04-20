//go:build unit

package ports

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditPublisher_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ AuditPublisher = (*mockAuditPublisher)(nil)
}

type mockAuditPublisher struct {
	publishedEvents []AuditEvent
}

func (m *mockAuditPublisher) PublishExceptionEvent(_ context.Context, event AuditEvent) error {
	m.publishedEvents = append(m.publishedEvents, event)
	return nil
}

func (m *mockAuditPublisher) PublishExceptionEventWithTx(
	_ context.Context,
	_ *sql.Tx,
	event AuditEvent,
) error {
	m.publishedEvents = append(m.publishedEvents, event)
	return nil
}

func TestAuditPublisher_MockImplementation(t *testing.T) {
	t.Parallel()

	t.Run("publishes event successfully", func(t *testing.T) {
		t.Parallel()

		publisher := &mockAuditPublisher{}
		ctx := t.Context()

		event := AuditEvent{
			ExceptionID: uuid.New(),
			Action:      "CREATED",
			Actor:       "user-123",
			Notes:       "Test notes",
			OccurredAt:  time.Now(),
		}

		err := publisher.PublishExceptionEvent(ctx, event)

		require.NoError(t, err)
		assert.Len(t, publisher.publishedEvents, 1)
		assert.Equal(t, event.ExceptionID, publisher.publishedEvents[0].ExceptionID)
	})
}

func TestAuditPublisher_MockImplementation_WithTx(t *testing.T) {
	t.Parallel()

	t.Run("publishes event successfully", func(t *testing.T) {
		t.Parallel()

		publisher := &mockAuditPublisher{}
		ctx := t.Context()

		event := AuditEvent{
			ExceptionID: uuid.New(),
			Action:      "CREATED",
			Actor:       "user-123",
			Notes:       "Test notes",
			OccurredAt:  time.Now(),
		}

		err := publisher.PublishExceptionEventWithTx(ctx, (*sql.Tx)(nil), event)

		require.NoError(t, err)
		assert.Len(t, publisher.publishedEvents, 1)
		assert.Equal(t, event.ExceptionID, publisher.publishedEvents[0].ExceptionID)
	})
}

func TestHashActor(t *testing.T) {
	t.Parallel()

	t.Run("returns empty string for empty input", func(t *testing.T) {
		t.Parallel()

		result := HashActor("", "any-salt")
		assert.Empty(t, result)
	})

	t.Run("returns hash of correct length without salt", func(t *testing.T) {
		t.Parallel()

		result := HashActor("user-123", "")
		assert.Len(t, result, ActorHashLength)
	})

	t.Run("returns hash of correct length with salt", func(t *testing.T) {
		t.Parallel()

		result := HashActor("user-123", "tenant-salt-abc")
		assert.Len(t, result, ActorHashLength)
	})

	t.Run("returns consistent hash for same input and salt", func(t *testing.T) {
		t.Parallel()

		actor := "user@example.com"
		salt := "tenant-salt-xyz"

		hash1 := HashActor(actor, salt)
		hash2 := HashActor(actor, salt)

		assert.Equal(t, hash1, hash2)
	})

	t.Run("returns different hashes for different inputs", func(t *testing.T) {
		t.Parallel()

		hash1 := HashActor("user-123", "salt")
		hash2 := HashActor("user-456", "salt")

		assert.NotEqual(t, hash1, hash2)
	})

	t.Run("returns different hashes for different salts", func(t *testing.T) {
		t.Parallel()

		actor := "user-123"
		hashA := HashActor(actor, "tenant-a-salt")
		hashB := HashActor(actor, "tenant-b-salt")

		assert.NotEqual(t, hashA, hashB,
			"salting must yield distinct digests across tenants for the same actor")
	})

	t.Run("salted and unsalted hashes differ", func(t *testing.T) {
		t.Parallel()

		actor := "user-123"
		unsalted := HashActor(actor, "")
		salted := HashActor(actor, "any-salt")

		assert.NotEqual(t, unsalted, salted,
			"introducing a salt must change the digest so operators can tell legacy rows apart")
	})

	t.Run("hash is hexadecimal", func(t *testing.T) {
		t.Parallel()

		result := HashActor("test-actor", "")

		// Verify all characters are valid hex
		for _, c := range result {
			assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
				"character %c is not valid hex", c)
		}
	})
}

func TestAuditEvent_ResolveActorHash(t *testing.T) {
	t.Parallel()

	t.Run("returns empty string for empty actor with no precomputed hash", func(t *testing.T) {
		t.Parallel()

		event := AuditEvent{Actor: "", ActorHash: ""}
		assert.Empty(t, event.ResolveActorHash("any-salt"))
	})

	t.Run("returns precomputed hash verbatim", func(t *testing.T) {
		t.Parallel()

		event := AuditEvent{Actor: "user-123", ActorHash: "precomputed"}
		assert.Equal(t, "precomputed", event.ResolveActorHash("any-salt"))
	})

	t.Run("derives hash from actor when ActorHash is empty", func(t *testing.T) {
		t.Parallel()

		event := AuditEvent{Actor: "user-123", ActorHash: ""}
		salt := "tenant-salt"

		assert.Equal(t, HashActor("user-123", salt), event.ResolveActorHash(salt))
	})
}

func TestSaltProviderFunc(t *testing.T) {
	t.Parallel()

	t.Run("nil func returns empty salt", func(t *testing.T) {
		t.Parallel()

		var provider SaltProviderFunc

		assert.Empty(t, provider.SaltFor(t.Context()))
	})

	t.Run("non-nil func forwards to underlying function", func(t *testing.T) {
		t.Parallel()

		provider := SaltProviderFunc(func(_ context.Context) string {
			return "delegated-salt"
		})

		assert.Equal(t, "delegated-salt", provider.SaltFor(t.Context()))
	})
}

func TestAuditEvent_Fields(t *testing.T) {
	t.Parallel()

	t.Run("creates event with all fields", func(t *testing.T) {
		t.Parallel()

		exceptionID := uuid.New()
		occurredAt := time.Now()
		reasonCode := "AMOUNT_CORRECTION"

		event := AuditEvent{
			ExceptionID: exceptionID,
			Action:      "RESOLVED",
			Actor:       "user-456",
			Notes:       "Manual resolution",
			ReasonCode:  &reasonCode,
			OccurredAt:  occurredAt,
			Metadata: map[string]string{
				"source": "api",
				"ip":     "192.168.1.1",
			},
		}

		assert.Equal(t, exceptionID, event.ExceptionID)
		assert.Equal(t, "RESOLVED", event.Action)
		assert.Equal(t, "user-456", event.Actor)
		assert.Equal(t, "Manual resolution", event.Notes)
		assert.NotNil(t, event.ReasonCode)
		assert.Equal(t, "AMOUNT_CORRECTION", *event.ReasonCode)
		assert.Equal(t, occurredAt, event.OccurredAt)
		assert.Len(t, event.Metadata, 2)
		assert.Equal(t, "api", event.Metadata["source"])
	})

	t.Run("creates event with nil optional fields", func(t *testing.T) {
		t.Parallel()

		exceptionID := uuid.New()
		occurredAt := time.Now()

		event := AuditEvent{
			ExceptionID: exceptionID,
			Action:      "CREATED",
			Actor:       "system",
			OccurredAt:  occurredAt,
		}

		assert.Equal(t, exceptionID, event.ExceptionID)
		assert.Equal(t, "CREATED", event.Action)
		assert.Equal(t, "system", event.Actor)
		assert.Equal(t, occurredAt, event.OccurredAt)
		assert.Nil(t, event.ReasonCode)
		assert.Nil(t, event.Metadata)
		assert.Empty(t, event.Notes)
	})
}
