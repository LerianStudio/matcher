//go:build integration

package services

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	outboxRepo "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/outbox"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

type integrationCapturePublishers struct {
	tenantIDs []uuid.UUID
}

func (c *integrationCapturePublishers) PublishIngestionCompleted(context.Context, *sharedDomain.IngestionCompletedEvent) error {
	return nil
}

func (c *integrationCapturePublishers) PublishIngestionFailed(context.Context, *sharedDomain.IngestionFailedEvent) error {
	return nil
}

func (c *integrationCapturePublishers) PublishMatchConfirmed(_ context.Context, event *sharedDomain.MatchConfirmedEvent) error {
	if event != nil {
		c.tenantIDs = append(c.tenantIDs, event.TenantID)
	}

	return nil
}

func (c *integrationCapturePublishers) PublishMatchUnmatched(context.Context, *sharedDomain.MatchUnmatchedEvent) error {
	return nil
}

func TestDispatcher_DispatchAcrossTenants_DBBacked(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())

		tenantA := uuid.New()
		tenantB := uuid.New()
		require.NoError(t, ensureTenantOutboxSchema(t, h, tenantA.String()))
		require.NoError(t, ensureTenantOutboxSchema(t, h, tenantB.String()))

		ctxA := context.WithValue(context.Background(), auth.TenantIDKey, tenantA.String())
		ctxB := context.WithValue(context.Background(), auth.TenantIDKey, tenantB.String())

		payloadA := validMatchConfirmedPayload(tenantA)
		eventA, err := outboxEntities.NewOutboxEvent(ctxA, sharedDomain.EventTypeMatchConfirmed, payloadA.MatchID, mustJSON(t, payloadA))
		require.NoError(t, err)
		createdA, err := repo.Create(ctxA, eventA)
		require.NoError(t, err)

		payloadB := validMatchConfirmedPayload(tenantB)
		eventB, err := outboxEntities.NewOutboxEvent(ctxB, sharedDomain.EventTypeMatchConfirmed, payloadB.MatchID, mustJSON(t, payloadB))
		require.NoError(t, err)
		createdB, err := repo.Create(ctxB, eventB)
		require.NoError(t, err)

		cap := &integrationCapturePublishers{}
		dispatcher, err := NewDispatcher(repo, cap, cap, nil, nil)
		require.NoError(t, err)

		dispatcher.dispatchAcrossTenants(context.Background())
		require.ElementsMatch(t, []uuid.UUID{tenantA, tenantB}, cap.tenantIDs)

		fetchedA, err := repo.GetByID(ctxA, createdA.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusPublished, fetchedA.Status)

		fetchedB, err := repo.GetByID(ctxB, createdB.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusPublished, fetchedB.Status)
	})
}

func ensureTenantOutboxSchema(t *testing.T, h *integration.TestHarness, tenantID string) error {
	t.Helper()

	resolver, err := h.Connection.Resolver(context.Background())
	if err != nil {
		return err
	}

	quotedID := auth.QuoteIdentifier(tenantID)
	queries := []string{
		"CREATE SCHEMA IF NOT EXISTS " + quotedID,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.outbox_events (LIKE public.outbox_events INCLUDING ALL)", quotedID),
	}

	for _, query := range queries {
		if _, err := resolver.ExecContext(context.Background(), query); err != nil {
			return err
		}
	}

	return nil
}

func validMatchConfirmedPayload(tenantID uuid.UUID) sharedDomain.MatchConfirmedEvent {
	return sharedDomain.MatchConfirmedEvent{
		EventType:      sharedDomain.EventTypeMatchConfirmed,
		TenantID:       tenantID,
		ContextID:      uuid.New(),
		RunID:          uuid.New(),
		MatchID:        uuid.New(),
		RuleID:         uuid.New(),
		TransactionIDs: []uuid.UUID{uuid.New(), uuid.New()},
		Confidence:     100,
		ConfirmedAt:    time.Now().UTC(),
		Timestamp:      time.Now().UTC(),
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()

	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}
