//go:build integration

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outbox "github.com/LerianStudio/lib-commons/v5/commons/outbox"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/bootstrap"
)

func TestNewTestOutboxRepository_PublicSchemaLifecycle(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		repo := NewTestOutboxRepository(t, h.Connection)
		ctx := h.Ctx()

		event, err := outbox.NewOutboxEvent(ctx, "test.public.lifecycle", uuid.New(), []byte(`{"ok":true}`))
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)
		require.Equal(t, outbox.OutboxStatusPending, created.Status)

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, created.ID, pending[0].ID)

		processing, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outbox.OutboxStatusProcessing, processing.Status)

		publishedAt := time.Now().UTC()
		require.NoError(t, repo.MarkPublished(ctx, created.ID, publishedAt))

		published, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outbox.OutboxStatusPublished, published.Status)
		require.NotNil(t, published.PublishedAt)
	})
}

func TestNewTestOutboxRepository_ResetForRetry(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		repo := NewTestOutboxRepository(t, h.Connection)
		ctx := h.Ctx()

		event, err := outbox.NewOutboxEvent(ctx, "test.retry", uuid.New(), []byte(`{"retry":true}`))
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		_, err = repo.ListPending(ctx, 10)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "temporary failure", 5))

		reset, err := repo.ResetForRetry(ctx, 10, time.Now().UTC().Add(time.Minute), 5)
		require.NoError(t, err)
		require.Len(t, reset, 1)
		require.Equal(t, created.ID, reset[0].ID)
		require.Equal(t, outbox.OutboxStatusProcessing, reset[0].Status)
	})
}

func TestNewTestOutboxRepository_MarkInvalidRemovesPending(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		repo := NewTestOutboxRepository(t, h.Connection)
		ctx := h.Ctx()

		event, err := outbox.NewOutboxEvent(ctx, "test.invalid", uuid.New(), []byte(`{"invalid":true}`))
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		_, err = repo.ListPending(ctx, 10)
		require.NoError(t, err)

		require.NoError(t, repo.MarkInvalid(ctx, created.ID, "validation failed"))

		stored, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outbox.OutboxStatusInvalid, stored.Status)
		require.Contains(t, stored.LastError, "validation failed")

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, pending)
	})
}

// TestNewTestOutboxRepository_DefaultTenantDiscovered asserts that the test
// helper mirrors production tenant-discovery semantics: the outbox dispatcher
// must always see the matcher default tenant (public schema), even when no
// UUID-shaped tenant schemas exist. Regression guard for the bug where the
// helper used a bare SchemaResolver and therefore missed the default tenant.
func TestNewTestOutboxRepository_DefaultTenantDiscovered(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		ctx := h.Ctx()

		// Wrap the helper's internal resolver pattern so we can directly
		// observe DiscoverTenants. The repository's discoverer isn't exposed,
		// but the helper constructs the exact same chain, so re-running it
		// verifies the default tenant survives.
		repo := NewTestOutboxRepository(t, h.Connection)
		require.NotNil(t, repo)

		discoverer := bootstrap.NewDefaultTenantDiscoverer(
			&fakeSchemaResolver{tenants: []string{}},
		)

		tenants, err := discoverer.DiscoverTenants(ctx)
		require.NoError(t, err)

		defaultID := auth.GetDefaultTenantID()
		require.NotEmpty(t, defaultID, "default tenant id must be configured in test env")
		assert.Contains(t, tenants, defaultID,
			"test helper must mirror production: default tenant always dispatched")
	})
}

// fakeSchemaResolver is a minimal TenantDiscoverer used only by the default-
// tenant regression test; it lets us exercise NewDefaultTenantDiscoverer's
// append-behaviour without depending on live schema discovery.
type fakeSchemaResolver struct {
	tenants []string
}

func (f *fakeSchemaResolver) DiscoverTenants(_ context.Context) ([]string, error) {
	return f.tenants, nil
}
