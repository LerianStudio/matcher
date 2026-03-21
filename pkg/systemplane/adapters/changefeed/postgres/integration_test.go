// Copyright 2025 Lerian Studio.

//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver registration

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed/feedtest"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"

	pgstore "github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/postgres"
)

// startPostgresContainer creates a PostgreSQL 17 testcontainer and returns
// its DSN. The container is terminated when the test finishes.
func startPostgresContainer(t *testing.T, ctx context.Context) string {
	t.Helper()

	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("feedtest"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	return dsn
}

// newTestStore creates a PG-backed Store with a unique schema for isolation.
// Returns the store and a cleanup function that closes the connection.
func newTestStore(t *testing.T, ctx context.Context, dsn, notifyChannel string) ports.Store {
	store, _ := newTestStoreWithSchema(t, ctx, dsn, notifyChannel)

	return store
}

func newTestStoreWithSchema(t *testing.T, ctx context.Context, dsn, notifyChannel string) (ports.Store, string) {
	t.Helper()

	schema := fmt.Sprintf("feed_%d", time.Now().UnixNano())

	cfg := &bootstrap.PostgresBootstrapConfig{
		DSN:           dsn,
		Schema:        schema,
		EntriesTable:  "runtime_entries",
		HistoryTable:  "runtime_history",
		NotifyChannel: notifyChannel,
	}

	store, _, closer, err := pgstore.New(ctx, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		closer.Close()
	})

	return store, schema
}

// buildTestTarget creates a valid global config target for testing.
func buildTestTarget(t *testing.T) domain.Target {
	t.Helper()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	return target
}

// TestPostgresChangeFeed verifies the full LISTEN/NOTIFY roundtrip:
// Store.Put() fires pg_notify, and a Feed subscriber receives the
// corresponding ChangeSignal.
func TestPostgresChangeFeed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := startPostgresContainer(t, ctx)

	t.Run("FeedContract", func(t *testing.T) {
		factory := func(t *testing.T) (ports.Store, ports.ChangeFeed, func()) {
			t.Helper()

			channel := fmt.Sprintf("contract_changes_%d", time.Now().UnixNano())
			store := newTestStore(t, ctx, dsn, channel)
			feed := New(dsn, channel, WithReconnectBounds(100*time.Millisecond, 1*time.Second))

			return store, feed, func() {}
		}

		feedtest.RunAll(t, factory)
	})

	const notifyChannel = "test_changes"

	store := newTestStore(t, ctx, dsn, notifyChannel)
	target := buildTestTarget(t)
	actor := domain.Actor{ID: "test-actor"}

	t.Run("ReceivesSignalOnPut", func(t *testing.T) {
		var mu sync.Mutex

		var received []ports.ChangeSignal

		handler := func(signal ports.ChangeSignal) {
			mu.Lock()
			received = append(received, signal)
			mu.Unlock()
		}

		feed := New(dsn, notifyChannel,
			WithReconnectBounds(100*time.Millisecond, 1*time.Second),
		)

		feedCtx, feedCancel := context.WithCancel(ctx)
		done := make(chan error, 1)

		go func() {
			done <- feed.Subscribe(feedCtx, handler)
		}()

		// Allow the LISTEN command to register on the connection.
		time.Sleep(300 * time.Millisecond)

		ops := []ports.WriteOp{
			{Key: "log_level", Value: "debug"},
		}

		rev, err := store.Put(ctx, target, ops, domain.RevisionZero, actor, "integration-test")
		require.NoError(t, err)
		assert.Equal(t, domain.Revision(1), rev)

		// Wait for the notification to arrive.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(received) >= 1
		}, 5*time.Second, 50*time.Millisecond, "expected at least 1 signal within timeout")

		mu.Lock()
		signal := received[0]
		mu.Unlock()

		assert.Equal(t, target.Kind, signal.Target.Kind)
		assert.Equal(t, target.Scope, signal.Target.Scope)
		assert.Equal(t, target.SubjectID, signal.Target.SubjectID)
		assert.Equal(t, domain.Revision(1), signal.Revision)

		feedCancel()

		select {
		case subErr := <-done:
			assert.ErrorIs(t, subErr, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not return after context cancellation")
		}
	})

	t.Run("MultipleSignals", func(t *testing.T) {
		// Create a dedicated store with a fresh schema to avoid revision
		// conflicts from the previous sub-test.
		multiStore := newTestStore(t, ctx, dsn, notifyChannel)
		multiTarget := buildTestTarget(t)

		var mu sync.Mutex

		var received []ports.ChangeSignal

		handler := func(signal ports.ChangeSignal) {
			mu.Lock()
			received = append(received, signal)
			mu.Unlock()
		}

		feed := New(dsn, notifyChannel,
			WithReconnectBounds(100*time.Millisecond, 1*time.Second),
		)

		feedCtx, feedCancel := context.WithCancel(ctx)
		done := make(chan error, 1)

		go func() {
			done <- feed.Subscribe(feedCtx, handler)
		}()

		time.Sleep(300 * time.Millisecond)

		// Perform 3 sequential puts, bumping revision each time.
		expectedRevision := domain.RevisionZero

		for i := range 3 {
			ops := []ports.WriteOp{
				{Key: "retry_count", Value: i + 1},
			}

			newRev, err := multiStore.Put(ctx, multiTarget, ops, expectedRevision, actor, "multi-signal-test")
			require.NoError(t, err, "Put %d failed", i+1)

			expectedRevision = newRev
		}

		// Wait for at least 3 signals.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(received) >= 3
		}, 5*time.Second, 50*time.Millisecond, "expected at least 3 signals within timeout")

		mu.Lock()
		signals := make([]ports.ChangeSignal, len(received))
		copy(signals, received)
		mu.Unlock()

		// Verify we received signals with increasing revisions.
		assert.GreaterOrEqual(t, len(signals), 3,
			"expected at least 3 signals, got %d", len(signals))

		for i := range 3 {
			assert.Equal(t, domain.Revision(i+1), signals[i].Revision,
				"signal %d should have revision %d", i, i+1)
		}

		feedCancel()

		select {
		case subErr := <-done:
			assert.ErrorIs(t, subErr, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not return after context cancellation")
		}
	})

	t.Run("ContextCancellationStops", func(t *testing.T) {
		feed := New(dsn, notifyChannel,
			WithReconnectBounds(100*time.Millisecond, 1*time.Second),
		)

		// Cancel quickly after the listener has started.
		feedCtx, feedCancel := context.WithCancel(ctx)
		done := make(chan error, 1)

		go func() {
			done <- feed.Subscribe(feedCtx, func(_ ports.ChangeSignal) {
				// No-op handler; this test only checks shutdown behavior.
			})
		}()

		// Give the listener time to issue LISTEN, then cancel.
		time.Sleep(300 * time.Millisecond)
		feedCancel()

		select {
		case subErr := <-done:
			assert.ErrorIs(t, subErr, context.Canceled,
				"Subscribe should return context.Canceled after cancellation")
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not return within 5 seconds after context cancellation")
		}
	})

	t.Run("ResyncMissedSignals", func(t *testing.T) {
		resyncStore, schema := newTestStoreWithSchema(t, ctx, dsn, notifyChannel)
		resyncTarget := buildTestTarget(t)
		feed := New(
			dsn,
			notifyChannel,
			WithReconnectBounds(100*time.Millisecond, 1*time.Second),
			WithRevisionSource(schema, bootstrap.DefaultPostgresRevisionTable),
		)

		actor := domain.Actor{ID: "resync-actor"}
		firstRevision, err := resyncStore.Put(ctx, resyncTarget, []ports.WriteOp{{Key: "retries", Value: 1}}, domain.RevisionZero, actor, "resync-test")
		require.NoError(t, err)

		known := map[string]trackedRevision{
			resyncTarget.String(): {Target: resyncTarget, Revision: firstRevision},
		}

		secondRevision, err := resyncStore.Put(ctx, resyncTarget, []ports.WriteOp{{Key: "retries", Value: 2}}, firstRevision, actor, "resync-test")
		require.NoError(t, err)

		var received []ports.ChangeSignal
		err = feed.resyncMissedSignals(ctx, known, func(signal ports.ChangeSignal) {
			received = append(received, signal)
		})
		require.NoError(t, err)
		require.Len(t, received, 1)
		assert.Equal(t, secondRevision, received[0].Revision)
		assert.Equal(t, resyncTarget, received[0].Target)
	})

	t.Run("TenantScopedSignal", func(t *testing.T) {
		// Verify that tenant-scoped targets propagate correctly through
		// the NOTIFY → LISTEN → parsePayload roundtrip.
		tenantStore := newTestStore(t, ctx, dsn, notifyChannel)

		tenantTarget, err := domain.NewTarget(
			domain.KindSetting,
			domain.ScopeTenant,
			"550e8400-e29b-41d4-a716-446655440000",
		)
		require.NoError(t, err)

		var mu sync.Mutex

		var received []ports.ChangeSignal

		handler := func(signal ports.ChangeSignal) {
			mu.Lock()
			received = append(received, signal)
			mu.Unlock()
		}

		feed := New(dsn, notifyChannel,
			WithReconnectBounds(100*time.Millisecond, 1*time.Second),
		)

		feedCtx, feedCancel := context.WithCancel(ctx)
		done := make(chan error, 1)

		go func() {
			done <- feed.Subscribe(feedCtx, handler)
		}()

		time.Sleep(300 * time.Millisecond)

		ops := []ports.WriteOp{
			{Key: "theme", Value: "dark"},
		}

		rev, err := tenantStore.Put(ctx, tenantTarget, ops, domain.RevisionZero, actor, "tenant-test")
		require.NoError(t, err)
		assert.Equal(t, domain.Revision(1), rev)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(received) >= 1
		}, 5*time.Second, 50*time.Millisecond, "expected tenant signal within timeout")

		mu.Lock()
		signal := received[0]
		mu.Unlock()

		assert.Equal(t, domain.KindSetting, signal.Target.Kind)
		assert.Equal(t, domain.ScopeTenant, signal.Target.Scope)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", signal.Target.SubjectID)
		assert.Equal(t, domain.Revision(1), signal.Revision)

		feedCancel()

		select {
		case subErr := <-done:
			assert.ErrorIs(t, subErr, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not return after context cancellation")
		}
	})
}
