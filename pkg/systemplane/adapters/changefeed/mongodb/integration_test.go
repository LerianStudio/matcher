// Copyright 2025 Lerian Studio.

//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed/feedtest"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"

	mongostore "github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/mongodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func waitForMongoReplicaSet(uri string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err == nil {
			err = client.Ping(ctx, nil)
			_ = client.Disconnect(context.Background())
		}
		cancel()

		if err == nil {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("mongo replica set did not become ready within %s", timeout)
}

// setupMongoDB starts a MongoDB container with a replica set (required for
// change streams and transactions) and returns the connection URI along with
// a cleanup function that terminates the container.
func setupMongoDB(t *testing.T) (string, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := mongodb.Run(ctx,
		"mongo:7",
		mongodb.WithReplicaSet("rs0"),
	)
	require.NoError(t, err, "failed to start MongoDB container")

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err, "failed to get MongoDB connection string")

	if err := waitForMongoReplicaSet(uri, 90*time.Second); err != nil {
		t.Skipf("skipping mongo changefeed integration: %v", err)
	}

	return uri, func() {
		if termErr := container.Terminate(context.Background()); termErr != nil {
			t.Errorf("failed to terminate MongoDB container: %v", termErr)
		}
	}
}

// makeStoreAndFeed creates a mongostore.Store (for writing) and a changefeed
// Feed (for subscribing) sharing the same underlying collection. Each call
// uses a unique database name to isolate sub-tests.
func makeStoreAndFeed(
	t *testing.T,
	uri string,
	watchMode string,
	pollInterval time.Duration,
) (ports.Store, *Feed, func()) {
	t.Helper()

	dbName := fmt.Sprintf("feedtest_%d", time.Now().UnixNano())

	storeCfg := bootstrap.MongoBootstrapConfig{
		URI:               uri,
		Database:          dbName,
		EntriesCollection: "runtime_entries",
		HistoryCollection: "runtime_history",
		WatchMode:         watchMode,
		PollInterval:      pollInterval,
	}

	store, _, storeCloser, err := mongostore.New(context.Background(), storeCfg)
	require.NoError(t, err, "failed to create MongoDB store")

	// Create a separate client for the feed so that closing the store does
	// not invalidate the feed's collection handle (and vice-versa).
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err, "failed to connect feed client")

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Ping(pingCtx, nil)
	pingCancel()
	require.NoError(t, err, "failed to ping feed client")

	entries := client.Database(dbName).Collection("runtime_entries")
	feed := New(entries, watchMode, pollInterval)

	cleanup := func() {
		_ = client.Disconnect(context.Background())

		if closeErr := storeCloser.Close(); closeErr != nil {
			t.Errorf("failed to close MongoDB store client: %v", closeErr)
		}
	}

	return store, feed, cleanup
}

// testTarget returns a deterministic global-scope target for test cases.
func testTarget(t *testing.T) domain.Target {
	t.Helper()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	return target
}

// testActor returns a deterministic actor for test writes.
func testActor() domain.Actor {
	return domain.Actor{ID: "integration-test"}
}

// ---------------------------------------------------------------------------
// Change-stream mode
// ---------------------------------------------------------------------------

func TestMongoChangeFeedChangeStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	uri, containerCleanup := setupMongoDB(t)
	t.Cleanup(containerCleanup)

	t.Run("FeedContract", func(t *testing.T) {
		factory := func(t *testing.T) (ports.Store, ports.ChangeFeed, func()) {
			t.Helper()

			return makeStoreAndFeed(t, uri, WatchModeChangeStream, 500*time.Millisecond)
		}

		feedtest.RunAll(t, factory)
	})

	t.Run("ReceivesSignalOnPut", func(t *testing.T) {
		store, feed, cleanup := makeStoreAndFeed(t, uri, WatchModeChangeStream, 5*time.Second)
		t.Cleanup(cleanup)

		target := testTarget(t)
		actor := testActor()

		// Channel receives the first signal from the handler.
		signalCh := make(chan ports.ChangeSignal, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)

		go func() {
			errCh <- feed.Subscribe(ctx, func(sig ports.ChangeSignal) {
				select {
				case signalCh <- sig:
				default:
				}
			})
		}()

		// Give the change stream a moment to open on the server.
		time.Sleep(500 * time.Millisecond)

		// Write an entry through the store.
		ops := []ports.WriteOp{{Key: "max_retries", Value: 5}}

		newRev, err := store.Put(context.Background(), target, ops, domain.RevisionZero, actor, "test")
		require.NoError(t, err)
		assert.Equal(t, domain.Revision(1), newRev)

		// Wait for the signal with a generous timeout (change stream
		// propagation in a testcontainer can be slow).
		select {
		case sig := <-signalCh:
			assert.Equal(t, target.Kind, sig.Target.Kind)
			assert.Equal(t, target.Scope, sig.Target.Scope)
			assert.Equal(t, target.SubjectID, sig.Target.SubjectID)
			// The revision in the signal comes from the document; it
			// should match the revision written by Put.
			assert.Equal(t, domain.Revision(1), sig.Revision)
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for change stream signal")
		}

		cancel()
		assert.ErrorIs(t, <-errCh, context.Canceled)
	})

	t.Run("ContextCancellationStops", func(t *testing.T) {
		_, feed, cleanup := makeStoreAndFeed(t, uri, WatchModeChangeStream, 5*time.Second)
		t.Cleanup(cleanup)

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)

		go func() {
			errCh <- feed.Subscribe(ctx, func(_ ports.ChangeSignal) {})
		}()

		// Cancel after allowing the stream to open.
		time.Sleep(500 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not return after context cancellation")
		}
	})

	t.Run("MultipleWritesProduceMultipleSignals", func(t *testing.T) {
		store, feed, cleanup := makeStoreAndFeed(t, uri, WatchModeChangeStream, 5*time.Second)
		t.Cleanup(cleanup)

		target := testTarget(t)
		actor := testActor()

		var mu sync.Mutex

		var received []ports.ChangeSignal

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)

		go func() {
			errCh <- feed.Subscribe(ctx, func(sig ports.ChangeSignal) {
				mu.Lock()
				received = append(received, sig)
				mu.Unlock()
			})
		}()

		// Let the stream open.
		time.Sleep(500 * time.Millisecond)

		// First write.
		ops1 := []ports.WriteOp{{Key: "retries", Value: 3}}

		rev1, err := store.Put(context.Background(), target, ops1, domain.RevisionZero, actor, "test")
		require.NoError(t, err)

		// Second write, advancing the revision.
		ops2 := []ports.WriteOp{{Key: "timeout_ms", Value: 5000}}

		_, err = store.Put(context.Background(), target, ops2, rev1, actor, "test")
		require.NoError(t, err)

		// Wait for signals to arrive.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(received) >= 2
		}, 10*time.Second, 200*time.Millisecond, "expected at least 2 signals")

		cancel()
		assert.ErrorIs(t, <-errCh, context.Canceled)

		mu.Lock()
		defer mu.Unlock()

		// Both signals should reference the same target.
		for _, sig := range received {
			assert.Equal(t, target.Kind, sig.Target.Kind)
			assert.Equal(t, target.Scope, sig.Target.Scope)
		}
		assert.Equal(t, domain.Revision(2), received[len(received)-1].Revision)
	})

	t.Run("ResyncMissedSignals", func(t *testing.T) {
		store, feed, cleanup := makeStoreAndFeed(t, uri, WatchModeChangeStream, 5*time.Second)
		t.Cleanup(cleanup)

		target := testTarget(t)
		actor := testActor()

		firstRevision, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "retries", Value: 1}}, domain.RevisionZero, actor, "resync-test")
		require.NoError(t, err)

		known := map[string]pollSnapshot{
			target.String(): {Target: target, Revision: firstRevision},
		}

		secondRevision, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "retries", Value: 2}}, firstRevision, actor, "resync-test")
		require.NoError(t, err)

		var received []ports.ChangeSignal
		err = feed.resyncMissedSignals(context.Background(), known, func(signal ports.ChangeSignal) {
			received = append(received, signal)
		})
		require.NoError(t, err)
		require.Len(t, received, 1)
		assert.Equal(t, secondRevision, received[0].Revision)
		assert.Equal(t, target, received[0].Target)
	})
}

// ---------------------------------------------------------------------------
// Poll mode
// ---------------------------------------------------------------------------

func TestMongoChangeFeedPoll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	uri, containerCleanup := setupMongoDB(t)
	t.Cleanup(containerCleanup)

	t.Run("DetectsRevisionChange", func(t *testing.T) {
		// Use a short poll interval so the test does not wait long.
		store, feed, cleanup := makeStoreAndFeed(t, uri, WatchModePoll, 500*time.Millisecond)
		t.Cleanup(cleanup)

		target := testTarget(t)
		actor := testActor()

		signalCh := make(chan ports.ChangeSignal, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)

		go func() {
			errCh <- feed.Subscribe(ctx, func(sig ports.ChangeSignal) {
				select {
				case signalCh <- sig:
				default:
				}
			})
		}()

		// Wait for the initial poll to complete (poll reads current state
		// before starting the ticker loop).
		time.Sleep(1 * time.Second)

		// Write an entry so the revision advances from 0 to 1.
		ops := []ports.WriteOp{{Key: "batch_size", Value: 100}}

		newRev, err := store.Put(context.Background(), target, ops, domain.RevisionZero, actor, "test")
		require.NoError(t, err)
		assert.Equal(t, domain.Revision(1), newRev)

		// Wait for the next poll cycle to detect the change.
		select {
		case sig := <-signalCh:
			assert.Equal(t, target.Kind, sig.Target.Kind)
			assert.Equal(t, target.Scope, sig.Target.Scope)
			assert.Equal(t, target.SubjectID, sig.Target.SubjectID)
			assert.Equal(t, domain.Revision(1), sig.Revision)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for poll-mode signal")
		}

		cancel()
		assert.ErrorIs(t, <-errCh, context.Canceled)
	})

	t.Run("ContextCancellationStops", func(t *testing.T) {
		_, feed, cleanup := makeStoreAndFeed(t, uri, WatchModePoll, 500*time.Millisecond)
		t.Cleanup(cleanup)

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)

		go func() {
			errCh <- feed.Subscribe(ctx, func(_ ports.ChangeSignal) {})
		}()

		// Let the initial poll snapshot complete.
		time.Sleep(500 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not return after context cancellation")
		}
	})

	t.Run("SubsequentWritesDetected", func(t *testing.T) {
		store, feed, cleanup := makeStoreAndFeed(t, uri, WatchModePoll, 500*time.Millisecond)
		t.Cleanup(cleanup)

		target := testTarget(t)
		actor := testActor()

		var mu sync.Mutex

		var received []ports.ChangeSignal

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)

		go func() {
			errCh <- feed.Subscribe(ctx, func(sig ports.ChangeSignal) {
				mu.Lock()
				received = append(received, sig)
				mu.Unlock()
			})
		}()

		// Wait for initial poll.
		time.Sleep(1 * time.Second)

		// First write.
		ops1 := []ports.WriteOp{{Key: "pool_size", Value: 10}}

		rev1, err := store.Put(context.Background(), target, ops1, domain.RevisionZero, actor, "test")
		require.NoError(t, err)

		// Wait for poll to detect rev 1.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(received) >= 1
		}, 5*time.Second, 200*time.Millisecond, "expected at least 1 signal after first write")

		// Second write.
		ops2 := []ports.WriteOp{{Key: "pool_size", Value: 20}}

		_, err = store.Put(context.Background(), target, ops2, rev1, actor, "test")
		require.NoError(t, err)

		// Wait for poll to detect rev 2.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(received) >= 2
		}, 5*time.Second, 200*time.Millisecond, "expected at least 2 signals after second write")

		cancel()
		assert.ErrorIs(t, <-errCh, context.Canceled)

		mu.Lock()
		defer mu.Unlock()

		// Verify both signals reference the correct target.
		for _, sig := range received {
			assert.Equal(t, target.Kind, sig.Target.Kind)
			assert.Equal(t, target.Scope, sig.Target.Scope)
		}
		assert.Equal(t, domain.Revision(2), received[len(received)-1].Revision)
	})
}
