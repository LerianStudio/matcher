// Copyright 2025 Lerian Studio.

//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/storetest"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
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

// newMongoFactory returns a storetest.Factory that creates fresh, isolated
// Store + HistoryStore instances per sub-test. Each invocation uses a unique
// database name so that sub-tests running sequentially against the same
// container do not interfere with one another.
func newMongoFactory(uri string) storetest.Factory {
	return func(t *testing.T) (ports.Store, ports.HistoryStore, func()) {
		t.Helper()

		// Unique database per sub-test avoids cross-contamination.
		dbName := fmt.Sprintf("test_%d", time.Now().UnixNano())

		cfg := bootstrap.MongoBootstrapConfig{
			URI:               uri,
			Database:          dbName,
			EntriesCollection: "runtime_entries",
			HistoryCollection: "runtime_history",
			WatchMode:         "change_stream",
			PollInterval:      5 * time.Second,
		}

		ctx := context.Background()

		store, history, closer, err := New(ctx, cfg)
		require.NoError(t, err, "failed to create MongoDB store for sub-test")

		return store, history, func() {
			if closeErr := closer.Close(); closeErr != nil {
				t.Errorf("failed to close MongoDB client: %v", closeErr)
			}
		}
	}
}

// TestMongoStoreContracts runs the full contract test suite against a real
// MongoDB instance using testcontainers. MongoDB transactions (used by the
// Store.Put method for atomicity and optimistic concurrency) require a
// replica set, so the container is started with mongodb.WithReplicaSet.
func TestMongoStoreContracts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Start MongoDB container with replica set (required for transactions).
	container, err := mongodb.Run(ctx,
		"mongo:7",
		mongodb.WithReplicaSet("rs0"),
	)
	require.NoError(t, err, "failed to start MongoDB container")

	t.Cleanup(func() {
		if termErr := container.Terminate(context.Background()); termErr != nil {
			t.Errorf("failed to terminate MongoDB container: %v", termErr)
		}
	})

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err, "failed to get MongoDB connection string")

	if err := waitForMongoReplicaSet(uri, 90*time.Second); err != nil {
		t.Skipf("skipping mongo integration contract: %v", err)
	}

	// Run the full contract test suite.
	factory := newMongoFactory(uri)
	storetest.RunAll(t, factory)
}
