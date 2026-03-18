// Copyright 2025 Lerian Studio.

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
)

var (
	errEmptyURI                = errors.New("mongodb store: URI is required")
	errTransactionsUnsupported = errors.New("mongodb store: deployment does not support transactions")
)

// clientCloser wraps a mongo.Client so that Close() disconnects it.
type clientCloser struct {
	client *mongo.Client
}

// Close disconnects the underlying MongoDB client.
func (c *clientCloser) Close() error {
	if c == nil || c.client == nil {
		return nil
	}

	if err := c.client.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("mongodb store: disconnect client: %w", err)
	}

	return nil
}

// New creates a connected MongoDB Store and HistoryStore from the given
// bootstrap configuration. It performs a ping to verify connectivity and
// creates the required indexes on both collections.
//
// The returned io.Closer must be called during shutdown to disconnect the
// MongoDB client. Callers typically defer closer.Close().
//
// Returns an error if the URI is empty, the connection fails, or index
// creation fails.
func New(ctx context.Context, cfg bootstrap.MongoBootstrapConfig) (*Store, *HistoryStore, io.Closer, error) {
	if cfg.URI == "" {
		return nil, nil, nil, errEmptyURI
	}

	client, err := mongo.Connect(options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("mongodb store: connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		// Best-effort disconnect on ping failure.
		_ = client.Disconnect(ctx)

		return nil, nil, nil, fmt.Errorf("mongodb store: ping: %w", err)
	}

	if err := ensureTransactionSupport(ctx, client); err != nil {
		_ = client.Disconnect(ctx)

		return nil, nil, nil, err
	}

	db := client.Database(cfg.Database)
	entriesColl := db.Collection(cfg.EntriesCollection)
	historyColl := db.Collection(cfg.HistoryCollection)

	if err := ensureIndexes(ctx, entriesColl, historyColl); err != nil {
		_ = client.Disconnect(ctx)

		return nil, nil, nil, fmt.Errorf("mongodb store: create indexes: %w", err)
	}

	store := &Store{
		client:  client,
		entries: entriesColl,
		history: historyColl,
	}

	historyStore := &HistoryStore{
		history: historyColl,
	}

	closer := &clientCloser{client: client}

	return store, historyStore, closer, nil
}

type helloResult struct {
	SetName                      string `bson:"setName"`
	Msg                          string `bson:"msg"`
	LogicalSessionTimeoutMinutes *int32 `bson:"logicalSessionTimeoutMinutes"`
}

func ensureTransactionSupport(ctx context.Context, client *mongo.Client) error {
	if client == nil {
		return fmt.Errorf("%w: client is nil", errTransactionsUnsupported)
	}

	var result helloResult
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&result); err != nil {
		return fmt.Errorf("mongodb store: check transaction support: %w", err)
	}

	if supportsTransactions(result) {
		return nil
	}

	return errTransactionsUnsupported
}

func supportsTransactions(result helloResult) bool {
	if result.LogicalSessionTimeoutMinutes == nil {
		return false
	}

	return result.SetName != "" || result.Msg == "isdbgrid"
}

// ensureIndexes creates the required indexes on the entries and history
// collections. The entries collection gets a unique compound index on
// (kind, scope, subject, key) to enforce the one-document-per-key
// invariant. The history collection gets a compound index for target+key
// lookups and a descending index on changed_at for efficient newest-first
// queries.
func ensureIndexes(ctx context.Context, entries, history *mongo.Collection) error {
	entriesModels := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "kind", Value: 1},
				{Key: "scope", Value: 1},
				{Key: "subject", Value: 1},
				{Key: "key", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
	}

	if _, err := entries.Indexes().CreateMany(ctx, entriesModels); err != nil {
		return fmt.Errorf("entries indexes: %w", err)
	}

	historyModels := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "kind", Value: 1},
				{Key: "scope", Value: 1},
				{Key: "subject", Value: 1},
				{Key: "key", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "changed_at", Value: -1},
			},
		},
	}

	if _, err := history.Indexes().CreateMany(ctx, historyModels); err != nil {
		return fmt.Errorf("history indexes: %w", err)
	}

	return nil
}
