// Copyright 2025 Lerian Studio.

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.Store = (*Store)(nil)

var (
	errEntryValueNotFound = errors.New("mongodb store: entry value not found")
	errUnexpectedTxResult = errors.New("mongodb store: unexpected transaction result type")
	// ErrNilStore is returned when a method is called on a nil Store receiver.
	ErrNilStore = errors.New("mongodb store: store is nil")
	// ErrNilClient is returned when a Store is constructed with a nil MongoDB client.
	ErrNilClient = errors.New("mongodb store: client is nil")
	// ErrNilEntries is returned when a Store is constructed with a nil entries collection.
	ErrNilEntries = errors.New("mongodb store: entries collection is nil")
	// ErrNilHistory is returned when a Store is constructed with a nil history collection.
	ErrNilHistory = errors.New("mongodb store: history collection is nil")
)

// Store implements ports.Store backed by MongoDB. Configuration entries are
// stored in one collection and history records in another. Optimistic
// concurrency is achieved by maintaining a per-target revision counter
// stored as a sentinel document (key = revisionMetaKey) in the entries
// collection. All mutations run inside a multi-document transaction to
// guarantee atomicity.
type Store struct {
	client         *mongo.Client
	entries        *mongo.Collection
	history        *mongo.Collection
	secretCodec    *secretcodec.Codec
	applyBehaviors map[string]domain.ApplyBehavior
}

// txResult carries the outcome of a Put transaction callback.
type txResult struct {
	newRevision     domain.Revision
	currentRevision domain.Revision
}

// EntriesCollection returns the underlying entries collection reference.
func (store *Store) EntriesCollection() *mongo.Collection {
	if store == nil {
		return nil
	}

	return store.entries
}

// Get retrieves all configuration entries for a target at its current
// revision. The internal revision-tracking document (revisionMetaKey) is
// excluded from the returned entries. If the target has never been written,
// Get returns an empty slice and RevisionZero.
func (store *Store) Get(ctx context.Context, target domain.Target) (ports.ReadResult, error) {
	if err := store.validateReadDependencies(); err != nil {
		return ports.ReadResult{}, err
	}

	type readResult struct {
		entries  []domain.Entry
		revision domain.Revision
	}

	session, err := store.client.StartSession()
	if err != nil {
		return ports.ReadResult{}, fmt.Errorf("mongodb store get: start session: %w", err)
	}
	defer session.EndSession(ctx)

	raw, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		revision, readErr := store.currentRevisionInCollection(ctx, store.entries, target)
		if readErr != nil {
			return nil, fmt.Errorf("read revision: %w", readErr)
		}

		entries, readErr := store.readEntriesInCollection(ctx, store.entries, target)
		if readErr != nil {
			return nil, fmt.Errorf("read entries: %w", readErr)
		}

		return readResult{entries: entries, revision: revision}, nil
	})
	if err != nil {
		return ports.ReadResult{}, fmt.Errorf("mongodb store get: %w", err)
	}

	result, ok := raw.(readResult)
	if !ok {
		return ports.ReadResult{}, errUnexpectedTxResult
	}

	return ports.ReadResult{Entries: result.entries, Revision: result.revision}, nil
}

func (store *Store) readEntriesInCollection(ctx context.Context, coll *mongo.Collection, target domain.Target) ([]domain.Entry, error) {
	if coll == nil {
		return nil, ErrNilEntries
	}

	filter := targetFilter(target)
	filter = append(filter, bson.E{Key: "key", Value: bson.D{{Key: "$ne", Value: revisionMetaKey}}})

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find entries: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []entryDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode entries: %w", err)
	}

	if len(docs) == 0 {
		return nil, nil
	}

	entries := make([]domain.Entry, len(docs))
	for i := range docs {
		entry, err := docs[i].toDomainEntryWithCodec(store.secretCodec)
		if err != nil {
			return nil, fmt.Errorf("decode secret entry: %w", err)
		}

		entries[i] = entry
	}

	return entries, nil
}

// Put atomically writes a batch of operations for a target. It enforces
// optimistic concurrency: if expected does not match the current revision,
// Put returns the current revision along with domain.ErrRevisionMismatch.
//
// For each WriteOp:
//   - If Reset is true or Value is nil, the entry is deleted.
//   - Otherwise, the entry is upserted with the new value and metadata.
//
// A history document is inserted for every operation regardless of whether
// it is a set or a reset.
func (store *Store) Put(ctx context.Context, target domain.Target, ops []ports.WriteOp,
	expected domain.Revision, actor domain.Actor, source string,
) (domain.Revision, error) {
	if err := store.validateWriteDependencies(); err != nil {
		return domain.RevisionZero, err
	}

	if len(ops) == 0 {
		revision, err := store.currentRevision(ctx, target)
		if err != nil {
			return domain.RevisionZero, fmt.Errorf("mongodb store put: read revision for empty batch: %w", err)
		}

		return revision, nil
	}

	session, err := store.client.StartSession()
	if err != nil {
		return domain.RevisionZero, fmt.Errorf("mongodb store put: start session: %w", err)
	}
	defer session.EndSession(ctx)

	raw, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return store.putTransaction(ctx, target, ops, expected, actor, source)
	})

	return store.interpretPutResult(raw, err)
}

// putTransaction executes the core Put logic inside a MongoDB transaction.
func (store *Store) putTransaction(
	ctx context.Context,
	target domain.Target,
	ops []ports.WriteOp,
	expected domain.Revision,
	actor domain.Actor,
	source string,
) (txResult, error) {
	current, readErr := store.currentRevisionInCollection(ctx, store.entries, target)
	if readErr != nil {
		return txResult{}, fmt.Errorf("read revision: %w", readErr)
	}

	if expected != current {
		return txResult{currentRevision: current}, domain.ErrRevisionMismatch
	}

	newRevision := expected.Next()
	now := time.Now().UTC()

	for _, operation := range ops {
		if applyErr := store.applyOperation(ctx, target, operation, newRevision, actor, source, now); applyErr != nil {
			return txResult{}, fmt.Errorf("apply operation for %q: %w", operation.Key, applyErr)
		}
	}

	if metaErr := store.upsertRevisionMeta(ctx, target, newRevision, actor, source, now, store.escalateBehavior(ops)); metaErr != nil {
		return txResult{}, fmt.Errorf("upsert revision meta: %w", metaErr)
	}

	return txResult{newRevision: newRevision}, nil
}

// interpretPutResult converts the raw transaction output and error into the
// Put return values, handling revision-mismatch extraction.
func (store *Store) interpretPutResult(raw any, err error) (domain.Revision, error) {
	if err != nil {
		if errors.Is(err, domain.ErrRevisionMismatch) {
			if result, ok := raw.(txResult); ok {
				return result.currentRevision, domain.ErrRevisionMismatch
			}

			return domain.RevisionZero, domain.ErrRevisionMismatch
		}

		return domain.RevisionZero, fmt.Errorf("mongodb store put: %w", err)
	}

	result, ok := raw.(txResult)
	if !ok {
		return domain.RevisionZero, errUnexpectedTxResult
	}

	return result.newRevision, nil
}
