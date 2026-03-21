// Copyright 2025 Lerian Studio.

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func (store *Store) applyOperation(ctx context.Context, target domain.Target,
	operation ports.WriteOp, newRevision domain.Revision,
	actor domain.Actor, source string, now time.Time,
) error {
	oldValue, lookupErr := store.lookupOldValue(ctx, target, operation.Key)
	if lookupErr != nil && !errors.Is(lookupErr, errEntryValueNotFound) {
		return fmt.Errorf("lookup old value: %w", lookupErr)
	}

	if errors.Is(lookupErr, errEntryValueNotFound) {
		oldValue = nil
	}

	if operation.Reset || domain.IsNilValue(operation.Value) {
		if deleteErr := store.deleteEntry(ctx, target, operation.Key); deleteErr != nil {
			return fmt.Errorf("delete entry: %w", deleteErr)
		}

		hDoc := newHistoryDoc(target, operation.Key, oldValue, nil, newRevision, actor, source, now)
		if _, insertErr := store.history.InsertOne(ctx, hDoc); insertErr != nil {
			return fmt.Errorf("insert history: %w", insertErr)
		}

		return nil
	}

	valueForStorage, err := store.encryptValue(target, operation.Key, operation.Value)
	if err != nil {
		return fmt.Errorf("encrypt new value: %w", err)
	}

	doc := newEntryDoc(target, operation.Key, valueForStorage, newRevision, actor, source, now)
	if upsertErr := store.upsertEntry(ctx, target, operation.Key, doc); upsertErr != nil {
		return fmt.Errorf("upsert entry: %w", upsertErr)
	}

	valueForHistory, err := store.encryptValue(target, operation.Key, operation.Value)
	if err != nil {
		return fmt.Errorf("encrypt history value: %w", err)
	}

	hDoc := newHistoryDoc(target, operation.Key, oldValue, valueForHistory, newRevision, actor, source, now)
	if _, insertErr := store.history.InsertOne(ctx, hDoc); insertErr != nil {
		return fmt.Errorf("insert history: %w", insertErr)
	}

	return nil
}

func (store *Store) encryptValue(target domain.Target, key string, value any) (any, error) {
	if store == nil || store.secretCodec == nil {
		return value, nil
	}

	encryptedValue, err := store.secretCodec.Encrypt(target, key, value)
	if err != nil {
		return nil, fmt.Errorf("mongodb store encrypt value %q: %w", key, err)
	}

	return encryptedValue, nil
}

// Internal helpers.

// targetFilter builds the base BSON filter matching a target's kind, scope,
// and subject.
func targetFilter(target domain.Target) bson.D {
	return bson.D{
		{Key: "kind", Value: target.Kind.String()},
		{Key: "scope", Value: target.Scope.String()},
		{Key: "subject", Value: target.SubjectID},
	}
}

// revisionMetaFilter builds a filter matching the revision-tracking sentinel
// document for a given target.
func revisionMetaFilter(target domain.Target) bson.D {
	f := targetFilter(target)
	return append(f, bson.E{Key: "key", Value: revisionMetaKey})
}

// currentRevision reads the current revision for a target from the entries
// collection. Returns RevisionZero when no meta document exists.
func (store *Store) currentRevision(ctx context.Context, target domain.Target) (domain.Revision, error) {
	return store.currentRevisionInCollection(ctx, store.entries, target)
}

// currentRevisionInCollection reads the revision from a specific collection
// reference. This allows callers within a transaction to pass the
// session-scoped collection.
func (store *Store) currentRevisionInCollection(ctx context.Context, coll *mongo.Collection,
	target domain.Target,
) (domain.Revision, error) {
	if coll == nil {
		return domain.RevisionZero, ErrNilEntries
	}

	filter := revisionMetaFilter(target)

	var doc entryDoc

	err := coll.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return domain.RevisionZero, nil
		}

		return domain.RevisionZero, fmt.Errorf("find revision meta: %w", err)
	}

	return domain.Revision(doc.Revision), nil
}

// lookupOldValue retrieves the current value for an entry key before a
// mutation. Returns nil when the entry does not exist.
func (store *Store) lookupOldValue(ctx context.Context, target domain.Target, key string) (any, error) {
	filter := targetFilter(target)
	filter = append(filter, bson.E{Key: "key", Value: key})

	var doc entryDoc

	err := store.entries.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errEntryValueNotFound
		}

		return nil, fmt.Errorf("lookup old value: %w", err)
	}

	return doc.Value, nil
}

// deleteEntry removes a single entry document from the entries collection.
func (store *Store) deleteEntry(ctx context.Context, target domain.Target, key string) error {
	filter := targetFilter(target)
	filter = append(filter, bson.E{Key: "key", Value: key})

	_, err := store.entries.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete entry: %w", err)
	}

	return nil
}

// upsertEntry replaces or inserts an entry document, matching on the
// compound key (kind, scope, subject, key).
func (store *Store) upsertEntry(ctx context.Context, target domain.Target, key string, doc entryDoc) error {
	filter := targetFilter(target)
	filter = append(filter, bson.E{Key: "key", Value: key})

	opts := options.Replace().SetUpsert(true)

	_, err := store.entries.ReplaceOne(ctx, filter, doc, opts)
	if err != nil {
		return fmt.Errorf("upsert entry: %w", err)
	}

	return nil
}

// upsertRevisionMeta updates (or creates) the revision-tracking sentinel
// document for a target.
func (store *Store) upsertRevisionMeta(ctx context.Context, target domain.Target,
	revision domain.Revision, actor domain.Actor, source string, now time.Time, behavior domain.ApplyBehavior,
) error {
	doc := newEntryDoc(target, revisionMetaKey, nil, revision, actor, source, now)
	doc.ApplyBehavior = string(behavior)

	return store.upsertEntry(ctx, target, revisionMetaKey, doc)
}

func (store *Store) escalateBehavior(ops []ports.WriteOp) domain.ApplyBehavior {
	if store == nil {
		return domain.ApplyBundleRebuild
	}

	escalation := domain.ApplyLiveRead

	for _, op := range ops {
		behavior, ok := store.applyBehaviors[op.Key]
		if !ok {
			return domain.ApplyBundleRebuild
		}

		if behavior.Strength() > escalation.Strength() {
			escalation = behavior
		}
	}

	return escalation
}

func (store *Store) validateReadDependencies() error {
	if store == nil {
		return ErrNilStore
	}

	if store.client == nil {
		return ErrNilClient
	}

	if store.entries == nil {
		return ErrNilEntries
	}

	return nil
}

func (store *Store) validateWriteDependencies() error {
	if store == nil {
		return ErrNilStore
	}

	if store.client == nil {
		return ErrNilClient
	}

	if store.entries == nil {
		return ErrNilEntries
	}

	if store.history == nil {
		return ErrNilHistory
	}

	return nil
}
