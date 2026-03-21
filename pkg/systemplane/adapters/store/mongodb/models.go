// Copyright 2025 Lerian Studio.

package mongodb

import (
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// revisionMetaKey is the sentinel key used internally to track per-target
// revision numbers. Documents with this key are excluded from Get() results
// and exist solely for optimistic concurrency bookkeeping.
const revisionMetaKey = "__revision_meta__"

// entryDoc is the BSON-tagged document stored in the entries collection.
// Each document represents a single configuration key/value pair within a
// target (kind/scope/subject). The primary key equivalent is the compound
// index on (kind, scope, subject, key).
type entryDoc struct {
	Kind          string    `bson:"kind"`
	Scope         string    `bson:"scope"`
	Subject       string    `bson:"subject"`
	Key           string    `bson:"key"`
	Value         any       `bson:"value,omitempty"`
	ApplyBehavior string    `bson:"apply_behavior,omitempty"`
	Revision      uint64    `bson:"revision"`
	UpdatedAt     time.Time `bson:"updated_at"`
	UpdatedBy     string    `bson:"updated_by"`
	Source        string    `bson:"source"`
}

// historyDoc is the BSON-tagged document stored in the history collection.
// One history document is created for every key affected by a Put() call,
// capturing the before/after values and the actor who made the change.
type historyDoc struct {
	Kind      string    `bson:"kind"`
	Scope     string    `bson:"scope"`
	Subject   string    `bson:"subject"`
	Key       string    `bson:"key"`
	OldValue  any       `bson:"old_value,omitempty"`
	NewValue  any       `bson:"new_value,omitempty"`
	Revision  uint64    `bson:"revision"`
	ActorID   string    `bson:"actor_id"`
	ChangedAt time.Time `bson:"changed_at"`
	Source    string    `bson:"source"`
}

// toDomainEntry converts an entryDoc back into the domain representation.
func (entryDocument *entryDoc) toDomainEntry() domain.Entry {
	return domain.Entry{
		Kind:      domain.Kind(entryDocument.Kind),
		Scope:     domain.Scope(entryDocument.Scope),
		Subject:   entryDocument.Subject,
		Key:       entryDocument.Key,
		Value:     normalizeBSONValue(entryDocument.Value),
		Revision:  domain.Revision(entryDocument.Revision),
		UpdatedAt: entryDocument.UpdatedAt,
		UpdatedBy: entryDocument.UpdatedBy,
		Source:    entryDocument.Source,
	}
}

func (entryDocument *entryDoc) toDomainEntryWithCodec(codec *secretcodec.Codec) (domain.Entry, error) {
	entry := entryDocument.toDomainEntry()
	if codec == nil {
		return entry, nil
	}

	value, err := codec.Decrypt(domain.Target{Kind: entry.Kind, Scope: entry.Scope, SubjectID: entry.Subject}, entry.Key, entry.Value)
	if err != nil {
		return domain.Entry{}, fmt.Errorf("mongodb models decrypt entry %q: %w", entry.Key, err)
	}

	entry.Value = value

	return entry, nil
}

// toHistoryEntry converts a historyDoc into the ports representation.
func (historyDocument *historyDoc) toHistoryEntry() ports.HistoryEntry {
	return ports.HistoryEntry{
		Revision:  domain.Revision(historyDocument.Revision),
		Key:       historyDocument.Key,
		Scope:     domain.Scope(historyDocument.Scope),
		SubjectID: historyDocument.Subject,
		OldValue:  normalizeBSONValue(historyDocument.OldValue),
		NewValue:  normalizeBSONValue(historyDocument.NewValue),
		ActorID:   historyDocument.ActorID,
		ChangedAt: historyDocument.ChangedAt,
	}
}

func (historyDocument *historyDoc) toHistoryEntryWithCodec(codec *secretcodec.Codec) (ports.HistoryEntry, error) {
	entry := historyDocument.toHistoryEntry()
	if codec == nil {
		return entry, nil
	}

	target := domain.Target{Kind: domain.Kind(historyDocument.Kind), Scope: entry.Scope, SubjectID: entry.SubjectID}

	oldValue, err := codec.Decrypt(target, entry.Key, entry.OldValue)
	if err != nil {
		return ports.HistoryEntry{}, fmt.Errorf("mongodb models decrypt old history value %q: %w", entry.Key, err)
	}

	newValue, err := codec.Decrypt(target, entry.Key, entry.NewValue)
	if err != nil {
		return ports.HistoryEntry{}, fmt.Errorf("mongodb models decrypt new history value %q: %w", entry.Key, err)
	}

	entry.OldValue = oldValue
	entry.NewValue = newValue

	return entry, nil
}

// normalizeBSONDocument converts a bson.D (ordered document) into a plain
// map[string]any, recursively normalizing each element value.
func normalizeBSONDocument(doc bson.D) map[string]any {
	normalized := make(map[string]any, len(doc))
	for _, item := range doc {
		normalized[item.Key] = normalizeBSONValue(item.Value)
	}

	return normalized
}

func normalizeBSONValue(value any) any {
	maxInt := int64(math.MaxInt)
	minInt := int64(math.MinInt)

	switch typedValue := value.(type) {
	case bson.D:
		return normalizeBSONDocument(typedValue)
	case bson.A:
		normalized := make([]any, len(typedValue))
		for index := range typedValue {
			normalized[index] = normalizeBSONValue(typedValue[index])
		}

		return normalized
	case map[string]any:
		normalized := make(map[string]any, len(typedValue))
		for key, item := range typedValue {
			normalized[key] = normalizeBSONValue(item)
		}

		return normalized
	case []any:
		normalized := make([]any, len(typedValue))
		for index := range typedValue {
			normalized[index] = normalizeBSONValue(typedValue[index])
		}

		return normalized
	case int32:
		return int(typedValue)
	case int64:
		if typedValue >= minInt && typedValue <= maxInt {
			return int(typedValue)
		}

		return typedValue
	default:
		return value
	}
}

// newEntryDoc builds an entryDoc from the domain target, write op, and metadata.
func newEntryDoc(target domain.Target, key string, value any, revision domain.Revision,
	actor domain.Actor, source string, now time.Time,
) entryDoc {
	return entryDoc{
		Kind:      target.Kind.String(),
		Scope:     target.Scope.String(),
		Subject:   target.SubjectID,
		Key:       key,
		Value:     value,
		Revision:  revision.Uint64(),
		UpdatedAt: now,
		UpdatedBy: actor.ID,
		Source:    source,
	}
}

// newHistoryDoc builds a historyDoc for a single key mutation.
func newHistoryDoc(target domain.Target, key string, oldValue, newValue any,
	revision domain.Revision, actor domain.Actor, source string, now time.Time,
) historyDoc {
	return historyDoc{
		Kind:      target.Kind.String(),
		Scope:     target.Scope.String(),
		Subject:   target.SubjectID,
		Key:       key,
		OldValue:  oldValue,
		NewValue:  newValue,
		Revision:  revision.Uint64(),
		ActorID:   actor.ID,
		ChangedAt: now,
		Source:    source,
	}
}
