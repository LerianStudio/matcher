// Copyright 2025 Lerian Studio.

// Package mongodb provides MongoDB-backed change feed adapters.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	basechangefeed "github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Watch modes supported by the MongoDB change feed.
const (
	WatchModeChangeStream             = "change_stream"
	WatchModePoll                     = "poll"
	defaultChangeStreamReconnectDelay = 1 * time.Second
)

// revisionMetaKey must match the sentinel key used by the MongoDB store adapter
// so the poll-based feed can locate per-target revision documents.
const revisionMetaKey = "__revision_meta__"

// Feed implements ports.ChangeFeed using MongoDB change streams or polling.
// In change-stream mode it opens a server-side change stream on the entries
// collection and translates insert/update/replace/delete events into
// ChangeSignal values. In poll mode it periodically queries the revision-meta
// documents and emits a signal whenever a target's revision advances.
type Feed struct {
	entries      *mongo.Collection
	watchMode    string
	pollInterval time.Duration
}

var (
	// ErrNilFeed is returned when Subscribe is called on a nil receiver.
	ErrNilFeed = errors.New("mongodb feed: feed is nil")
	// ErrNilEntries is returned when the entries collection is not configured.
	ErrNilEntries = errors.New("mongodb feed: entries collection is nil")
	// ErrNilFeedHandler is returned when Subscribe receives a nil handler.
	ErrNilFeedHandler = errors.New("mongodb feed: handler is nil")
	// ErrUnsupportedMode is returned when watch mode is not recognized.
	ErrUnsupportedMode = errors.New("mongodb feed: unsupported watch mode")
)

// Compile-time interface check.
var _ ports.ChangeFeed = (*Feed)(nil)

// New creates a MongoDB change feed. If watchMode is empty it defaults to
// WatchModeChangeStream. If pollInterval is zero or negative it defaults to
// 5 seconds.
func New(entries *mongo.Collection, watchMode string, pollInterval time.Duration) *Feed {
	if watchMode == "" {
		watchMode = WatchModeChangeStream
	}

	if pollInterval <= 0 {
		pollInterval = bootstrap.DefaultMongoPollInterval
	}

	return &Feed{
		entries:      entries,
		watchMode:    watchMode,
		pollInterval: pollInterval,
	}
}

// Subscribe registers a handler that is called whenever a configuration target
// is updated. The method blocks until ctx is cancelled or an unrecoverable
// error occurs. The concrete notification mechanism is determined by the
// feed's watchMode.
func (feed *Feed) Subscribe(ctx context.Context, handler func(ports.ChangeSignal)) error {
	if feed == nil {
		return ErrNilFeed
	}

	if feed.entries == nil {
		return ErrNilEntries
	}

	if handler == nil {
		return ErrNilFeedHandler
	}

	switch feed.watchMode {
	case WatchModeChangeStream:
		return feed.subscribeChangeStream(ctx, handler)
	case WatchModePoll:
		return feed.subscribePoll(ctx, handler)
	default:
		return fmt.Errorf("%w %q", ErrUnsupportedMode, feed.watchMode)
	}
}

// Change-stream mode.

func (feed *Feed) subscribeChangeStream(ctx context.Context, handler func(ports.ChangeSignal)) error {
	known, err := feed.pollRevisions(ctx)
	if err != nil {
		return fmt.Errorf("mongodb feed: initial revision snapshot: %w", err)
	}

	for {
		streamErr := feed.subscribeChangeStreamOnce(ctx, known, func(signal ports.ChangeSignal) error {
			if err := basechangefeed.SafeInvokeHandler(handler, signal); err != nil {
				return err
			}

			known[signal.Target.String()] = pollSnapshot{Target: signal.Target, Revision: signal.Revision, ApplyBehavior: signal.ApplyBehavior}
			return nil
		})
		if errors.Is(streamErr, basechangefeed.ErrHandlerPanic) {
			return fmt.Errorf("mongodb feed: %w", streamErr)
		}

		if ctx.Err() != nil {
			return fmt.Errorf("mongodb feed: context done: %w", ctx.Err())
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("mongodb feed: context done: %w", ctx.Err())
		case <-time.After(defaultChangeStreamReconnectDelay):
		}

		if err := feed.resyncMissedSignals(ctx, known, func(signal ports.ChangeSignal) error {
			return basechangefeed.SafeInvokeHandler(handler, signal)
		}); err != nil {
			if errors.Is(err, basechangefeed.ErrHandlerPanic) {
				return fmt.Errorf("mongodb feed: %w", err)
			}

			if ctx.Err() != nil {
				return fmt.Errorf("mongodb feed: context done: %w", ctx.Err())
			}

			continue
		}
	}
}

// changeEvent represents the subset of a MongoDB change stream event document
// that we decode. The driver returns each event as a raw BSON document; we
// decode only the fields we need.
type changeEvent struct {
	OperationType string  `bson:"operationType"`
	FullDocument  *bson.D `bson:"fullDocument,omitempty"`
	DocumentKey   *bson.D `bson:"documentKey,omitempty"`
}

// subscribeChangeStream opens a server-side change stream on the entries
// collection filtering for data-mutation operations and translates each event
// into a ChangeSignal delivered to the handler.
func (feed *Feed) subscribeChangeStreamOnce(ctx context.Context, known map[string]pollSnapshot, handler func(ports.ChangeSignal) error) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", "delete"}},
			}},
		}}},
	}

	opts := options.ChangeStream().
		SetFullDocument(options.FullDocument("updateLookup"))

	stream, err := feed.entries.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("mongodb feed: open change stream: %w", err)
	}
	defer stream.Close(ctx)

	if err := feed.resyncMissedSignals(ctx, known, handler); err != nil {
		if errors.Is(err, basechangefeed.ErrHandlerPanic) {
			return err
		}
		return fmt.Errorf("mongodb feed initial resync: %w", err)
	}

	for stream.Next(ctx) {
		var event changeEvent
		if err := stream.Decode(&event); err != nil {
			// Malformed event -- skip rather than crash the loop.
			continue
		}

		signal, ok := signalFromEvent(event)
		if !ok {
			continue
		}

		if err := handler(signal); err != nil {
			return fmt.Errorf("mongodb feed: handler: %w", err)
		}
	}

	// stream.Next returned false -- either context cancelled or stream error.
	if ctx.Err() != nil {
		return fmt.Errorf("mongodb feed: context done: %w", ctx.Err())
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("mongodb feed: change stream error: %w", err)
	}

	return nil
}

func (feed *Feed) resyncMissedSignals(ctx context.Context, known map[string]pollSnapshot, handler func(ports.ChangeSignal) error) error {
	current, err := feed.pollRevisions(ctx)
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		revision := current[key]

		previous, exists := known[key]
		if exists && previous.Revision == revision.Revision {
			continue
		}
		if exists && revision.Revision > previous.Revision.Next() {
			revision.ApplyBehavior = domain.ApplyBundleRebuild
		}

		if err := handler(ports.ChangeSignal{Target: revision.Target, Revision: revision.Revision, ApplyBehavior: revision.ApplyBehavior}); err != nil {
			return fmt.Errorf("mongodb changefeed resync: %w", err)
		}
		known[key] = revision
	}

	return nil
}

// signalFromEvent extracts a ChangeSignal from a decoded change event.
// Only revision-meta sentinel updates are emitted because they are the
// canonical per-target revision signal for MongoDB-backed systemplane state.
func signalFromEvent(event changeEvent) (ports.ChangeSignal, bool) {
	var source *bson.D

	switch event.OperationType {
	case "insert", "update", "replace":
		source = event.FullDocument
	case "delete":
		source = event.DocumentKey
	default:
		return ports.ChangeSignal{}, false
	}

	if source == nil {
		return ports.ChangeSignal{}, false
	}

	if bsonLookupString(source, "key") != revisionMetaKey {
		return ports.ChangeSignal{}, false
	}

	target, revision, behavior, ok := targetFromDoc(source)
	if !ok {
		return ports.ChangeSignal{}, false
	}

	return ports.ChangeSignal{
		Target:        target,
		Revision:      revision,
		ApplyBehavior: behavior,
	}, true
}

// Poll mode.

// subscribePoll periodically queries the revision-meta documents and emits
// a ChangeSignal for every target whose revision has advanced since the last
// poll cycle.
func (feed *Feed) subscribePoll(ctx context.Context, handler func(ports.ChangeSignal)) error {
	// Build initial snapshot of known revisions.
	known, err := feed.pollRevisions(ctx)
	if err != nil {
		return fmt.Errorf("mongodb feed poll: initial snapshot: %w", err)
	}

	ticker := time.NewTicker(feed.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("mongodb feed poll: context done: %w", ctx.Err())
		case <-ticker.C:
			current, pollErr := feed.pollRevisions(ctx)
			if pollErr != nil {
				// Transient query failure -- skip this tick and retry next.
				continue
			}

			for key, rev := range current {
				prev, exists := known[key]
				if !exists || rev.Revision != prev.Revision {
					if exists && rev.Revision > prev.Revision.Next() {
						rev.ApplyBehavior = domain.ApplyBundleRebuild
					}
					signal := ports.ChangeSignal{
						Target:        rev.Target,
						Revision:      rev.Revision,
						ApplyBehavior: rev.ApplyBehavior,
					}
					if err := basechangefeed.SafeInvokeHandler(handler, signal); err != nil {
						return fmt.Errorf("mongodb feed poll: handler: %w", err)
					}
					known[key] = rev
				}
			}

			known = current
		}
	}
}

// pollSnapshot holds a target and its last-known revision.
type pollSnapshot struct {
	Target        domain.Target
	Revision      domain.Revision
	ApplyBehavior domain.ApplyBehavior
}

// pollRevisions queries all revision-meta documents and returns a map keyed by
// target string representation.
func (feed *Feed) pollRevisions(ctx context.Context) (map[string]pollSnapshot, error) {
	filter := bson.D{{Key: "key", Value: revisionMetaKey}}
	projection := bson.D{
		{Key: "kind", Value: 1},
		{Key: "scope", Value: 1},
		{Key: "subject", Value: 1},
		{Key: "revision", Value: 1},
		{Key: "apply_behavior", Value: 1},
	}

	opts := options.Find().SetProjection(projection)

	cursor, err := feed.entries.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb feed poll: find revision meta: %w", err)
	}
	defer cursor.Close(ctx)

	result := make(map[string]pollSnapshot)

	for cursor.Next(ctx) {
		var doc struct {
			Kind          string `bson:"kind"`
			Scope         string `bson:"scope"`
			Subject       string `bson:"subject"`
			Revision      uint64 `bson:"revision"`
			ApplyBehavior string `bson:"apply_behavior"`
		}

		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		kind, err := domain.ParseKind(doc.Kind)
		if err != nil {
			continue
		}

		scope, err := domain.ParseScope(doc.Scope)
		if err != nil {
			continue
		}

		target, err := domain.NewTarget(kind, scope, doc.Subject)
		if err != nil {
			continue
		}

		result[target.String()] = pollSnapshot{
			Target:        target,
			Revision:      domain.Revision(doc.Revision),
			ApplyBehavior: domain.ApplyBehavior(doc.ApplyBehavior),
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongodb feed poll: cursor error: %w", err)
	}

	return result, nil
}

// BSON helpers.

// targetFromDoc extracts a domain.Target and revision from a BSON document
// containing kind, scope, subject, and revision fields. Returns false when
// any required field is missing or invalid.
func targetFromDoc(doc *bson.D) (domain.Target, domain.Revision, domain.ApplyBehavior, bool) {
	kindStr := bsonLookupString(doc, "kind")
	scopeStr := bsonLookupString(doc, "scope")
	subject := bsonLookupString(doc, "subject")

	kind, err := domain.ParseKind(kindStr)
	if err != nil {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	scope, err := domain.ParseScope(scopeStr)
	if err != nil {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	target, err := domain.NewTarget(kind, scope, subject)
	if err != nil {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	revisionRaw, ok := bsonLookupUint64(doc, "revision")
	if !ok {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	revision := domain.Revision(revisionRaw)
	behavior := domain.ApplyBehavior(bsonLookupString(doc, "apply_behavior"))

	return target, revision, behavior, true
}

// bsonLookupString extracts a string value from a bson.D by key.
// Returns "" when the key is absent or the value is not a string.
func bsonLookupString(doc *bson.D, key string) string {
	if doc == nil {
		return ""
	}

	for _, elem := range *doc {
		if elem.Key == key {
			s, ok := elem.Value.(string)
			if ok {
				return s
			}

			return ""
		}
	}

	return ""
}

// bsonLookupUint64 extracts a uint64 value from a bson.D by key.
// Returns false when the key is absent or the value cannot be represented as uint64.
func bsonLookupUint64(doc *bson.D, key string) (uint64, bool) {
	if doc == nil {
		return 0, false
	}

	for _, elem := range *doc {
		if elem.Key == key {
			switch value := elem.Value.(type) {
			case int32:
				if value < 0 {
					return 0, false
				}

				return uint64(value), true
			case int64:
				if value < 0 {
					return 0, false
				}

				return uint64(value), true
			case uint64:
				return value, true
			default:
				return 0, false
			}
		}
	}

	return 0, false
}
