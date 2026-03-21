// Copyright 2025 Lerian Studio.

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
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Change-stream mode.

func (feed *Feed) subscribeChangeStream(ctx context.Context, handler func(ports.ChangeSignal)) error {
	known, err := feed.pollRevisions(ctx)
	if err != nil {
		return fmt.Errorf("mongodb feed: initial revision snapshot: %w", err)
	}

	for {
		streamErr := feed.subscribeChangeStreamOnce(ctx, known, func(signal ports.ChangeSignal) error {
			if err := basechangefeed.SafeInvokeHandler(handler, signal); err != nil {
				return fmt.Errorf("mongodb feed: handler: %w", err)
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
			return fmt.Errorf("mongodb feed initial resync: %w", err)
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
		return fmt.Errorf("mongodb changefeed resync revisions: %w", err)
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
