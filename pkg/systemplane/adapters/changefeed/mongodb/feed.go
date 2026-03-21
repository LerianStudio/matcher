// Copyright 2025 Lerian Studio.

// Package mongodb provides MongoDB-backed change feed adapters.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
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
