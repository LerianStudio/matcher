// Copyright 2025 Lerian Studio.

// Package postgres provides PostgreSQL-backed change feed adapters.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Feed implements ports.ChangeFeed using PostgreSQL LISTEN/NOTIFY.
// It maintains a persistent pgx connection that listens on a configured channel
// and converts NOTIFY payloads into ChangeSignal values. On connection loss it
// automatically reconnects with exponential backoff and jitter.
type Feed struct {
	dsn           string
	channel       string
	schema        string
	revisionTable string
	reconnectMin  time.Duration
	reconnectMax  time.Duration
}

var (
	// ErrNilFeed is returned when Subscribe is called on a nil receiver.
	ErrNilFeed = errors.New("pg changefeed: feed is nil")
	// ErrEmptyDSN is returned when feed DSN is empty.
	ErrEmptyDSN = errors.New("pg changefeed: dsn is empty")
	// ErrEmptyChannel is returned when notification channel is empty.
	ErrEmptyChannel = errors.New("pg changefeed: channel is empty")
	// ErrInvalidChannel is returned when channel name does not match PostgreSQL identifier rules.
	ErrInvalidChannel = errors.New("pg changefeed: invalid channel")
	// ErrInvalidIdentifier is returned when a schema or table name does not match PostgreSQL identifier rules.
	ErrInvalidIdentifier = errors.New("pg changefeed: invalid identifier")
	// ErrNilFeedHandler is returned when Subscribe receives a nil handler.
	ErrNilFeedHandler  = errors.New("pg changefeed: handler is nil")
	channelNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	// identifierPattern validates PostgreSQL schema/table identifiers. Matches
	// the bootstrap package's postgresIdentifierPattern for defense-in-depth.
	identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

const (
	defaultReconnectMin = 1 * time.Second
	defaultReconnectMax = 30 * time.Second
	closeTimeout        = 2 * time.Second
	backoffBase         = 2.0
	jitterRatio         = 0.25
)

// Compile-time interface check.
var _ ports.ChangeFeed = (*Feed)(nil)

// New creates a new PostgreSQL change feed listener.
func New(dsn, channel string, opts ...Option) *Feed {
	feed := &Feed{
		dsn:          dsn,
		channel:      channel,
		reconnectMin: defaultReconnectMin,
		reconnectMax: defaultReconnectMax,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt(feed)
	}

	return feed
}

// notifyPayload matches the JSON structure emitted by the postgres Store
// adapter via pg_notify on each Put operation.
type notifyPayload struct {
	Kind          string `json:"kind"`
	Scope         string `json:"scope"`
	Subject       string `json:"subject"`
	Revision      uint64 `json:"revision"`
	ApplyBehavior string `json:"apply_behavior,omitempty"`
}

// Subscribe registers a handler that is called whenever a configuration target
// is updated. It blocks until ctx is cancelled or an unrecoverable error occurs.
//
// On each NOTIFY the handler receives a ChangeSignal synchronously from the
// listener goroutine. If the handler blocks, subsequent signals are delayed but
// never dropped (PostgreSQL buffers them until the connection reads them).
func (feed *Feed) Subscribe(ctx context.Context, handler func(ports.ChangeSignal)) error {
	if err := feed.validateSubscribeInput(handler); err != nil {
		return err
	}

	knownRevisions, err := feed.fetchRevisions(ctx)
	if err != nil {
		return fmt.Errorf("pg changefeed subscribe: initial revision snapshot: %w", err)
	}

	return feed.subscribeLoop(ctx, knownRevisions, handler)
}

func (feed *Feed) validateSubscribeInput(handler func(ports.ChangeSignal)) error {
	if feed == nil {
		return ErrNilFeed
	}

	if feed.dsn == "" {
		return ErrEmptyDSN
	}

	if feed.channel == "" {
		return ErrEmptyChannel
	}

	if !channelNamePattern.MatchString(feed.channel) {
		return fmt.Errorf("%w: %q", ErrInvalidChannel, feed.channel)
	}

	if handler == nil {
		return ErrNilFeedHandler
	}

	if err := feed.validateRevisionSource(); err != nil {
		return fmt.Errorf("pg changefeed subscribe: validate revision source: %w", err)
	}

	return nil
}
