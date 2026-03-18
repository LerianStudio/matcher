// Copyright 2025 Lerian Studio.

// Package postgres provides PostgreSQL-backed change feed adapters.
package postgres

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	basechangefeed "github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
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
	Kind     string `json:"kind"`
	Scope    string `json:"scope"`
	Subject  string `json:"subject"`
	Revision uint64 `json:"revision"`
}

// Subscribe registers a handler that is called whenever a configuration target
// is updated. It blocks until ctx is cancelled or an unrecoverable error occurs.
//
// On each NOTIFY the handler receives a ChangeSignal synchronously from the
// listener goroutine. If the handler blocks, subsequent signals are delayed but
// never dropped (PostgreSQL buffers them until the connection reads them).
func (feed *Feed) Subscribe(ctx context.Context, handler func(ports.ChangeSignal)) error {
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
		return err
	}

	knownRevisions, err := feed.fetchRevisions(ctx)
	if err != nil {
		return fmt.Errorf("pg changefeed subscribe: initial revision snapshot: %w", err)
	}

	var attempt int

	for {
		listenErr := feed.listenLoop(ctx, func(signal ports.ChangeSignal) error {
			knownRevisions[signal.Target.String()] = trackedRevision{Target: signal.Target, Revision: signal.Revision}

			return basechangefeed.SafeInvokeHandler(handler, signal)
		})
		if errors.Is(listenErr, basechangefeed.ErrHandlerPanic) {
			return fmt.Errorf("pg changefeed subscribe: %w", listenErr)
		}

		if ctx.Err() != nil {
			return fmt.Errorf("pg changefeed subscribe: context done: %w", ctx.Err())
		}

		shouldReset, err := feed.reconnectAndResync(ctx, &attempt, knownRevisions, handler)
		if err != nil {
			return err
		}

		if shouldReset {
			attempt = 0
		}
	}
}

// reconnectAndResync handles the backoff delay and revision resync after a
// connection loss. It returns (true, nil) when resync succeeded and the attempt
// counter should be reset, (false, nil) when resync failed transiently and the
// caller should retry, or (_, err) on unrecoverable errors.
func (feed *Feed) reconnectAndResync(ctx context.Context, attempt *int, knownRevisions map[string]trackedRevision, handler func(ports.ChangeSignal)) (bool, error) {
	delay := feed.backoff(*attempt)
	*attempt++

	select {
	case <-ctx.Done():
		return false, fmt.Errorf("pg changefeed subscribe: context done: %w", ctx.Err())
	case <-time.After(delay):
		// Retry after backoff.
	}

	if err := feed.resyncMissedSignals(ctx, knownRevisions, handler); err != nil {
		if errors.Is(err, basechangefeed.ErrHandlerPanic) {
			return false, fmt.Errorf("pg changefeed subscribe: %w", err)
		}

		if ctx.Err() != nil {
			return false, fmt.Errorf("pg changefeed subscribe: context done: %w", ctx.Err())
		}

		return false, nil
	}

	return true, nil
}

// listenLoop establishes a single pgx connection, issues LISTEN, and processes
// notifications until the context is cancelled or the connection fails.
func (feed *Feed) listenLoop(ctx context.Context, handler func(ports.ChangeSignal) error) error {
	listenStmt := fmt.Sprintf("LISTEN %q", feed.channel)
	unlistenStmt := fmt.Sprintf("UNLISTEN %q", feed.channel)

	conn, err := pgx.Connect(ctx, feed.dsn)
	if err != nil {
		return fmt.Errorf("pg changefeed connect: %w", err)
	}

	defer func(parentCtx context.Context) {
		// Best-effort UNLISTEN and close; ignore errors because the
		// connection may already be broken.
		cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), closeTimeout)
		defer cancel()

		_, _ = conn.Exec(cleanCtx, unlistenStmt)
		_ = conn.Close(cleanCtx)
	}(ctx)

	if _, err := conn.Exec(ctx, listenStmt); err != nil {
		return fmt.Errorf("pg changefeed listen: %w", err)
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("pg changefeed wait: context done: %w", ctx.Err())
			}

			return fmt.Errorf("pg changefeed wait: %w", err)
		}

		signal, err := parsePayload(notification.Payload)
		if err != nil {
			// Malformed payload -- skip and continue listening rather
			// than tearing down the connection.
			continue
		}

		if err := handler(signal); err != nil {
			return fmt.Errorf("pg changefeed handler: %w", err)
		}
	}
}

type trackedRevision struct {
	Target   domain.Target
	Revision domain.Revision
}

func (feed *Feed) fetchRevisions(ctx context.Context) (map[string]trackedRevision, error) {
	if feed.schema == "" || feed.revisionTable == "" {
		return map[string]trackedRevision{}, nil
	}

	conn, err := pgx.Connect(ctx, feed.dsn)
	if err != nil {
		return nil, fmt.Errorf("pg changefeed revisions: connect: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT kind, scope, subject, revision FROM `+feed.qualifiedRevisions())
	if err != nil {
		return nil, fmt.Errorf("pg changefeed revisions: query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]trackedRevision)

	for rows.Next() {
		var (
			kindText     string
			scopeText    string
			subject      string
			revisionUint uint64
		)

		if err := rows.Scan(&kindText, &scopeText, &subject, &revisionUint); err != nil {
			return nil, fmt.Errorf("pg changefeed revisions: scan: %w", err)
		}

		kind, err := domain.ParseKind(kindText)
		if err != nil {
			continue
		}

		scope, err := domain.ParseScope(scopeText)
		if err != nil {
			continue
		}

		target, err := domain.NewTarget(kind, scope, subject)
		if err != nil {
			continue
		}

		result[target.String()] = trackedRevision{Target: target, Revision: domain.Revision(revisionUint)}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg changefeed revisions: rows: %w", err)
	}

	return result, nil
}

func (feed *Feed) resyncMissedSignals(ctx context.Context, known map[string]trackedRevision, handler func(ports.ChangeSignal)) error {
	current, err := feed.fetchRevisions(ctx)
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

		known[key] = revision
		if err := basechangefeed.SafeInvokeHandler(handler, ports.ChangeSignal{Target: revision.Target, Revision: revision.Revision}); err != nil {
			return fmt.Errorf("pg changefeed resync: %w", err)
		}
	}

	return nil
}

// parsePayload converts a NOTIFY JSON payload into a ChangeSignal.
func parsePayload(data string) (ports.ChangeSignal, error) {
	var payload notifyPayload

	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return ports.ChangeSignal{}, fmt.Errorf("pg changefeed unmarshal: %w", err)
	}

	kind, err := domain.ParseKind(payload.Kind)
	if err != nil {
		return ports.ChangeSignal{}, fmt.Errorf("pg changefeed parse kind: %w", err)
	}

	scope, err := domain.ParseScope(payload.Scope)
	if err != nil {
		return ports.ChangeSignal{}, fmt.Errorf("pg changefeed parse scope: %w", err)
	}

	target, err := domain.NewTarget(kind, scope, payload.Subject)
	if err != nil {
		return ports.ChangeSignal{}, fmt.Errorf("pg changefeed build target: %w", err)
	}

	return ports.ChangeSignal{
		Target:   target,
		Revision: domain.Revision(payload.Revision),
	}, nil
}

// backoff calculates the reconnection delay for a given attempt using
// exponential backoff capped at reconnectMax with 0-25% jitter.
func (feed *Feed) backoff(attempt int) time.Duration {
	base := float64(feed.reconnectMin)
	delay := base * math.Pow(backoffBase, float64(attempt))

	if delay > float64(feed.reconnectMax) {
		delay = float64(feed.reconnectMax)
	}

	// Add jitter: 0-25% of delay.
	randomFactor := secureRandomFactor()
	jitter := delay * jitterRatio * randomFactor

	return time.Duration(delay + jitter)
}

func secureRandomFactor() float64 {
	var randomBytes [8]byte

	if _, err := rand.Read(randomBytes[:]); err != nil {
		return 0
	}

	randomValue := binary.LittleEndian.Uint64(randomBytes[:])

	return float64(randomValue) / float64(math.MaxUint64)
}

// validateRevisionSource ensures that schema and revisionTable identifiers are
// safe for SQL interpolation. Both must be empty (revision tracking disabled)
// or both must match the PostgreSQL identifier pattern. This provides
// defense-in-depth — the bootstrap layer validates upstream, but the Feed is a
// public API and must enforce its own safety contract.
func (feed *Feed) validateRevisionSource() error {
	if feed.schema == "" && feed.revisionTable == "" {
		return nil // Revision tracking disabled — no identifiers to validate.
	}

	if feed.schema != "" && !identifierPattern.MatchString(feed.schema) {
		return fmt.Errorf("%w: schema %q", ErrInvalidIdentifier, feed.schema)
	}

	if feed.revisionTable != "" && !identifierPattern.MatchString(feed.revisionTable) {
		return fmt.Errorf("%w: revision table %q", ErrInvalidIdentifier, feed.revisionTable)
	}

	return nil
}

func (feed *Feed) qualifiedRevisions() string {
	return feed.schema + "." + feed.revisionTable
}
