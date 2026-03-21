// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	basechangefeed "github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func (feed *Feed) subscribeLoop(ctx context.Context, knownRevisions map[string]trackedRevision, handler func(ports.ChangeSignal)) error {
	var attempt int

	for {
		listenErr := feed.listenLoop(ctx, knownRevisions, func(signal ports.ChangeSignal) error {
			if err := basechangefeed.SafeInvokeHandler(handler, signal); err != nil {
				return fmt.Errorf("pg changefeed subscribe: handler: %w", err)
			}

			knownRevisions[signal.Target.String()] = trackedRevision{Target: signal.Target, Revision: signal.Revision, ApplyBehavior: signal.ApplyBehavior}

			return nil
		})
		if errors.Is(listenErr, basechangefeed.ErrHandlerPanic) {
			return fmt.Errorf("pg changefeed subscribe: %w", listenErr)
		}

		if ctx.Err() != nil {
			return fmt.Errorf("pg changefeed subscribe: context done: %w", ctx.Err())
		}

		shouldReset, err := feed.reconnectAndResync(ctx, &attempt, knownRevisions, handler)
		if err != nil {
			return fmt.Errorf("pg changefeed subscribe: reconnect and resync: %w", err)
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

	if err := feed.resyncMissedSignals(ctx, knownRevisions, func(signal ports.ChangeSignal) error {
		return basechangefeed.SafeInvokeHandler(handler, signal)
	}); err != nil {
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
func (feed *Feed) listenLoop(ctx context.Context, known map[string]trackedRevision, handler func(ports.ChangeSignal) error) error {
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

	if err := feed.resyncMissedSignals(ctx, known, handler); err != nil {
		if errors.Is(err, basechangefeed.ErrHandlerPanic) {
			return fmt.Errorf("pg changefeed initial resync: %w", err)
		}

		return fmt.Errorf("pg changefeed initial resync: %w", err)
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
	Target        domain.Target
	Revision      domain.Revision
	ApplyBehavior domain.ApplyBehavior
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

	rows, err := conn.Query(ctx, `SELECT kind, scope, subject, revision, apply_behavior FROM `+feed.qualifiedRevisions())
	if err != nil {
		return nil, fmt.Errorf("pg changefeed revisions: query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]trackedRevision)

	for rows.Next() {
		var (
			kindText          string
			scopeText         string
			subject           string
			revisionUint      uint64
			applyBehaviorText string
		)

		if err := rows.Scan(&kindText, &scopeText, &subject, &revisionUint, &applyBehaviorText); err != nil {
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

		result[target.String()] = trackedRevision{Target: target, Revision: domain.Revision(revisionUint), ApplyBehavior: domain.ApplyBehavior(applyBehaviorText)}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg changefeed revisions: rows: %w", err)
	}

	return result, nil
}

func (feed *Feed) resyncMissedSignals(ctx context.Context, known map[string]trackedRevision, handler func(ports.ChangeSignal) error) error {
	current, err := feed.fetchRevisions(ctx)
	if err != nil {
		return fmt.Errorf("pg changefeed resync revisions: %w", err)
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
			return fmt.Errorf("pg changefeed resync: %w", err)
		}

		known[key] = revision
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
		Target:        target,
		Revision:      domain.Revision(payload.Revision),
		ApplyBehavior: domain.ApplyBehavior(payload.ApplyBehavior),
	}, nil
}

// backoff calculates the reconnection delay for a given attempt using
// exponential backoff capped at reconnectMax with 0-25% jitter.
