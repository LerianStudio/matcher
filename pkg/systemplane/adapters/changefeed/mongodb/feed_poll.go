// Copyright 2025 Lerian Studio.

package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	basechangefeed "github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

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
