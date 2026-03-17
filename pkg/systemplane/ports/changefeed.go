// Copyright 2025 Lerian Studio.

package ports

import (
	"context"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// ChangeSignal notifies that a target's entries have been updated.
type ChangeSignal struct {
	Target   domain.Target
	Revision domain.Revision
}

// ChangeFeed provides real-time notifications when configuration entries change.
// Implementations may use PostgreSQL LISTEN/NOTIFY, MongoDB change streams,
// Redis Pub/Sub, polling, or any other mechanism that delivers signals with
// at-least-once semantics.
//
// Subscribe blocks until the context is cancelled. The handler is invoked
// synchronously for each signal; implementations should document whether
// signals may be coalesced or reordered.
type ChangeFeed interface {
	// Subscribe registers a handler that is called whenever a configuration
	// target is updated. The method blocks until ctx is cancelled or an
	// unrecoverable error occurs.
	Subscribe(ctx context.Context, handler func(ChangeSignal)) error
}
