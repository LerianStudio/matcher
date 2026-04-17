// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"time"
)

// BridgeHeartbeatReader reads the bridge worker's last observed tick
// timestamp. Implementations are expected to be cheap (single Redis GET on
// the hot path) because the dashboard summary endpoint calls this on every
// request.
//
// Returns (zero time, nil) when the worker has never written a heartbeat —
// callers must treat that as "unknown liveness" rather than an error so a
// fresh deploy or a Fetcher-disabled environment doesn't surface scary
// empty-state noise. Returns an error only for genuine infrastructure
// failures (Redis unreachable, marshal error). C15.
type BridgeHeartbeatReader interface {
	ReadLastTickAt(ctx context.Context) (time.Time, error)
}

// BridgeHeartbeatWriter writes the bridge worker's current tick timestamp.
// The worker calls this at the end of every pollCycle (whether lock was
// acquired or not, empty-batch or productive), so operators can distinguish
// "worker is alive and draining" from "worker is dead, backlog growing".
//
// The TTL on the stored value is enforced by the adapter; on expiry the
// reader returns the zero timestamp and the dashboard surfaces a "no
// heartbeat" signal.
//
// WriteLastTickAt errors are non-fatal for the worker — a momentarily
// unavailable Redis must not stop the bridge from processing eligible
// extractions. The worker logs the error and continues. C15.
type BridgeHeartbeatWriter interface {
	WriteLastTickAt(ctx context.Context, now time.Time, ttl time.Duration) error
}
