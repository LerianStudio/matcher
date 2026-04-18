// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/LerianStudio/matcher/internal/discovery/ports"
)

// bridgeHeartbeatKey is the Redis key the bridge worker writes its tick
// timestamp to. A single global key is sufficient for cross-replica
// liveness: all replicas write "I am alive at T", the dashboard reads the
// latest value, and TTL-expiry naturally surfaces "all replicas dead" as a
// missing key. No SET/sorted-set needed — the worker only has to prove at
// least one replica ticked recently. C15.
const bridgeHeartbeatKey = "matcher:fetcher_bridge:last_tick_at"

// ErrNilRedisClient is returned when the heartbeat adapter is constructed
// without a redis client. Distinct from ErrTenantContextRequired which
// signals a runtime (per-request) wiring issue.
var ErrNilRedisClient = errors.New("bridge heartbeat adapter requires a redis client")

// ErrHeartbeatTTLInvalid indicates WriteLastTickAt was called with a
// non-positive TTL. SET EX requires a positive expiry; surfacing this as a
// distinct sentinel keeps config-bug diagnostics out of the generic "redis
// set" error class.
var ErrHeartbeatTTLInvalid = errors.New("bridge heartbeat ttl must be positive")

// Compile-time interface compliance checks. Kept side by side so a single
// signature drift on either interface fails at build time rather than at
// runtime on the first handler call.
var (
	_ ports.BridgeHeartbeatReader = (*BridgeHeartbeat)(nil)
	_ ports.BridgeHeartbeatWriter = (*BridgeHeartbeat)(nil)
)

// BridgeHeartbeat is a Redis-backed adapter that records the bridge worker's
// last observed tick timestamp. The same type satisfies both read and write
// ports because there is exactly one underlying key and operators benefit
// from a single construction site.
type BridgeHeartbeat struct {
	client goredis.UniversalClient
}

// NewBridgeHeartbeat constructs the adapter. Returns ErrNilRedisClient when
// the caller forgot to resolve the Redis connection — keeps construction
// explicit rather than failing later on the hot read/write path.
func NewBridgeHeartbeat(client goredis.UniversalClient) (*BridgeHeartbeat, error) {
	if client == nil {
		return nil, ErrNilRedisClient
	}

	return &BridgeHeartbeat{client: client}, nil
}

// ReadLastTickAt returns the most recent tick timestamp written by any
// bridge worker replica. Returns (zero time, nil) on a missing key — the
// caller treats that as "no heartbeat yet / expired" rather than an error
// so a Fetcher-disabled deployment doesn't surface a scary empty-state.
//
// Parses the value as a Unix-second integer; anything else is treated as
// corruption and surfaced as an error so operators can see the bad write
// on the next dashboard request.
func (heartbeat *BridgeHeartbeat) ReadLastTickAt(ctx context.Context) (time.Time, error) {
	if heartbeat == nil || heartbeat.client == nil {
		return time.Time{}, nil
	}

	raw, err := heartbeat.client.Get(ctx, bridgeHeartbeatKey).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return time.Time{}, nil
		}

		return time.Time{}, fmt.Errorf("redis get heartbeat: %w", err)
	}

	secs, parseErr := strconv.ParseInt(raw, 10, 64)
	if parseErr != nil {
		return time.Time{}, fmt.Errorf("parse heartbeat unix seconds %q: %w", raw, parseErr)
	}

	return time.Unix(secs, 0).UTC(), nil
}

// WriteLastTickAt records `now` as the latest cycle timestamp using SET EX
// so the key auto-expires if the worker stops ticking. The TTL is
// typically 3 × bridgeInterval so a single missed tick doesn't erase
// liveness, but two consecutive failures do.
//
// ttl <= 0 is rejected because SET EX requires a positive expiry — it is
// better to surface the misconfiguration loudly than to write a
// never-expiring key that outlives a crashed worker.
func (heartbeat *BridgeHeartbeat) WriteLastTickAt(ctx context.Context, now time.Time, ttl time.Duration) error {
	if heartbeat == nil || heartbeat.client == nil {
		return ErrNilRedisClient
	}

	if ttl <= 0 {
		return fmt.Errorf("%w: got %s", ErrHeartbeatTTLInvalid, ttl)
	}

	value := strconv.FormatInt(now.UTC().Unix(), 10)

	if err := heartbeat.client.Set(ctx, bridgeHeartbeatKey, value, ttl).Err(); err != nil {
		return fmt.Errorf("redis set heartbeat: %w", err)
	}

	return nil
}
