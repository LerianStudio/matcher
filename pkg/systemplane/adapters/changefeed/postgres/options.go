// Copyright 2025 Lerian Studio.

package postgres

import "time"

// Option configures a Feed.
type Option func(*Feed)

// WithReconnectBounds sets the minimum and maximum reconnection delay.
func WithReconnectBounds(minDelay, maxDelay time.Duration) Option {
	return func(feed *Feed) {
		if minDelay > 0 {
			feed.reconnectMin = minDelay
		}

		if maxDelay > 0 {
			feed.reconnectMax = maxDelay
		}
	}
}

// WithRevisionSource configures the revision table that should be consulted to
// resync missed signals after connection interruptions.
func WithRevisionSource(schema, revisionTable string) Option {
	return func(feed *Feed) {
		if schema != "" {
			feed.schema = schema
		}

		if revisionTable != "" {
			feed.revisionTable = revisionTable
		}
	}
}
