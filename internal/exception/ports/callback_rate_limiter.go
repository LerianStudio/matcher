// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import "context"

// CallbackRateLimiter provides rate limiting for callback processing.
// Implementations should use a sliding window counter pattern to prevent
// callback flooding attacks while allowing legitimate burst traffic.
type CallbackRateLimiter interface {
	// Allow checks whether a callback from the given key (typically external system
	// or tenant identifier) is within the configured rate limit.
	// Returns true if the request is allowed, false if the rate limit is exceeded.
	Allow(ctx context.Context, key string) (bool, error)
}
