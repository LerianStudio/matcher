// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package mocks provides test doubles for exception ports.
package mocks

import "context"

// MockCallbackRateLimiter is a test double for ports.CallbackRateLimiter.
// It exposes an injectable AllowFn field rather than using testify/mock, per
// matcher's convention for interfaces with 5 or fewer methods (see
// docs/PROJECT_RULES.md §14).
type MockCallbackRateLimiter struct {
	AllowFn func(ctx context.Context, key string) (bool, error)
}

// Allow delegates to AllowFn. Returns (true, nil) when AllowFn is unset so the
// zero-value mock defaults to "allow all", keeping setup ceremony-free for the
// common case.
func (m *MockCallbackRateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	if m.AllowFn == nil {
		return true, nil
	}

	return m.AllowFn(ctx, key)
}
