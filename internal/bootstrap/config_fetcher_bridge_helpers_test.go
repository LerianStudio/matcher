// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestFetcherBridgeStaleThreshold_NilConfig falls back to the documented
// one-hour default when the config pointer is nil. Mirrors the
// FetcherBridgeInterval / FetcherBridgeBatchSize defaults so dashboards
// keep working when the systemplane snapshot has not yet hydrated.
func TestFetcherBridgeStaleThreshold_NilConfig(t *testing.T) {
	t.Parallel()

	var cfg *Config
	assert.Equal(t, time.Hour, cfg.FetcherBridgeStaleThreshold())
}

// TestFetcherBridgeStaleThreshold_ZeroValue falls back to one hour rather
// than collapsing every COMPLETE+unlinked extraction into the stale bucket.
func TestFetcherBridgeStaleThreshold_ZeroValue(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.BridgeStaleThresholdSec = 0
	assert.Equal(t, time.Hour, cfg.FetcherBridgeStaleThreshold())
}

// TestFetcherBridgeStaleThreshold_NegativeValue treats nonsense input the
// same way as zero. Defence in depth: the systemplane validator rejects
// negative values up front, but if a refactor ever bypasses it the
// helper still produces a meaningful threshold.
func TestFetcherBridgeStaleThreshold_NegativeValue(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.BridgeStaleThresholdSec = -42
	assert.Equal(t, time.Hour, cfg.FetcherBridgeStaleThreshold())
}

// TestFetcherBridgeStaleThreshold_PositiveValue returns the configured
// duration when the value is sane.
func TestFetcherBridgeStaleThreshold_PositiveValue(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.BridgeStaleThresholdSec = 1800
	assert.Equal(t, 30*time.Minute, cfg.FetcherBridgeStaleThreshold())
}
