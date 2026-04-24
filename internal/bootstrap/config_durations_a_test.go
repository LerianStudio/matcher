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

func TestConfig_DBMetricsInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 30,
			expected: 30 * time.Second,
		},
		{
			name:     "zero returns minimum 1 second",
			interval: 0,
			expected: time.Second,
		},
		{
			name:     "negative returns minimum 1 second",
			interval: -10,
			expected: time.Second,
		},
		{
			name:     "default value 15 seconds",
			interval: 15,
			expected: 15 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{DBMetricsIntervalSec: tt.interval})
			assert.Equal(t, tt.expected, cfg.DBMetricsInterval())
		})
	}
}

func TestConfig_IdempotencyRetryWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		window   int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			window:   600,
			expected: 600 * time.Second,
		},
		{
			name:     "zero returns minimum 1 minute",
			window:   0,
			expected: time.Minute,
		},
		{
			name:     "negative returns minimum 1 minute",
			window:   -100,
			expected: time.Minute,
		},
		{
			name:     "default value 300 seconds (5 minutes)",
			window:   300,
			expected: 5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{IdempotencyRetryWindowSec: tt.window})
			assert.Equal(t, tt.expected, cfg.IdempotencyRetryWindow())
		})
	}
}

func TestConfig_IdempotencySuccessTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hours    int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			hours:    48,
			expected: 48 * time.Hour,
		},
		{
			name:     "zero returns minimum 1 hour",
			hours:    0,
			expected: time.Hour,
		},
		{
			name:     "negative returns minimum 1 hour",
			hours:    -10,
			expected: time.Hour,
		},
		{
			name:     "default value 168 hours (7 days)",
			hours:    168,
			expected: 168 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{IdempotencySuccessTTLHours: tt.hours})
			assert.Equal(t, tt.expected, cfg.IdempotencySuccessTTL())
		})
	}
}

func TestConfig_InfraConnectTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{
			name:     "default 30 seconds",
			timeout:  30,
			expected: 30 * time.Second,
		},
		{
			name:     "custom 60 seconds",
			timeout:  60,
			expected: 60 * time.Second,
		},
		{
			name:     "lower timeout for fast-fail",
			timeout:  10,
			expected: 10 * time.Second,
		},
		{
			name:     "zero value returns default 30s",
			timeout:  0,
			expected: 30 * time.Second,
		},
		{
			name:     "negative value returns default 30s",
			timeout:  -1,
			expected: 30 * time.Second,
		},
		{
			name:     "caps absurdly high values",
			timeout:  9999,
			expected: 300 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{InfraConnectTimeoutSec: tt.timeout})
			assert.Equal(t, tt.expected, cfg.InfraConnectTimeout())
		})
	}
}

