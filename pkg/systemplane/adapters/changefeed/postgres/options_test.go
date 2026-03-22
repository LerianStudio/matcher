//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// WithReconnectBounds
// ---------------------------------------------------------------------------

func TestWithReconnectBounds_SetsMinAndMax(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(2*time.Second, 60*time.Second))

	assert.Equal(t, 2*time.Second, feed.reconnectMin)
	assert.Equal(t, 60*time.Second, feed.reconnectMax)
}

func TestWithReconnectBounds_ZeroMin_KeepsDefault(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(0, 60*time.Second))

	assert.Equal(t, defaultReconnectMin, feed.reconnectMin)
	assert.Equal(t, 60*time.Second, feed.reconnectMax)
}

func TestWithReconnectBounds_ZeroMax_KeepsDefault(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(2*time.Second, 0))

	assert.Equal(t, 2*time.Second, feed.reconnectMin)
	assert.Equal(t, defaultReconnectMax, feed.reconnectMax)
}

func TestWithReconnectBounds_NegativeValues_KeepsDefaults(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(-1*time.Second, -5*time.Second))

	assert.Equal(t, defaultReconnectMin, feed.reconnectMin)
	assert.Equal(t, defaultReconnectMax, feed.reconnectMax)
}

func TestWithReconnectBounds_BothZero_KeepsDefaults(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(0, 0))

	assert.Equal(t, defaultReconnectMin, feed.reconnectMin)
	assert.Equal(t, defaultReconnectMax, feed.reconnectMax)
}

func TestWithReconnectBounds_OnlyMinPositive(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(5*time.Second, -1))

	assert.Equal(t, 5*time.Second, feed.reconnectMin)
	assert.Equal(t, defaultReconnectMax, feed.reconnectMax)
}

func TestWithReconnectBounds_OnlyMaxPositive(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(-1, 120*time.Second))

	assert.Equal(t, defaultReconnectMin, feed.reconnectMin)
	assert.Equal(t, 120*time.Second, feed.reconnectMax)
}

// ---------------------------------------------------------------------------
// WithRevisionSource
// ---------------------------------------------------------------------------

func TestWithRevisionSource_SetsBoth(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithRevisionSource("systemplane", "config_revisions"))

	assert.Equal(t, "systemplane", feed.schema)
	assert.Equal(t, "config_revisions", feed.revisionTable)
}

func TestWithRevisionSource_EmptySchema_NotSet(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithRevisionSource("", "config_revisions"))

	assert.Equal(t, "", feed.schema)
	assert.Equal(t, "config_revisions", feed.revisionTable)
}

func TestWithRevisionSource_EmptyTable_NotSet(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithRevisionSource("systemplane", ""))

	assert.Equal(t, "systemplane", feed.schema)
	assert.Equal(t, "", feed.revisionTable)
}

func TestWithRevisionSource_BothEmpty_NeitherSet(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithRevisionSource("", ""))

	assert.Equal(t, "", feed.schema)
	assert.Equal(t, "", feed.revisionTable)
}

// ---------------------------------------------------------------------------
// Option type
// ---------------------------------------------------------------------------

func TestOptionType_NilOption_Ignored(t *testing.T) {
	t.Parallel()

	// New should handle nil options gracefully.
	feed := New("dsn", "ch", nil, WithReconnectBounds(3*time.Second, 45*time.Second))

	assert.Equal(t, 3*time.Second, feed.reconnectMin)
	assert.Equal(t, 45*time.Second, feed.reconnectMax)
}

func TestOptionType_MultipleOptions_AppliedInOrder(t *testing.T) {
	t.Parallel()

	// Second WithReconnectBounds overwrites the first.
	feed := New("dsn", "ch",
		WithReconnectBounds(1*time.Second, 10*time.Second),
		WithReconnectBounds(5*time.Second, 120*time.Second),
	)

	assert.Equal(t, 5*time.Second, feed.reconnectMin)
	assert.Equal(t, 120*time.Second, feed.reconnectMax)
}

func TestOptionType_CombineReconnectAndRevision(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch",
		WithReconnectBounds(2*time.Second, 60*time.Second),
		WithRevisionSource("my_schema", "my_table"),
	)

	assert.Equal(t, 2*time.Second, feed.reconnectMin)
	assert.Equal(t, 60*time.Second, feed.reconnectMax)
	assert.Equal(t, "my_schema", feed.schema)
	assert.Equal(t, "my_table", feed.revisionTable)
}
