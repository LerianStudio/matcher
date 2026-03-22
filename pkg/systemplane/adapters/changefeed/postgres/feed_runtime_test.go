//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// secureRandomFactor
// ---------------------------------------------------------------------------

func TestSecureRandomFactor_Range(t *testing.T) {
	t.Parallel()

	for range 100 {
		f := secureRandomFactor()
		assert.GreaterOrEqual(t, f, 0.0, "random factor must be >= 0")
		assert.Less(t, f, 1.0, "random factor must be < 1")
	}
}

func TestSecureRandomFactor_NotConstant(t *testing.T) {
	t.Parallel()

	// Generate many values and verify they are not all identical (statistical check).
	seen := make(map[float64]struct{})

	for range 50 {
		seen[secureRandomFactor()] = struct{}{}
	}

	assert.Greater(t, len(seen), 1, "secureRandomFactor should produce varying values")
}

// ---------------------------------------------------------------------------
// Feed.backoff
// ---------------------------------------------------------------------------

func TestBackoff_AttemptZero(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(1*time.Second, 30*time.Second))

	d := feed.backoff(0)

	// At attempt 0: base = 1s * 2^0 = 1s. With up to 25% jitter: [1s, 1.25s].
	assert.GreaterOrEqual(t, d, 1*time.Second)
	assert.LessOrEqual(t, d, 1250*time.Millisecond)
}

func TestBackoff_ExponentialGrowth(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(1*time.Second, 1*time.Minute))

	prevBase := float64(0)

	for attempt := range 5 {
		d := feed.backoff(attempt)
		base := float64(1*time.Second) * float64(uint(1)<<attempt)

		assert.GreaterOrEqual(t, float64(d), base,
			"attempt %d: duration should be >= base", attempt)
		assert.Greater(t, base, prevBase,
			"base should grow exponentially")

		prevBase = base
	}
}

func TestBackoff_CappedAtMax(t *testing.T) {
	t.Parallel()

	maxDelay := 5 * time.Second
	feed := New("dsn", "ch", WithReconnectBounds(1*time.Second, maxDelay))

	// At attempt 10, the uncapped base would be 1024s, well above 5s.
	for range 20 {
		d := feed.backoff(10)
		maxWithJitter := time.Duration(float64(maxDelay) * 1.25)

		assert.GreaterOrEqual(t, d, maxDelay,
			"backoff should be >= max (jitter is additive)")
		assert.LessOrEqual(t, d, maxWithJitter,
			"backoff should not exceed max + 25%% jitter")
	}
}

func TestBackoff_JitterWithinRange(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(100*time.Millisecond, 10*time.Second))

	// At attempt 0: base = 100ms. Range should be [100ms, 125ms].
	for range 50 {
		d := feed.backoff(0)
		assert.GreaterOrEqual(t, d, 100*time.Millisecond)
		assert.LessOrEqual(t, d, 125*time.Millisecond)
	}
}

func TestBackoff_CustomBounds(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithReconnectBounds(500*time.Millisecond, 10*time.Second))

	// Attempt 0: base = 500ms. Range: [500ms, 625ms].
	d := feed.backoff(0)
	assert.GreaterOrEqual(t, d, 500*time.Millisecond)
	assert.LessOrEqual(t, d, 625*time.Millisecond)

	// Attempt 1: base = 1s. Range: [1s, 1.25s].
	d = feed.backoff(1)
	assert.GreaterOrEqual(t, d, 1*time.Second)
	assert.LessOrEqual(t, d, 1250*time.Millisecond)
}

// ---------------------------------------------------------------------------
// Feed.validateRevisionSource
// ---------------------------------------------------------------------------

func TestValidateRevisionSource_BothEmpty_OK(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch")
	// schema and revisionTable are both empty by default.

	err := feed.validateRevisionSource()

	require.NoError(t, err)
}

func TestValidateRevisionSource_BothValid_OK(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithRevisionSource("public", "revisions"))

	err := feed.validateRevisionSource()

	require.NoError(t, err)
}

func TestValidateRevisionSource_SchemaOnlySet_Error(t *testing.T) {
	t.Parallel()

	feed := &Feed{
		dsn:     "dsn",
		channel: "ch",
		schema:  "public",
		// revisionTable is empty.
	}

	err := feed.validateRevisionSource()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
	assert.Contains(t, err.Error(), "must be configured together")
}

func TestValidateRevisionSource_TableOnlySet_Error(t *testing.T) {
	t.Parallel()

	feed := &Feed{
		dsn:           "dsn",
		channel:       "ch",
		revisionTable: "revisions",
		// schema is empty.
	}

	err := feed.validateRevisionSource()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
	assert.Contains(t, err.Error(), "must be configured together")
}

func TestValidateRevisionSource_InvalidSchema_Error(t *testing.T) {
	t.Parallel()

	feed := &Feed{
		dsn:           "dsn",
		channel:       "ch",
		schema:        "INVALID-SCHEMA!",
		revisionTable: "revisions",
	}

	err := feed.validateRevisionSource()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
	assert.Contains(t, err.Error(), "schema")
}

func TestValidateRevisionSource_InvalidTable_Error(t *testing.T) {
	t.Parallel()

	feed := &Feed{
		dsn:           "dsn",
		channel:       "ch",
		schema:        "public",
		revisionTable: "DROP TABLE; --",
	}

	err := feed.validateRevisionSource()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
	assert.Contains(t, err.Error(), "revision table")
}

func TestValidateRevisionSource_ValidIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		schema string
		table  string
	}{
		{name: "simple lowercase", schema: "myschema", table: "mytable"},
		{name: "with underscores", schema: "my_schema", table: "my_table"},
		{name: "starts with underscore", schema: "_private", table: "_internal"},
		{name: "with numbers", schema: "schema1", table: "table_v2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			feed := &Feed{
				dsn:           "dsn",
				channel:       "ch",
				schema:        tt.schema,
				revisionTable: tt.table,
			}

			err := feed.validateRevisionSource()
			require.NoError(t, err)
		})
	}
}

func TestValidateRevisionSource_InvalidIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		schema string
		table  string
	}{
		{name: "starts with digit", schema: "1schema", table: "valid_table"},
		{name: "contains dash", schema: "my-schema", table: "valid_table"},
		{name: "contains uppercase", schema: "MySchema", table: "valid_table"},
		{name: "too long", schema: "a" + string(make([]byte, 63)), table: "valid_table"},
		{name: "SQL injection attempt", schema: "valid_schema", table: "t; DROP TABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			feed := &Feed{
				dsn:           "dsn",
				channel:       "ch",
				schema:        tt.schema,
				revisionTable: tt.table,
			}

			err := feed.validateRevisionSource()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidIdentifier)
		})
	}
}

// ---------------------------------------------------------------------------
// Feed.qualifiedRevisions
// ---------------------------------------------------------------------------

func TestQualifiedRevisions(t *testing.T) {
	t.Parallel()

	feed := &Feed{schema: "systemplane", revisionTable: "config_revisions"}

	result := feed.qualifiedRevisions()

	assert.Equal(t, "systemplane.config_revisions", result)
}

func TestQualifiedRevisions_Empty(t *testing.T) {
	t.Parallel()

	feed := &Feed{}

	result := feed.qualifiedRevisions()

	assert.Equal(t, ".", result)
}
