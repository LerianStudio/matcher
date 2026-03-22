//go:build unit

// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// PostgreSQL default constants
// ---------------------------------------------------------------------------

func TestDefaultPostgresSchema_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "system", DefaultPostgresSchema)
}

func TestDefaultPostgresEntriesTable_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "runtime_entries", DefaultPostgresEntriesTable)
}

func TestDefaultPostgresHistoryTable_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "runtime_history", DefaultPostgresHistoryTable)
}

func TestDefaultPostgresRevisionTable_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "runtime_revisions", DefaultPostgresRevisionTable)
}

func TestDefaultPostgresNotifyChannel_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "systemplane_changes", DefaultPostgresNotifyChannel)
}

// ---------------------------------------------------------------------------
// MongoDB default constants
// ---------------------------------------------------------------------------

func TestDefaultMongoDatabase_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "systemplane", DefaultMongoDatabase)
}

func TestDefaultMongoEntriesCollection_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "runtime_entries", DefaultMongoEntriesCollection)
}

func TestDefaultMongoHistoryCollection_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "runtime_history", DefaultMongoHistoryCollection)
}

func TestDefaultMongoWatchMode_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "change_stream", DefaultMongoWatchMode)
}

func TestDefaultMongoPollInterval_Value(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 5*time.Second, DefaultMongoPollInterval)
}

func TestDefaultMongoPollInterval_Positive(t *testing.T) {
	t.Parallel()
	assert.Greater(t, DefaultMongoPollInterval, time.Duration(0))
}

// ---------------------------------------------------------------------------
// Cross-backend consistency: shared naming conventions
// ---------------------------------------------------------------------------

func TestDefaults_PostgresAndMongoEntriesShareBaseName(t *testing.T) {
	t.Parallel()

	// Postgres uses "runtime_entries" for the table, Mongo for the collection.
	// They should share the same base name for conceptual alignment.
	assert.Equal(t, DefaultPostgresEntriesTable, DefaultMongoEntriesCollection)
}

func TestDefaults_PostgresAndMongoHistoryShareBaseName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, DefaultPostgresHistoryTable, DefaultMongoHistoryCollection)
}

// ---------------------------------------------------------------------------
// Default identifiers are valid PostgreSQL identifiers
// ---------------------------------------------------------------------------

func TestDefaults_PostgresIdentifiersAreValid(t *testing.T) {
	t.Parallel()

	// All default Postgres identifiers must pass ValidatePostgresObjectNames.
	err := ValidatePostgresObjectNames(
		DefaultPostgresSchema,
		DefaultPostgresEntriesTable,
		DefaultPostgresHistoryTable,
		DefaultPostgresRevisionTable,
		DefaultPostgresNotifyChannel,
	)
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Default identifiers are non-empty
// ---------------------------------------------------------------------------

func TestDefaults_AllNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "DefaultPostgresSchema", value: DefaultPostgresSchema},
		{name: "DefaultPostgresEntriesTable", value: DefaultPostgresEntriesTable},
		{name: "DefaultPostgresHistoryTable", value: DefaultPostgresHistoryTable},
		{name: "DefaultPostgresRevisionTable", value: DefaultPostgresRevisionTable},
		{name: "DefaultPostgresNotifyChannel", value: DefaultPostgresNotifyChannel},
		{name: "DefaultMongoDatabase", value: DefaultMongoDatabase},
		{name: "DefaultMongoEntriesCollection", value: DefaultMongoEntriesCollection},
		{name: "DefaultMongoHistoryCollection", value: DefaultMongoHistoryCollection},
		{name: "DefaultMongoWatchMode", value: DefaultMongoWatchMode},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, tc.value)
		})
	}
}

// ---------------------------------------------------------------------------
// Default watch mode is a recognized value
// ---------------------------------------------------------------------------

func TestDefaultMongoWatchMode_IsRecognized(t *testing.T) {
	t.Parallel()

	recognized := map[string]bool{
		"change_stream": true,
		"poll":          true,
	}

	assert.True(t, recognized[DefaultMongoWatchMode],
		"DefaultMongoWatchMode %q should be one of: change_stream, poll", DefaultMongoWatchMode)
}

// ---------------------------------------------------------------------------
// Default tables/collections do not collide
// ---------------------------------------------------------------------------

func TestDefaults_PostgresTableNamesAreDistinct(t *testing.T) {
	t.Parallel()

	names := []string{
		DefaultPostgresEntriesTable,
		DefaultPostgresHistoryTable,
		DefaultPostgresRevisionTable,
	}

	seen := make(map[string]bool, len(names))
	for _, n := range names {
		assert.False(t, seen[n], "duplicate postgres table name: %s", n)
		seen[n] = true
	}
}

func TestDefaults_MongoCollectionNamesAreDistinct(t *testing.T) {
	t.Parallel()

	names := []string{
		DefaultMongoEntriesCollection,
		DefaultMongoHistoryCollection,
	}

	seen := make(map[string]bool, len(names))
	for _, n := range names {
		assert.False(t, seen[n], "duplicate mongo collection name: %s", n)
		seen[n] = true
	}
}
