// Copyright 2025 Lerian Studio.

package bootstrap

import "time"

// Default PostgreSQL and MongoDB bootstrap configuration values.
const (
	DefaultPostgresSchema        = "system"
	DefaultPostgresEntriesTable  = "runtime_entries"
	DefaultPostgresHistoryTable  = "runtime_history"
	DefaultPostgresRevisionTable = "runtime_revisions"
	DefaultPostgresNotifyChannel = "systemplane_changes"

	DefaultMongoDatabase          = "systemplane"
	DefaultMongoEntriesCollection = "runtime_entries"
	DefaultMongoHistoryCollection = "runtime_history"
	DefaultMongoWatchMode         = "change_stream"
)

// DefaultMongoPollInterval is the default polling interval for MongoDB change stream fallback.
var DefaultMongoPollInterval = 5 * time.Second
