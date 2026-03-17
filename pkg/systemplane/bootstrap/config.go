// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// Sentinel errors for bootstrap configuration validation.
var (
	ErrMissingBackend        = errors.New("systemplane: backend is required")
	ErrMissingPostgresConfig = errors.New("systemplane: postgres config is required when backend is postgres")
	ErrMissingMongoConfig    = errors.New("systemplane: mongodb config is required when backend is mongodb")
	ErrMissingPostgresDSN    = errors.New("systemplane: postgres DSN is required")
	ErrMissingMongoURI       = errors.New("systemplane: mongodb URI is required")
	ErrInvalidWatchMode      = errors.New("systemplane: mongodb watch mode must be change_stream or poll")
	ErrInvalidPollInterval   = errors.New("systemplane: mongodb poll interval must be greater than zero when watch mode is poll")
)

// BootstrapConfig holds the initial configuration needed to connect to the
// systemplane backend before any runtime configuration is loaded.
type BootstrapConfig struct {
	Backend  domain.BackendKind
	Postgres *PostgresBootstrapConfig
	MongoDB  *MongoBootstrapConfig
}

// PostgresBootstrapConfig holds PostgreSQL-specific bootstrap settings.
type PostgresBootstrapConfig struct {
	DSN           string
	Schema        string
	EntriesTable  string
	HistoryTable  string
	NotifyChannel string
}

// MongoBootstrapConfig holds MongoDB-specific bootstrap settings.
type MongoBootstrapConfig struct {
	URI               string
	Database          string
	EntriesCollection string
	HistoryCollection string
	WatchMode         string
	PollInterval      time.Duration
}

// Validate checks that the bootstrap configuration is well-formed.
func (c *BootstrapConfig) Validate() error {
	if c == nil || !c.Backend.IsValid() {
		return fmt.Errorf("%w: %q", ErrMissingBackend, backendString(c))
	}

	switch c.Backend {
	case domain.BackendPostgres:
		if c.Postgres == nil {
			return ErrMissingPostgresConfig
		}
		if c.Postgres.DSN == "" {
			return ErrMissingPostgresDSN
		}
	case domain.BackendMongoDB:
		if c.MongoDB == nil {
			return ErrMissingMongoConfig
		}
		if c.MongoDB.URI == "" {
			return ErrMissingMongoURI
		}
		if c.MongoDB.WatchMode != "" && c.MongoDB.WatchMode != "change_stream" && c.MongoDB.WatchMode != "poll" {
			return ErrInvalidWatchMode
		}
		if c.MongoDB.WatchMode == "poll" && c.MongoDB.PollInterval <= 0 {
			return ErrInvalidPollInterval
		}
	}

	return nil
}

// ApplyDefaults fills in zero-value fields with sensible defaults.
func (c *BootstrapConfig) ApplyDefaults() {
	if c == nil {
		return
	}

	if c.Postgres != nil {
		if c.Postgres.Schema == "" {
			c.Postgres.Schema = "system"
		}
		if c.Postgres.EntriesTable == "" {
			c.Postgres.EntriesTable = "runtime_entries"
		}
		if c.Postgres.HistoryTable == "" {
			c.Postgres.HistoryTable = "runtime_history"
		}
		if c.Postgres.NotifyChannel == "" {
			c.Postgres.NotifyChannel = "systemplane_changes"
		}
	}

	if c.MongoDB != nil {
		if c.MongoDB.Database == "" {
			c.MongoDB.Database = "systemplane"
		}
		if c.MongoDB.EntriesCollection == "" {
			c.MongoDB.EntriesCollection = "runtime_entries"
		}
		if c.MongoDB.HistoryCollection == "" {
			c.MongoDB.HistoryCollection = "runtime_history"
		}
		if c.MongoDB.WatchMode == "" {
			c.MongoDB.WatchMode = "change_stream"
		}
		if c.MongoDB.PollInterval == 0 {
			c.MongoDB.PollInterval = 5 * time.Second
		}
	}
}

func backendString(c *BootstrapConfig) string {
	if c == nil {
		return ""
	}

	return string(c.Backend)
}
