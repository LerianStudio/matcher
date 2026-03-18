// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// Sentinel errors for bootstrap configuration validation.
var (
	ErrMissingBackend            = errors.New("systemplane: backend is required")
	ErrMissingPostgresConfig     = errors.New("systemplane: postgres config is required when backend is postgres")
	ErrMissingMongoConfig        = errors.New("systemplane: mongodb config is required when backend is mongodb")
	ErrMissingPostgresDSN        = errors.New("systemplane: postgres DSN is required")
	ErrMissingMongoURI           = errors.New("systemplane: mongodb URI is required")
	ErrInvalidPostgresIdentifier = errors.New("systemplane: invalid postgres identifier")
	ErrInvalidWatchMode          = errors.New("systemplane: mongodb watch mode must be change_stream or poll")
	ErrInvalidPollInterval       = errors.New("systemplane: mongodb poll interval must be greater than zero when watch mode is poll")
	ErrInvalidMongoIdentifier    = errors.New("systemplane: invalid mongodb identifier")
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
	RevisionTable string
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
func (cfg *BootstrapConfig) Validate() error {
	if cfg == nil || !cfg.Backend.IsValid() {
		return fmt.Errorf("%w: %q", ErrMissingBackend, backendString(cfg))
	}

	switch cfg.Backend {
	case domain.BackendPostgres:
		return validatePostgresBootstrap(cfg.Postgres)
	case domain.BackendMongoDB:
		return validateMongoBootstrap(cfg.MongoDB)
	}

	return nil
}

// ApplyDefaults fills in zero-value fields with sensible defaults.
func (cfg *BootstrapConfig) ApplyDefaults() {
	if cfg == nil {
		return
	}

	if cfg.Postgres != nil {
		applyPostgresDefaults(cfg.Postgres)
	}

	if cfg.MongoDB != nil {
		applyMongoDefaults(cfg.MongoDB)
	}
}

func validatePostgresBootstrap(postgresConfig *PostgresBootstrapConfig) error {
	if postgresConfig == nil {
		return ErrMissingPostgresConfig
	}

	if strings.TrimSpace(postgresConfig.DSN) == "" {
		return ErrMissingPostgresDSN
	}

	if err := ValidatePostgresObjectNames(
		defaultString(postgresConfig.Schema, DefaultPostgresSchema),
		defaultString(postgresConfig.EntriesTable, DefaultPostgresEntriesTable),
		defaultString(postgresConfig.HistoryTable, DefaultPostgresHistoryTable),
		defaultString(postgresConfig.RevisionTable, DefaultPostgresRevisionTable),
		defaultString(postgresConfig.NotifyChannel, DefaultPostgresNotifyChannel),
	); err != nil {
		return err
	}

	return nil
}

func validateMongoBootstrap(mongoConfig *MongoBootstrapConfig) error {
	if mongoConfig == nil {
		return ErrMissingMongoConfig
	}

	if strings.TrimSpace(mongoConfig.URI) == "" {
		return ErrMissingMongoURI
	}

	if err := validateMongoIdentifier("database", defaultString(mongoConfig.Database, DefaultMongoDatabase)); err != nil {
		return err
	}

	if err := validateMongoIdentifier("entries collection", defaultString(mongoConfig.EntriesCollection, DefaultMongoEntriesCollection)); err != nil {
		return err
	}

	if err := validateMongoIdentifier("history collection", defaultString(mongoConfig.HistoryCollection, DefaultMongoHistoryCollection)); err != nil {
		return err
	}

	if mongoConfig.WatchMode != "" && mongoConfig.WatchMode != "change_stream" && mongoConfig.WatchMode != "poll" {
		return ErrInvalidWatchMode
	}

	if mongoConfig.WatchMode == "poll" && mongoConfig.PollInterval <= 0 {
		return ErrInvalidPollInterval
	}

	return nil
}

// validateMongoIdentifier checks that a MongoDB database or collection name
// does not contain forbidden characters (null bytes, $, empty).
func validateMongoIdentifier(kind, value string) error {
	trimmedValue := strings.TrimSpace(value)
	if trimmedValue == "" {
		return fmt.Errorf("%w %s: must not be empty", ErrInvalidMongoIdentifier, kind)
	}

	if strings.ContainsRune(trimmedValue, '$') {
		return fmt.Errorf("%w %s %q: must not contain '$'", ErrInvalidMongoIdentifier, kind, value)
	}

	if strings.ContainsRune(trimmedValue, 0) {
		return fmt.Errorf("%w %s: must not contain null bytes", ErrInvalidMongoIdentifier, kind)
	}

	return nil
}

func applyPostgresDefaults(postgresConfig *PostgresBootstrapConfig) {
	postgresConfig.DSN = strings.TrimSpace(postgresConfig.DSN)
	postgresConfig.Schema = strings.TrimSpace(postgresConfig.Schema)
	postgresConfig.EntriesTable = strings.TrimSpace(postgresConfig.EntriesTable)
	postgresConfig.HistoryTable = strings.TrimSpace(postgresConfig.HistoryTable)
	postgresConfig.RevisionTable = strings.TrimSpace(postgresConfig.RevisionTable)
	postgresConfig.NotifyChannel = strings.TrimSpace(postgresConfig.NotifyChannel)

	if postgresConfig.Schema == "" {
		postgresConfig.Schema = DefaultPostgresSchema
	}

	if postgresConfig.EntriesTable == "" {
		postgresConfig.EntriesTable = DefaultPostgresEntriesTable
	}

	if postgresConfig.HistoryTable == "" {
		postgresConfig.HistoryTable = DefaultPostgresHistoryTable
	}

	if postgresConfig.RevisionTable == "" {
		postgresConfig.RevisionTable = DefaultPostgresRevisionTable
	}

	if postgresConfig.NotifyChannel == "" {
		postgresConfig.NotifyChannel = DefaultPostgresNotifyChannel
	}
}

func applyMongoDefaults(mongoConfig *MongoBootstrapConfig) {
	mongoConfig.URI = strings.TrimSpace(mongoConfig.URI)
	mongoConfig.Database = strings.TrimSpace(mongoConfig.Database)
	mongoConfig.EntriesCollection = strings.TrimSpace(mongoConfig.EntriesCollection)
	mongoConfig.HistoryCollection = strings.TrimSpace(mongoConfig.HistoryCollection)
	mongoConfig.WatchMode = strings.TrimSpace(mongoConfig.WatchMode)

	if mongoConfig.Database == "" {
		mongoConfig.Database = DefaultMongoDatabase
	}

	if mongoConfig.EntriesCollection == "" {
		mongoConfig.EntriesCollection = DefaultMongoEntriesCollection
	}

	if mongoConfig.HistoryCollection == "" {
		mongoConfig.HistoryCollection = DefaultMongoHistoryCollection
	}

	if mongoConfig.WatchMode == "" {
		mongoConfig.WatchMode = DefaultMongoWatchMode
	}

	if mongoConfig.PollInterval == 0 {
		mongoConfig.PollInterval = DefaultMongoPollInterval
	}
}

func backendString(c *BootstrapConfig) string {
	if c == nil {
		return ""
	}

	return string(c.Backend)
}

func defaultString(value, fallback string) string {
	trimmedValue := strings.TrimSpace(value)
	if trimmedValue == "" {
		return fallback
	}

	return trimmedValue
}
