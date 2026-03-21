//go:build unit

// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapConfig_Validate_ValidPostgres(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN: "postgres://user:pass@localhost:5432/db",
		},
	}

	err := cfg.Validate()
	require.NoError(t, err)
}

func TestBootstrapConfig_Validate_ValidMongoDB(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI:      "mongodb://localhost:27017",
			Database: "testdb",
		},
	}

	err := cfg.Validate()
	require.NoError(t, err)
}

func TestBootstrapConfig_Validate_MissingBackend(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendKind(""),
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingBackend)
}

func TestBootstrapConfig_Validate_InvalidBackend(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendKind("cockroachdb"),
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingBackend)
	assert.Contains(t, err.Error(), "cockroachdb")
}

func TestBootstrapConfig_Validate_PostgresWithoutConfigStruct(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend:  domain.BackendPostgres,
		Postgres: nil,
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingPostgresConfig)
}

func TestBootstrapConfig_Validate_PostgresWithoutDSN(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN: "",
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingPostgresDSN)
}

func TestBootstrapConfig_Validate_PostgresWhitespaceOnlyDSN(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN: "   ",
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingPostgresDSN)
}

func TestBootstrapConfig_Validate_InvalidPostgresIdentifier(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN:           "postgres://user:pass@localhost:5432/db",
			Schema:        "bad-schema",
			EntriesTable:  DefaultPostgresEntriesTable,
			HistoryTable:  DefaultPostgresHistoryTable,
			RevisionTable: DefaultPostgresRevisionTable,
			NotifyChannel: DefaultPostgresNotifyChannel,
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
}

func TestBootstrapConfig_Validate_MongoDBWithoutConfigStruct(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: nil,
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingMongoConfig)
}

func TestBootstrapConfig_Validate_MongoDBWithoutURI(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI:      "",
			Database: "testdb",
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingMongoURI)
}

func TestBootstrapConfig_Validate_MongoDBWhitespaceOnlyURI(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI:      "   ",
			Database: "testdb",
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingMongoURI)
}

func TestBootstrapConfig_Validate_MongoDBPollModeWithoutPositiveInterval(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI:          "mongodb://localhost:27017",
			WatchMode:    "poll",
			PollInterval: 0,
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPollInterval)
}

func TestBootstrapConfig_Validate_MongoDBWithoutDatabase_Allowed(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI:      "mongodb://localhost:27017",
			Database: "",
		},
	}

	err := cfg.Validate()
	require.NoError(t, err)
}

func TestBootstrapConfig_ApplyDefaults_Postgres(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN: "postgres://localhost/db",
		},
	}

	cfg.ApplyDefaults()

	assert.Equal(t, "system", cfg.Postgres.Schema)
	assert.Equal(t, "runtime_entries", cfg.Postgres.EntriesTable)
	assert.Equal(t, "runtime_history", cfg.Postgres.HistoryTable)
	assert.Equal(t, "runtime_revisions", cfg.Postgres.RevisionTable)
	assert.Equal(t, "systemplane_changes", cfg.Postgres.NotifyChannel)
}

func TestBootstrapConfig_ApplyDefaults_MongoDB(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI:      "mongodb://localhost:27017",
			Database: "testdb",
		},
	}

	cfg.ApplyDefaults()

	assert.Equal(t, "runtime_entries", cfg.MongoDB.EntriesCollection)
	assert.Equal(t, "runtime_history", cfg.MongoDB.HistoryCollection)
	assert.Equal(t, "change_stream", cfg.MongoDB.WatchMode)
	assert.Equal(t, DefaultMongoPollInterval, cfg.MongoDB.PollInterval)
}

func TestBootstrapConfig_ApplyDefaults_DoesNotOverwriteSetValues(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN:           "postgres://localhost/db",
			Schema:        "custom_schema",
			EntriesTable:  "custom_entries",
			HistoryTable:  "custom_history",
			RevisionTable: "custom_revisions",
			NotifyChannel: "custom_channel",
		},
	}

	cfg.ApplyDefaults()

	assert.Equal(t, "custom_schema", cfg.Postgres.Schema)
	assert.Equal(t, "custom_entries", cfg.Postgres.EntriesTable)
	assert.Equal(t, "custom_history", cfg.Postgres.HistoryTable)
	assert.Equal(t, "custom_revisions", cfg.Postgres.RevisionTable)
	assert.Equal(t, "custom_channel", cfg.Postgres.NotifyChannel)
}

func TestBootstrapConfig_ApplyDefaults_MongoDoesNotOverwriteSetValues(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI:               "mongodb://localhost:27017",
			Database:          "testdb",
			EntriesCollection: "my_entries",
			HistoryCollection: "my_history",
			WatchMode:         "poll",
			PollInterval:      10 * time.Second,
		},
	}

	cfg.ApplyDefaults()

	assert.Equal(t, "my_entries", cfg.MongoDB.EntriesCollection)
	assert.Equal(t, "my_history", cfg.MongoDB.HistoryCollection)
	assert.Equal(t, "poll", cfg.MongoDB.WatchMode)
	assert.Equal(t, 10*time.Second, cfg.MongoDB.PollInterval)
}

func TestBootstrapConfig_ApplyDefaults_NilSubConfigs(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
	}

	// Should not panic when sub-configs are nil.
	assert.NotPanics(t, func() {
		cfg.ApplyDefaults()
	})
}
