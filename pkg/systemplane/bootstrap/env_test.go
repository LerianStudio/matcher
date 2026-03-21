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

func TestLoadFromEnv_ValidPostgres(t *testing.T) {
	t.Setenv(EnvBackend, "postgres")
	t.Setenv(EnvPostgresDSN, "postgres://user:pass@localhost:5432/db")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, domain.BackendPostgres, cfg.Backend)
	assert.NotNil(t, cfg.Postgres)
	assert.Equal(t, "postgres://user:pass@localhost:5432/db", cfg.Postgres.DSN)
	// Defaults applied.
	assert.Equal(t, "system", cfg.Postgres.Schema)
	assert.Equal(t, "runtime_entries", cfg.Postgres.EntriesTable)
	assert.Equal(t, "runtime_history", cfg.Postgres.HistoryTable)
	assert.Equal(t, "runtime_revisions", cfg.Postgres.RevisionTable)
	assert.Equal(t, "systemplane_changes", cfg.Postgres.NotifyChannel)
}

func TestLoadFromEnv_ValidMongoDB(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	t.Setenv(EnvMongoDatabase, "testdb")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, domain.BackendMongoDB, cfg.Backend)
	assert.NotNil(t, cfg.MongoDB)
	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoDB.URI)
	assert.Equal(t, "testdb", cfg.MongoDB.Database)
	// Defaults applied.
	assert.Equal(t, "runtime_entries", cfg.MongoDB.EntriesCollection)
	assert.Equal(t, "runtime_history", cfg.MongoDB.HistoryCollection)
	assert.Equal(t, "change_stream", cfg.MongoDB.WatchMode)
	assert.Equal(t, DefaultMongoPollInterval, cfg.MongoDB.PollInterval)
}

func TestLoadFromEnv_MissingBackend(t *testing.T) {
	// EnvBackend not set — t.Setenv is not called for it.

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingBackend)
}

func TestLoadFromEnv_InvalidBackend(t *testing.T) {
	t.Setenv(EnvBackend, "cockroachdb")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidBackendKind)
}

func TestLoadFromEnv_MissingPostgresDSN(t *testing.T) {
	t.Setenv(EnvBackend, "postgres")
	// EnvPostgresDSN intentionally not set.

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingPostgresDSN)
}

func TestLoadFromEnv_MissingMongoURI(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoDatabase, "testdb")
	// EnvMongoURI intentionally not set.

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingMongoURI)
}

func TestLoadFromEnv_MissingMongoDatabase_DefaultsToSystemplane(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	// EnvMongoDatabase intentionally not set.

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "systemplane", cfg.MongoDB.Database)
}

func TestLoadFromEnv_PollIntervalAsDefaultValue(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	t.Setenv(EnvMongoDatabase, "testdb")
	t.Setenv(EnvMongoPollIntervalSec, "5")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, cfg.MongoDB.PollInterval)
}

func TestLoadFromEnv_PollIntervalAsIntegerSeconds(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	t.Setenv(EnvMongoDatabase, "testdb")
	t.Setenv(EnvMongoPollIntervalSec, "10")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.MongoDB.PollInterval)
}

func TestLoadFromEnv_InvalidPollInterval(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	t.Setenv(EnvMongoDatabase, "testdb")
	t.Setenv(EnvMongoPollIntervalSec, "not-a-duration")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), EnvMongoPollIntervalSec)
}

func TestLoadFromEnv_ExplicitZeroPollIntervalRejected(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	t.Setenv(EnvMongoWatchMode, "poll")
	t.Setenv(EnvMongoPollIntervalSec, "0")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPollInterval)
}

func TestLoadFromEnv_ExplicitNegativePollIntervalRejected(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	t.Setenv(EnvMongoWatchMode, "poll")
	t.Setenv(EnvMongoPollIntervalSec, "-1")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPollInterval)
}

func TestLoadFromEnv_PostgresWhitespaceOnlyDSNRejected(t *testing.T) {
	t.Setenv(EnvBackend, "postgres")
	t.Setenv(EnvPostgresDSN, "   ")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingPostgresDSN)
}

func TestLoadFromEnv_MongoWhitespaceOnlyURIRejected(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "   ")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingMongoURI)
}

func TestLoadFromEnv_InvalidPostgresIdentifierRejected(t *testing.T) {
	t.Setenv(EnvBackend, "postgres")
	t.Setenv(EnvPostgresDSN, "postgres://localhost/db")
	t.Setenv(EnvPostgresSchema, "bad-schema")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
}

func TestLoadFromEnv_DefaultsAppliedWhenOptionalVarsMissing(t *testing.T) {
	t.Setenv(EnvBackend, "postgres")
	t.Setenv(EnvPostgresDSN, "postgres://localhost/db")
	// No optional env vars set.

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "system", cfg.Postgres.Schema)
	assert.Equal(t, "runtime_entries", cfg.Postgres.EntriesTable)
	assert.Equal(t, "runtime_history", cfg.Postgres.HistoryTable)
	assert.Equal(t, "runtime_revisions", cfg.Postgres.RevisionTable)
	assert.Equal(t, "systemplane_changes", cfg.Postgres.NotifyChannel)
}

func TestLoadFromEnv_PostgresCustomOptionalVars(t *testing.T) {
	t.Setenv(EnvBackend, "postgres")
	t.Setenv(EnvPostgresDSN, "postgres://localhost/db")
	t.Setenv(EnvPostgresSchema, "custom_schema")
	t.Setenv(EnvPostgresEntriesTable, "my_entries")
	t.Setenv(EnvPostgresHistoryTable, "my_history")
	t.Setenv(EnvPostgresRevisionTable, "my_revisions")
	t.Setenv(EnvPostgresNotifyChannel, "my_channel")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "custom_schema", cfg.Postgres.Schema)
	assert.Equal(t, "my_entries", cfg.Postgres.EntriesTable)
	assert.Equal(t, "my_history", cfg.Postgres.HistoryTable)
	assert.Equal(t, "my_revisions", cfg.Postgres.RevisionTable)
	assert.Equal(t, "my_channel", cfg.Postgres.NotifyChannel)
}

func TestLoadFromEnv_MongoDBCustomOptionalVars(t *testing.T) {
	t.Setenv(EnvBackend, "mongodb")
	t.Setenv(EnvMongoURI, "mongodb://localhost:27017")
	t.Setenv(EnvMongoDatabase, "testdb")
	t.Setenv(EnvMongoEntriesCollection, "custom_entries")
	t.Setenv(EnvMongoHistoryCollection, "custom_history")
	t.Setenv(EnvMongoWatchMode, "poll")
	t.Setenv(EnvMongoPollIntervalSec, "15")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "custom_entries", cfg.MongoDB.EntriesCollection)
	assert.Equal(t, "custom_history", cfg.MongoDB.HistoryCollection)
	assert.Equal(t, "poll", cfg.MongoDB.WatchMode)
	assert.Equal(t, 15*time.Second, cfg.MongoDB.PollInterval)
}

func TestLoadFromEnv_BackendCaseInsensitive(t *testing.T) {
	t.Setenv(EnvBackend, "POSTGRES")
	t.Setenv(EnvPostgresDSN, "postgres://localhost/db")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, domain.BackendPostgres, cfg.Backend)
}
