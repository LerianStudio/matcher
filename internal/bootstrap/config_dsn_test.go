// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_PrimaryDSN(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		PrimaryDBHost:             "localhost",
		PrimaryDBPort:             "5432",
		PrimaryDBUser:             "matcher",
		PrimaryDBPassword:         "pr0d-s3cure-p@ss!",
		PrimaryDBName:             "matcher_db",
		PrimaryDBSSLMode:          "disable",
		PostgresConnectTimeoutSec: 10,
	})

	expected := "host=localhost port=5432 user=matcher password=pr0d-s3cure-p@ss! dbname=matcher_db sslmode=disable connect_timeout=10"
	assert.Equal(t, expected, cfg.PrimaryDSN())
}

func TestConfig_ReplicaDSN_FallbackToPrimary(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		PrimaryDBHost:             "primary.db",
		PrimaryDBPort:             "5432",
		PrimaryDBUser:             "matcher",
		PrimaryDBPassword:         "pr0d-s3cure-p@ss!",
		PrimaryDBName:             "matcher_db",
		PrimaryDBSSLMode:          "require",
		PostgresConnectTimeoutSec: 10,
		ReplicaDBHost:             "",
	})

	assert.Equal(t, cfg.PrimaryDSN(), cfg.ReplicaDSN())
}

func TestConfig_ReplicaDSN_WithReplica(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		PrimaryDBHost:             "primary.db",
		PrimaryDBPort:             "5432",
		PrimaryDBUser:             "matcher",
		PrimaryDBPassword:         "pr0d-s3cure-p@ss!",
		PrimaryDBName:             "matcher_db",
		PrimaryDBSSLMode:          "require",
		PostgresConnectTimeoutSec: 10,
		ReplicaDBHost:             "replica.db",
		ReplicaDBPort:             "5433",
	})

	expected := "host=replica.db port=5433 user=matcher password=pr0d-s3cure-p@ss! dbname=matcher_db sslmode=require connect_timeout=10"
	assert.Equal(t, expected, cfg.ReplicaDSN())
}

func TestConfig_RabbitMQDSN(t *testing.T) {
	t.Parallel()

	t.Run("encodes default vhost slash", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "guest",
			RabbitMQPassword: "guest",
			RabbitMQVHost:    "/",
		})

		expected := "amqp://guest:guest@localhost:5672/%2F"
		assert.Equal(t, expected, cfg.RabbitMQDSN())
	})

	t.Run("encodes credentials and vhost per RFC3986", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "us:er",
			RabbitMQPassword: "p@ss:word",
			RabbitMQVHost:    "my/vhost",
		})

		expected := "amqp://us%3Aer:p%40ss%3Aword@localhost:5672/my%2Fvhost"
		assert.Equal(t, expected, cfg.RabbitMQDSN())
	})

	t.Run("omits password separator when password empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "user",
			RabbitMQPassword: "",
			RabbitMQVHost:    "vhost",
		})

		expected := "amqp://user@localhost:5672/vhost"
		assert.Equal(t, expected, cfg.RabbitMQDSN())
	})
}

func TestConfig_ReplicaDSN_ExtendedCases(t *testing.T) {
	t.Parallel()

	t.Run("uses replica settings when configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:             "primary-host",
			PrimaryDBPort:             "5432",
			PrimaryDBUser:             "matcher",
			PrimaryDBPassword:         "pr0d-s3cure-p@ss!",
			PrimaryDBName:             "matcher_db",
			PrimaryDBSSLMode:          "require",
			PostgresConnectTimeoutSec: 10,
			ReplicaDBHost:             "replica-host",
			ReplicaDBPort:             "5433",
			ReplicaDBUser:             "replica_user",
			ReplicaDBPassword:         "replica_secret",
			ReplicaDBName:             "replica_db",
			ReplicaDBSSLMode:          "disable",
		})

		dsn := cfg.ReplicaDSN()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5433")
		assert.Contains(t, dsn, "user=replica_user")
		assert.Contains(t, dsn, "password=replica_secret")
		assert.Contains(t, dsn, "dbname=replica_db")
		assert.Contains(t, dsn, "sslmode=disable")
	})

	t.Run("uses primary fallbacks for empty replica fields", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:             "primary-host",
			PrimaryDBPort:             "5432",
			PrimaryDBUser:             "matcher",
			PrimaryDBPassword:         "pr0d-s3cure-p@ss!",
			PrimaryDBName:             "matcher_db",
			PrimaryDBSSLMode:          "require",
			PostgresConnectTimeoutSec: 10,
			ReplicaDBHost:             "replica-host",
		})

		dsn := cfg.ReplicaDSN()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5432")
		assert.Contains(t, dsn, "user=matcher")
		assert.Contains(t, dsn, "password=pr0d-s3cure-p@ss!")
		assert.Contains(t, dsn, "dbname=matcher_db")
		assert.Contains(t, dsn, "sslmode=require")
	})
}

func TestConfig_PrimaryDSNMasked(t *testing.T) {
	t.Parallel()

	t.Run("redacts password", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "localhost",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "super_secret_password",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
		})

		dsn := cfg.PrimaryDSNMasked()

		assert.Contains(t, dsn, "password=***REDACTED***")
		assert.NotContains(t, dsn, "super_secret_password")
	})
}

func TestConfig_ReplicaDSNMasked(t *testing.T) {
	t.Parallel()

	t.Run("falls back to primary masked when replica not configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "pr0d-s3cure-p@ss!",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=primary-host")
		assert.Contains(t, dsn, "password=***REDACTED***")
	})

	t.Run("uses replica settings with masked password", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "pr0d-s3cure-p@ss!",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBPort:     "5433",
			ReplicaDBUser:     "replica_user",
			ReplicaDBPassword: "replica_secret",
			ReplicaDBName:     "replica_db",
			ReplicaDBSSLMode:  "disable",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "password=***REDACTED***")
		assert.NotContains(t, dsn, "replica_secret")
	})
}
func TestConfig_ReplicaDSNMasked_PartialFallbacks(t *testing.T) {
	t.Parallel()

	t.Run("uses primary port when replica port empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "pr0d-s3cure-p@ss!",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBPort:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5432")
		assert.Contains(t, dsn, "password=***REDACTED***")
	})

	t.Run("uses primary user when replica user empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "pr0d-s3cure-p@ss!",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBUser:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "user=matcher")
	})

	t.Run("uses primary dbname when replica dbname empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "pr0d-s3cure-p@ss!",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBName:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "dbname=matcher_db")
	})

	t.Run("uses primary sslmode when replica sslmode empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "pr0d-s3cure-p@ss!",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBSSLMode:  "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "sslmode=require")
	})

	t.Run("uses all replica settings when all configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "pr0d-s3cure-p@ss!",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBPort:     "5433",
			ReplicaDBUser:     "replica_user",
			ReplicaDBPassword: "replica_secret",
			ReplicaDBName:     "replica_db",
			ReplicaDBSSLMode:  "disable",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5433")
		assert.Contains(t, dsn, "user=replica_user")
		assert.Contains(t, dsn, "dbname=replica_db")
		assert.Contains(t, dsn, "sslmode=disable")
		assert.Contains(t, dsn, "password=***REDACTED***")
		assert.NotContains(t, dsn, "replica_secret")
	})
}

