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

func TestPrimaryDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      Config
		contains []string
	}{
		{
			name: "constructs_dsn_from_all_fields",
			cfg: Config{
				Postgres: PostgresConfig{
					PrimaryHost:       "db.example.com",
					PrimaryPort:       "5433",
					PrimaryUser:       "admin",
					PrimaryPassword:   "s3cret",
					PrimaryDB:         "mydb",
					PrimarySSLMode:    "require",
					ConnectTimeoutSec: 15,
				},
			},
			contains: []string{
				"host=db.example.com",
				"port=5433",
				"user=admin",
				"password=s3cret",
				"dbname=mydb",
				"sslmode=require",
				"connect_timeout=15",
			},
		},
		{
			name: "handles_empty_password",
			cfg: Config{
				Postgres: PostgresConfig{
					PrimaryHost:     "localhost",
					PrimaryPort:     "5432",
					PrimaryUser:     "matcher",
					PrimaryPassword: "",
					PrimaryDB:       "matcher",
					PrimarySSLMode:  "disable",
				},
			},
			contains: []string{
				"password=",
				"host=localhost",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dsn := tt.cfg.PrimaryDSN()
			for _, substr := range tt.contains {
				assert.Contains(t, dsn, substr)
			}
		})
	}
}

func TestReplicaDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         Config
		expectEqual bool // if true, ReplicaDSN == PrimaryDSN
		contains    []string
	}{
		{
			name: "falls_back_to_primary_when_replica_host_empty",
			cfg: Config{
				Postgres: PostgresConfig{
					PrimaryHost:     "primary.db",
					PrimaryPort:     "5432",
					PrimaryUser:     "user",
					PrimaryPassword: "pass",
					PrimaryDB:       "db",
					PrimarySSLMode:  "disable",
					ReplicaHost:     "",
				},
			},
			expectEqual: true,
		},
		{
			name: "uses_replica_host_with_primary_fallbacks",
			cfg: Config{
				Postgres: PostgresConfig{
					PrimaryHost:     "primary.db",
					PrimaryPort:     "5432",
					PrimaryUser:     "user",
					PrimaryPassword: "pass",
					PrimaryDB:       "db",
					PrimarySSLMode:  "disable",
					ReplicaHost:     "replica.db",
					// All other replica fields empty → fall back to primary
				},
			},
			expectEqual: false,
			contains: []string{
				"host=replica.db",
				"port=5432",
				"user=user",
				"password=pass",
				"dbname=db",
				"sslmode=disable",
			},
		},
		{
			name: "uses_all_replica_fields_when_set",
			cfg: Config{
				Postgres: PostgresConfig{
					PrimaryHost:       "primary.db",
					PrimaryPort:       "5432",
					PrimaryUser:       "user",
					PrimaryPassword:   "pass",
					PrimaryDB:         "db",
					PrimarySSLMode:    "disable",
					ConnectTimeoutSec: 10,
					ReplicaHost:       "replica.db",
					ReplicaPort:       "5433",
					ReplicaUser:       "readonly",
					ReplicaPassword:   "r0pass",
					ReplicaDB:         "replica_db",
					ReplicaSSLMode:    "require",
				},
			},
			expectEqual: false,
			contains: []string{
				"host=replica.db",
				"port=5433",
				"user=readonly",
				"password=r0pass",
				"dbname=replica_db",
				"sslmode=require",
				"connect_timeout=10",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			replicaDSN := tt.cfg.ReplicaDSN()

			if tt.expectEqual {
				assert.Equal(t, tt.cfg.PrimaryDSN(), replicaDSN)
			} else {
				for _, substr := range tt.contains {
					assert.Contains(t, replicaDSN, substr)
				}
			}
		})
	}
}

func TestPrimaryDSNMasked(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Postgres: PostgresConfig{
			PrimaryHost:     "db.example.com",
			PrimaryPort:     "5432",
			PrimaryUser:     "admin",
			PrimaryPassword: "supersecret",
			PrimaryDB:       "mydb",
			PrimarySSLMode:  "require",
		},
	}

	masked := cfg.PrimaryDSNMasked()
	assert.Contains(t, masked, "***REDACTED***")
	assert.NotContains(t, masked, "supersecret")
	assert.Contains(t, masked, "host=db.example.com")
}

func TestReplicaDSNMasked(t *testing.T) {
	t.Parallel()

	t.Run("falls_back_to_primary_masked_when_no_replica", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{
				PrimaryHost:     "primary.db",
				PrimaryPort:     "5432",
				PrimaryUser:     "user",
				PrimaryPassword: "pass",
				PrimaryDB:       "db",
				PrimarySSLMode:  "disable",
			},
		}

		assert.Equal(t, cfg.PrimaryDSNMasked(), cfg.ReplicaDSNMasked())
	})

	t.Run("masks_replica_password", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{
				PrimaryHost:     "primary.db",
				PrimaryPort:     "5432",
				PrimaryUser:     "user",
				PrimaryPassword: "pass",
				PrimaryDB:       "db",
				PrimarySSLMode:  "disable",
				ReplicaHost:     "replica.db",
				ReplicaPassword: "replicapass",
			},
		}

		masked := cfg.ReplicaDSNMasked()
		assert.Contains(t, masked, "***REDACTED***")
		assert.NotContains(t, masked, "replicapass")
		assert.Contains(t, masked, "host=replica.db")
	})
}

func TestWebhookTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{name: "defaults_to_30s_when_zero", timeout: 0, expected: 30 * time.Second},
		{name: "defaults_to_30s_when_negative", timeout: -5, expected: 30 * time.Second},
		{name: "uses_configured_value", timeout: 60, expected: 60 * time.Second},
		{name: "caps_at_300s", timeout: 500, expected: 300 * time.Second},
		{name: "allows_exact_max", timeout: 300, expected: 300 * time.Second},
		{name: "allows_1s", timeout: 1, expected: 1 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Webhook: WebhookConfig{TimeoutSec: tt.timeout}}
			assert.Equal(t, tt.expected, cfg.WebhookTimeout())
		})
	}
}

func TestQueryTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{name: "defaults_to_30s_when_zero", timeout: 0, expected: 30 * time.Second},
		{name: "defaults_to_30s_when_negative", timeout: -1, expected: 30 * time.Second},
		{name: "uses_configured_value", timeout: 60, expected: 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Postgres: PostgresConfig{QueryTimeoutSec: tt.timeout}}
			assert.Equal(t, tt.expected, cfg.QueryTimeout())
		})
	}
}

func TestConnMaxLifetime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mins     int
		expected time.Duration
	}{
		{name: "zero_returns_zero_duration", mins: 0, expected: 0},
		{name: "positive_value_in_minutes", mins: 30, expected: 30 * time.Minute},
		{name: "negative_returns_negative", mins: -1, expected: -1 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Postgres: PostgresConfig{ConnMaxLifetimeMins: tt.mins}}
			assert.Equal(t, tt.expected, cfg.ConnMaxLifetime())
		})
	}
}

func TestExportPresignExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sec      int
		expected time.Duration
	}{
		{name: "defaults_to_1h_when_zero", sec: 0, expected: 3600 * time.Second},
		{name: "defaults_to_1h_when_negative", sec: -1, expected: 3600 * time.Second},
		{name: "uses_configured_value", sec: 7200, expected: 7200 * time.Second},
		{name: "caps_at_7_days", sec: 700000, expected: 604800 * time.Second},
		{name: "allows_exact_max", sec: 604800, expected: 604800 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{ExportWorker: ExportWorkerConfig{PresignExpirySec: tt.sec}}
			assert.Equal(t, tt.expected, cfg.ExportPresignExpiry())
		})
	}
}

func TestInfraConnectTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sec      int
		expected time.Duration
	}{
		{name: "defaults_to_30s_when_zero", sec: 0, expected: 30 * time.Second},
		{name: "defaults_to_30s_when_negative", sec: -5, expected: 30 * time.Second},
		{name: "uses_configured_value", sec: 30, expected: 30 * time.Second},
		{name: "caps_at_300s", sec: 999, expected: 300 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Infrastructure: InfrastructureConfig{ConnectTimeoutSec: tt.sec}}
			assert.Equal(t, tt.expected, cfg.InfraConnectTimeout())
		})
	}
}

func TestRabbitMQDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      RabbitMQConfig
		contains []string
	}{
		{
			name: "standard_dsn_with_default_vhost",
			cfg: RabbitMQConfig{
				URI:      "amqp",
				Host:     "localhost",
				Port:     "5672",
				User:     "guest",
				Password: "guest",
				VHost:    "/",
			},
			contains: []string{"amqp://guest:guest@localhost:5672"},
		},
		{
			name: "url_encodes_special_password",
			cfg: RabbitMQConfig{
				URI:      "amqp",
				Host:     "rmq.prod",
				Port:     "5672",
				User:     "app",
				Password: "p@ss/word",
				VHost:    "myapp",
			},
			contains: []string{"amqp://app:p%40ss%2Fword@rmq.prod:5672"},
		},
		{
			name: "empty_password_omits_colon",
			cfg: RabbitMQConfig{
				URI:      "amqp",
				Host:     "localhost",
				Port:     "5672",
				User:     "guest",
				Password: "",
			},
			contains: []string{"amqp://guest@localhost:5672"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{RabbitMQ: tt.cfg}
			dsn := cfg.RabbitMQDSN()

			for _, substr := range tt.contains {
				assert.Contains(t, dsn, substr)
			}
		})
	}
}

func TestRedisTimeouts(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Redis: RedisConfig{
			ReadTimeoutMs:  3000,
			WriteTimeoutMs: 5000,
			DialTimeoutMs:  2000,
		},
	}

	assert.Equal(t, 3*time.Second, cfg.RedisReadTimeout())
	assert.Equal(t, 5*time.Second, cfg.RedisWriteTimeout())
	assert.Equal(t, 2*time.Second, cfg.RedisDialTimeout())
}

func TestIdempotencyDurations(t *testing.T) {
	t.Parallel()

	t.Run("defaults_when_zero", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Idempotency: IdempotencyConfig{}}
		assert.Equal(t, time.Minute, cfg.IdempotencyRetryWindow())
		assert.Equal(t, time.Hour, cfg.IdempotencySuccessTTL())
	})

	t.Run("uses_configured_values", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Idempotency: IdempotencyConfig{
			RetryWindowSec:  600,
			SuccessTTLHours: 48,
		}}
		assert.Equal(t, 600*time.Second, cfg.IdempotencyRetryWindow())
		assert.Equal(t, 48*time.Hour, cfg.IdempotencySuccessTTL())
	})
}

func TestSchedulerInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sec      int
		expected time.Duration
	}{
		{name: "defaults_to_1m_when_zero", sec: 0, expected: time.Minute},
		{name: "uses_configured_value", sec: 120, expected: 120 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Scheduler: SchedulerConfig{IntervalSec: tt.sec}}
			assert.Equal(t, tt.expected, cfg.SchedulerInterval())
		})
	}
}

func TestCleanupWorkerDurations(t *testing.T) {
	t.Parallel()

	t.Run("defaults_when_zero", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{CleanupWorker: CleanupWorkerConfig{}}
		assert.Equal(t, 3600*time.Second, cfg.CleanupWorkerInterval())
		assert.Equal(t, 100, cfg.CleanupWorkerBatchSize())
		assert.Equal(t, 3600*time.Second, cfg.CleanupWorkerGracePeriod())
	})

	t.Run("uses_configured_values", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{CleanupWorker: CleanupWorkerConfig{
			IntervalSec:    7200,
			BatchSize:      500,
			GracePeriodSec: 1800,
		}}
		assert.Equal(t, 7200*time.Second, cfg.CleanupWorkerInterval())
		assert.Equal(t, 500, cfg.CleanupWorkerBatchSize())
		assert.Equal(t, 1800*time.Second, cfg.CleanupWorkerGracePeriod())
	})
}

func TestArchivalDurations(t *testing.T) {
	t.Parallel()

	t.Run("defaults_when_zero", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Archival: ArchivalConfig{}}
		assert.Equal(t, time.Hour, cfg.ArchivalInterval())
		assert.Equal(t, 3600*time.Second, cfg.ArchivalPresignExpiry())
	})

	t.Run("uses_configured_values", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Archival: ArchivalConfig{
			IntervalHours:    48,
			PresignExpirySec: 7200,
		}}
		assert.Equal(t, 48*time.Hour, cfg.ArchivalInterval())
		assert.Equal(t, 7200*time.Second, cfg.ArchivalPresignExpiry())
	})

	t.Run("caps_presign_at_7_days", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Archival: ArchivalConfig{PresignExpirySec: 999999}}
		assert.Equal(t, 604800*time.Second, cfg.ArchivalPresignExpiry())
	})
}

func TestCallbackRateLimitPerMinute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		per      int
		expected int
	}{
		{name: "defaults_to_60_when_zero", per: 0, expected: 60},
		{name: "defaults_to_60_when_negative", per: -1, expected: 60},
		{name: "uses_configured_value", per: 120, expected: 120},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{CallbackRateLimit: CallbackRateLimitConfig{PerMinute: tt.per}}
			assert.Equal(t, tt.expected, cfg.CallbackRateLimitPerMinute())
		})
	}
}

func TestDedupeTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sec      int
		expected time.Duration
	}{
		{name: "defaults_to_1h_when_zero", sec: 0, expected: time.Hour},
		{name: "defaults_to_1h_when_negative", sec: -1, expected: time.Hour},
		{name: "uses_configured_value", sec: 120, expected: 120 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Dedupe: DedupeConfig{TTLSec: tt.sec}}
			assert.Equal(t, tt.expected, cfg.DedupeTTL())
		})
	}
}

func TestDBMetricsInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sec      int
		expected time.Duration
	}{
		{name: "defaults_to_1s_when_zero", sec: 0, expected: time.Second},
		{name: "uses_configured_value", sec: 15, expected: 15 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Telemetry: TelemetryConfig{DBMetricsIntervalSec: tt.sec}}
			assert.Equal(t, tt.expected, cfg.DBMetricsInterval())
		})
	}
}

func TestExportWorkerPollInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sec      int
		expected time.Duration
	}{
		{name: "defaults_to_5s_when_zero", sec: 0, expected: 5 * time.Second},
		{name: "uses_configured_value", sec: 10, expected: 10 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{ExportWorker: ExportWorkerConfig{PollIntervalSec: tt.sec}}
			assert.Equal(t, tt.expected, cfg.ExportWorkerPollInterval())
		})
	}
}

func TestFetcherDurationHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      Config
		getter   func(*Config) time.Duration
		expected time.Duration
	}{
		{
			name:     "request_timeout_defaults_when_zero",
			cfg:      Config{Fetcher: FetcherConfig{RequestTimeoutSec: 0}},
			getter:   (*Config).FetcherRequestTimeout,
			expected: 30 * time.Second,
		},
		{
			name:     "request_timeout_defaults_when_negative",
			cfg:      Config{Fetcher: FetcherConfig{RequestTimeoutSec: -1}},
			getter:   (*Config).FetcherRequestTimeout,
			expected: 30 * time.Second,
		},
		{
			name:     "request_timeout_uses_configured_value",
			cfg:      Config{Fetcher: FetcherConfig{RequestTimeoutSec: 42}},
			getter:   (*Config).FetcherRequestTimeout,
			expected: 42 * time.Second,
		},
		{
			name:     "discovery_interval_defaults_when_zero",
			cfg:      Config{Fetcher: FetcherConfig{DiscoveryIntervalSec: 0}},
			getter:   (*Config).FetcherDiscoveryInterval,
			expected: time.Minute,
		},
		{
			name:     "discovery_interval_defaults_when_negative",
			cfg:      Config{Fetcher: FetcherConfig{DiscoveryIntervalSec: -5}},
			getter:   (*Config).FetcherDiscoveryInterval,
			expected: time.Minute,
		},
		{
			name:     "discovery_interval_uses_configured_value",
			cfg:      Config{Fetcher: FetcherConfig{DiscoveryIntervalSec: 90}},
			getter:   (*Config).FetcherDiscoveryInterval,
			expected: 90 * time.Second,
		},
		{
			name:     "schema_cache_ttl_defaults_when_zero",
			cfg:      Config{Fetcher: FetcherConfig{SchemaCacheTTLSec: 0}},
			getter:   (*Config).FetcherSchemaCacheTTL,
			expected: 5 * time.Minute,
		},
		{
			name:     "schema_cache_ttl_defaults_when_negative",
			cfg:      Config{Fetcher: FetcherConfig{SchemaCacheTTLSec: -10}},
			getter:   (*Config).FetcherSchemaCacheTTL,
			expected: 5 * time.Minute,
		},
		{
			name:     "schema_cache_ttl_uses_configured_value",
			cfg:      Config{Fetcher: FetcherConfig{SchemaCacheTTLSec: 120}},
			getter:   (*Config).FetcherSchemaCacheTTL,
			expected: 120 * time.Second,
		},
		{
			name:     "extraction_poll_interval_defaults_when_zero",
			cfg:      Config{Fetcher: FetcherConfig{ExtractionPollSec: 0}},
			getter:   (*Config).FetcherExtractionPollInterval,
			expected: 5 * time.Second,
		},
		{
			name:     "extraction_poll_interval_defaults_when_negative",
			cfg:      Config{Fetcher: FetcherConfig{ExtractionPollSec: -3}},
			getter:   (*Config).FetcherExtractionPollInterval,
			expected: 5 * time.Second,
		},
		{
			name:     "extraction_poll_interval_uses_configured_value",
			cfg:      Config{Fetcher: FetcherConfig{ExtractionPollSec: 11}},
			getter:   (*Config).FetcherExtractionPollInterval,
			expected: 11 * time.Second,
		},
		{
			name:     "extraction_timeout_defaults_when_zero",
			cfg:      Config{Fetcher: FetcherConfig{ExtractionTimeoutSec: 0}},
			getter:   (*Config).FetcherExtractionTimeout,
			expected: 10 * time.Minute,
		},
		{
			name:     "extraction_timeout_defaults_when_negative",
			cfg:      Config{Fetcher: FetcherConfig{ExtractionTimeoutSec: -2}},
			getter:   (*Config).FetcherExtractionTimeout,
			expected: 10 * time.Minute,
		},
		{
			name:     "extraction_timeout_uses_configured_value",
			cfg:      Config{Fetcher: FetcherConfig{ExtractionTimeoutSec: 601}},
			getter:   (*Config).FetcherExtractionTimeout,
			expected: 601 * time.Second,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.getter(&tt.cfg))
		})
	}
}

func TestConnMaxIdleTime(t *testing.T) {
	t.Parallel()

	cfg := &Config{Postgres: PostgresConfig{ConnMaxIdleTimeMins: 5}}
	assert.Equal(t, 5*time.Minute, cfg.ConnMaxIdleTime())
}

func TestM2MCredentialCacheTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      Config
		expected time.Duration
	}{
		{
			name:     "defaults_to_5_minutes_when_zero",
			cfg:      Config{M2M: M2MConfig{M2MCredentialCacheTTLSec: 0}},
			expected: 5 * time.Minute,
		},
		{
			name:     "defaults_to_5_minutes_when_negative",
			cfg:      Config{M2M: M2MConfig{M2MCredentialCacheTTLSec: -1}},
			expected: 5 * time.Minute,
		},
		{
			name:     "uses_configured_value",
			cfg:      Config{M2M: M2MConfig{M2MCredentialCacheTTLSec: 600}},
			expected: 600 * time.Second,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.cfg.M2MCredentialCacheTTL())
		})
	}
}
