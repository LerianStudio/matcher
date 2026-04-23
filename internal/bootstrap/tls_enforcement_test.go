// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ValidateRequiredTLS enforces TLS per infra stack based on explicit
// X_TLS_REQUIRED flags. It is called once at bootstrap, BEFORE any
// infrastructure connection opens. The contract:
//
//   - cfg == nil: noop, returns nil.
//   - Each stack (Postgres primary, Postgres replica, Redis, RabbitMQ,
//     Object Storage) has an independent TLSRequired flag on its Config
//     sub-struct. When the flag is unset (default false), the stack is
//     unenforced regardless of its posture.
//   - When TLSRequired is true AND the stack is configured, the stack MUST
//     declare TLS. Non-TLS configuration fails with
//     ErrTLSRequiredButNotDeclared naming the offending stack.
//   - When TLSRequired is true AND the stack is not configured (empty DSN /
//     host), the check is skipped — an absent stack is not forced into being.
//   - Malformed configuration under TLSRequired=true fails closed with
//     ErrTLSMalformedDependencyConfig.
//   - Deployment mode is NOT consulted. TLS enforcement is orthogonal to
//     cfg.App.Mode.

func TestValidateRequiredTLS_NilConfigNoop(t *testing.T) {
	t.Parallel()

	assert.NoError(t, ValidateRequiredTLS(nil))
}

func TestValidateRequiredTLS_NoFlagsSetNoEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{"local_mode_no_enforcement", "local"},
		{"byoc_mode_no_enforcement", "byoc"},
		{"saas_mode_no_enforcement", "saas"}, // previously gated; now explicit opt-in
		{"empty_mode_no_enforcement", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := cfgWithMode(tt.mode)
			// Every stack plaintext, every TLS_REQUIRED flag unset → no enforcement.
			cfg.Postgres.PrimarySSLMode = "disable"
			cfg.Redis.TLS = false
			cfg.RabbitMQ.URI = "amqp"
			cfg.ObjectStorage.Endpoint = "http://localhost:8333"

			assert.NoError(t, ValidateRequiredTLS(cfg))
		})
	}
}

func TestValidateRequiredTLS_PostgresRequired(t *testing.T) {
	t.Parallel()

	t.Run("tls_required_true_plaintext_fails", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Postgres.TLSRequired = true
		cfg.Postgres.PrimarySSLMode = "disable"

		err := ValidateRequiredTLS(cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTLSRequiredButNotDeclared)
		assert.Contains(t, strings.ToLower(err.Error()), "postgres")
	})

	t.Run("tls_required_true_tls_configured_passes", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Postgres.TLSRequired = true
		cfg.Postgres.PrimarySSLMode = "require"

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})

	t.Run("tls_required_false_plaintext_passes", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Postgres.TLSRequired = false
		cfg.Postgres.PrimarySSLMode = "disable"

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})
}

// TestValidateRequiredTLS_PostgresReplicaRequired exercises the replica-specific
// check path. The replica flag only fires when a distinct replica host is
// configured; otherwise ReplicaDSN() falls back to the primary and the check
// is a no-op.
func TestValidateRequiredTLS_PostgresReplicaRequired(t *testing.T) {
	t.Parallel()

	t.Run("replica_required_true_plaintext_fails", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Postgres.PrimarySSLMode = "require"
		cfg.Postgres.ReplicaHost = "replica.example.com"
		cfg.Postgres.ReplicaSSLMode = "disable"
		cfg.Postgres.ReplicaTLSRequired = true

		err := ValidateRequiredTLS(cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTLSRequiredButNotDeclared)
		assert.Contains(t, strings.ToLower(err.Error()), "postgres_replica",
			"error must name the replica stack, not the primary")
	})

	t.Run("replica_required_true_tls_configured_passes", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Postgres.PrimarySSLMode = "require"
		cfg.Postgres.ReplicaHost = "replica.example.com"
		cfg.Postgres.ReplicaSSLMode = "require"
		cfg.Postgres.ReplicaTLSRequired = true

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})

	t.Run("replica_required_true_but_replica_not_configured_skipped", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Postgres.PrimarySSLMode = "require"
		cfg.Postgres.ReplicaHost = "" // no distinct replica
		cfg.Postgres.ReplicaTLSRequired = true

		// No distinct replica configured — flag is a no-op.
		assert.NoError(t, ValidateRequiredTLS(cfg))
	})
}

func TestValidateRequiredTLS_RedisRequired(t *testing.T) {
	t.Parallel()

	t.Run("tls_required_true_plaintext_fails", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Redis.TLSRequired = true
		cfg.Redis.TLS = false

		err := ValidateRequiredTLS(cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTLSRequiredButNotDeclared)
		assert.Contains(t, strings.ToLower(err.Error()), "redis")
	})

	t.Run("tls_required_true_tls_on_passes", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Redis.TLSRequired = true
		cfg.Redis.TLS = true

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})

	t.Run("tls_required_true_but_redis_not_configured_skipped", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Redis.Host = "" // not configured
		cfg.Redis.TLSRequired = true

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})

	// Malformed Redis host under TLS_REQUIRED=true must fail closed with
	// ErrTLSMalformedDependencyConfig, not silently pass.
	t.Run("tls_required_true_malformed_host_fails_closed", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.Redis.TLSRequired = true
		cfg.Redis.TLS = true
		cfg.Redis.Host = "[::1" // invalid IPv6 literal → url.Parse fails

		err := ValidateRequiredTLS(cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTLSMalformedDependencyConfig)
		assert.Contains(t, err.Error(), "redis")
		assert.NotContains(t, strings.ToLower(err.Error()), "parse",
			"raw parser details must not leak into the returned error text")
	})
}

func TestValidateRequiredTLS_RabbitMQRequired(t *testing.T) {
	t.Parallel()

	t.Run("tls_required_true_plaintext_fails", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.RabbitMQ.TLSRequired = true
		cfg.RabbitMQ.URI = "amqp"

		err := ValidateRequiredTLS(cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTLSRequiredButNotDeclared)
		assert.Contains(t, strings.ToLower(err.Error()), "rabbitmq")
	})

	t.Run("tls_required_true_amqps_passes", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.RabbitMQ.TLSRequired = true
		cfg.RabbitMQ.URI = "amqps"

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})
}

func TestValidateRequiredTLS_ObjectStorageRequired(t *testing.T) {
	t.Parallel()

	t.Run("tls_required_true_http_endpoint_fails", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.ObjectStorage.TLSRequired = true
		cfg.ObjectStorage.Endpoint = "http://insecure.example.com"

		err := ValidateRequiredTLS(cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTLSRequiredButNotDeclared)
		assert.Contains(t, strings.ToLower(err.Error()), "object_storage")
	})

	t.Run("tls_required_true_https_endpoint_passes", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.ObjectStorage.TLSRequired = true
		cfg.ObjectStorage.Endpoint = "https://s3.example.com"

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})

	// Empty endpoint == AWS default (HTTPS). TLS_REQUIRED=true must not
	// mistake the AWS default for a plaintext configuration.
	t.Run("tls_required_true_empty_endpoint_aws_default_passes", func(t *testing.T) {
		t.Parallel()

		cfg := cfgWithMode("local")
		cfg.ObjectStorage.TLSRequired = true
		cfg.ObjectStorage.Endpoint = ""

		assert.NoError(t, ValidateRequiredTLS(cfg))
	})
}

// TestValidateRequiredTLS_AllStacksRequiredAndTLS is a happy-path integration
// check for a fully-enforced production posture.
func TestValidateRequiredTLS_AllStacksRequiredAndTLS(t *testing.T) {
	t.Parallel()

	cfg := cfgWithMode("saas")
	cfg.Postgres.PrimarySSLMode = "require"
	cfg.Postgres.TLSRequired = true
	cfg.Postgres.ReplicaHost = "replica.example.com"
	cfg.Postgres.ReplicaSSLMode = "require"
	cfg.Postgres.ReplicaTLSRequired = true
	cfg.Redis.TLS = true
	cfg.Redis.TLSRequired = true
	cfg.RabbitMQ.URI = "amqps"
	cfg.RabbitMQ.TLSRequired = true
	cfg.ObjectStorage.Endpoint = "https://s3.example.com"
	cfg.ObjectStorage.TLSRequired = true

	assert.NoError(t, ValidateRequiredTLS(cfg))
}

// TestValidateRequiredTLS_SelectiveEnforcement covers the common real-world
// case: Postgres and Redis enforced, RabbitMQ unenforced, object storage
// unenforced. Only the flagged stacks are checked.
func TestValidateRequiredTLS_SelectiveEnforcement(t *testing.T) {
	t.Parallel()

	cfg := cfgWithMode("byoc")
	// Enforced and TLS-on → pass.
	cfg.Postgres.PrimarySSLMode = "require"
	cfg.Postgres.TLSRequired = true
	cfg.Redis.TLS = true
	cfg.Redis.TLSRequired = true
	// Unenforced, plaintext → no error.
	cfg.RabbitMQ.URI = "amqp"
	cfg.RabbitMQ.TLSRequired = false
	cfg.ObjectStorage.Endpoint = "http://localhost:8333"
	cfg.ObjectStorage.TLSRequired = false

	assert.NoError(t, ValidateRequiredTLS(cfg))
}

// TestBuildRedisURLForTLSCheck pins the current behaviour of the URL
// synthesis helper. It is deliberately narrow — scheme choice, empty-host
// short-circuit, and comma-separated host handling. Future refactors that
// touch this helper must keep these contracts or update the tests explicitly.
func TestBuildRedisURLForTLSCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *Config
		want string
	}{
		{
			name: "nil_config_returns_empty",
			cfg:  nil,
			want: "",
		},
		{
			name: "empty_host_returns_empty",
			cfg:  &Config{Redis: RedisConfig{Host: "", TLS: true}},
			want: "",
		},
		{
			name: "single_host_tls_off_uses_redis_scheme",
			cfg:  &Config{Redis: RedisConfig{Host: "localhost:6379", TLS: false}},
			want: "redis://localhost:6379",
		},
		{
			name: "single_host_tls_on_uses_rediss_scheme",
			cfg:  &Config{Redis: RedisConfig{Host: "localhost:6379", TLS: true}},
			want: "rediss://localhost:6379",
		},
		{
			name: "comma_separated_hosts_tls_on_first_only",
			// cluster/sentinel topologies use a comma-separated list; only
			// the first address is sufficient for scheme detection.
			cfg:  &Config{Redis: RedisConfig{Host: "host1:6379,host2:6379", TLS: true}},
			want: "rediss://host1:6379",
		},
		{
			name: "comma_separated_hosts_tls_off_first_only",
			cfg:  &Config{Redis: RedisConfig{Host: "host1:6379,host2:6379", TLS: false}},
			want: "redis://host1:6379",
		},
		{
			name: "sentinel_mode_with_master_name_uses_host_only",
			// MasterName is not surfaced — the helper is only used to detect
			// scheme, never to dial. This is intentional; callers must not use
			// the return value as a real connection string.
			cfg: &Config{Redis: RedisConfig{
				Host:       "sentinel1:26379,sentinel2:26379",
				MasterName: "mymaster",
				TLS:        true,
			}},
			want: "rediss://sentinel1:26379",
		},
		{
			name: "leading_comma_produces_empty_address",
			// The helper skips empty segments and selects the first non-empty
			// host so malformed leading separators are not treated as valid.
			cfg:  &Config{Redis: RedisConfig{Host: ",host2:6379", TLS: true}},
			want: "rediss://host2:6379",
		},
		{
			name: "all_empty_segments_return_empty",
			cfg:  &Config{Redis: RedisConfig{Host: " , , ", TLS: true}},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := buildRedisURLForTLSCheck(tc.cfg)
			assert.Equal(t, tc.want, got)
		})
	}
}

// cfgWithMode returns a minimal Config with common fields populated plus the
// supplied deployment mode. Tests tweak fields per scenario. The mode is
// informational only in this module (it does not gate enforcement); tests
// set it mainly to keep the fixture realistic.
func cfgWithMode(mode string) *Config {
	return &Config{
		App: AppConfig{
			Mode:    mode,
			EnvName: "development",
		},
		Postgres: PostgresConfig{
			PrimaryHost:     "pg.example.com",
			PrimaryPort:     "5432",
			PrimaryUser:     "u",
			PrimaryPassword: "p",
			PrimaryDB:       "db",
		},
		Redis: RedisConfig{
			Host: "redis.example.com:6380",
		},
		RabbitMQ: RabbitMQConfig{
			URI:  "amqps",
			Host: "rabbit.example.com",
			Port: "5671",
			User: "u",
		},
		ObjectStorage: ObjectStorageConfig{
			Endpoint: "https://s3.example.com",
		},
	}
}
