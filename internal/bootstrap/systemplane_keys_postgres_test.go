//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
)

func TestMatcherKeyDefsPostgres_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgres()

	require.NotEmpty(t, defs, "matcherKeyDefsPostgres must return at least one key definition")
}

func TestMatcherKeyDefsPostgres_CombinesAllSubGroups(t *testing.T) {
	t.Parallel()

	primary := matcherKeyDefsPostgresPrimary()
	replica := matcherKeyDefsPostgresReplica()
	pooling := matcherKeyDefsPostgresPooling()
	operations := matcherKeyDefsPostgresOperations()

	combined := matcherKeyDefsPostgres()

	expected := len(primary) + len(replica) + len(pooling) + len(operations)
	assert.Len(t, combined, expected,
		"matcherKeyDefsPostgres must combine primary + replica + pooling + operations")
}

func TestMatcherKeyDefsPostgresPrimary_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresPrimary()

	require.Len(t, defs, 6)

	expectedKeys := []string{
		"postgres.primary_host",
		"postgres.primary_port",
		"postgres.primary_user",
		"postgres.primary_password",
		"postgres.primary_db",
		"postgres.primary_ssl_mode",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "postgres", def.Group)
			assert.Equal(t, "postgres", def.Component)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsPostgresPrimary_PasswordIsSecret(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresPrimary()
	require.Len(t, defs, 6)

	passwordDef := defs[3]
	assert.Equal(t, "postgres.primary_password", passwordDef.Key)
	assert.True(t, passwordDef.Secret, "primary_password must be marked as Secret")
	assert.Equal(t, domain.RedactFull, passwordDef.RedactPolicy,
		"primary_password must use RedactFull")
}

func TestMatcherKeyDefsPostgresPrimary_SSLModeHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresPrimary()
	require.Len(t, defs, 6)

	sslDef := defs[5]
	assert.Equal(t, "postgres.primary_ssl_mode", sslDef.Key)
	assert.NotNil(t, sslDef.Validator, "primary_ssl_mode must have a validator")
}

func TestMatcherKeyDefsPostgresPrimary_HostHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresPrimary()
	require.NotEmpty(t, defs)

	hostDef := defs[0]
	assert.Equal(t, "postgres.primary_host", hostDef.Key)
	assert.NotNil(t, hostDef.Validator, "primary_host must have a validator")
}

func TestMatcherKeyDefsPostgresReplica_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresReplica()

	require.Len(t, defs, 6)

	expectedKeys := []string{
		"postgres.replica_host",
		"postgres.replica_port",
		"postgres.replica_user",
		"postgres.replica_password",
		"postgres.replica_db",
		"postgres.replica_ssl_mode",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "postgres", def.Group)
			assert.Equal(t, "postgres", def.Component)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsPostgresReplica_PasswordIsSecret(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresReplica()
	require.Len(t, defs, 6)

	passwordDef := defs[3]
	assert.Equal(t, "postgres.replica_password", passwordDef.Key)
	assert.True(t, passwordDef.Secret, "replica_password must be marked as Secret")
	assert.Equal(t, domain.RedactFull, passwordDef.RedactPolicy,
		"replica_password must use RedactFull")
}

func TestMatcherKeyDefsPostgresReplica_SSLModeHasOptionalValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresReplica()
	require.Len(t, defs, 6)

	sslDef := defs[5]
	assert.Equal(t, "postgres.replica_ssl_mode", sslDef.Key)
	assert.NotNil(t, sslDef.Validator, "replica_ssl_mode must have a validator")
}

func TestMatcherKeyDefsPostgresReplica_EmptyDefaults(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresReplica()

	for _, def := range defs {
		t.Run(def.Key, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, "", def.DefaultValue,
				"replica key %q must have empty default (replica is optional)", def.Key)
		})
	}
}

func TestMatcherKeyDefsPostgresPooling_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresPooling()

	require.Len(t, defs, 6)

	expectedKeys := []string{
		"postgres.max_open_connections",
		"postgres.max_idle_connections",
		"postgres.conn_max_lifetime_mins",
		"postgres.conn_max_idle_time_mins",
		"postgres.connect_timeout_sec",
		"postgres.query_timeout_sec",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "postgres", def.Group)
			assert.Equal(t, domain.ValueTypeInt, def.ValueType)
			assert.NotNil(t, def.Validator,
				"pooling key %q must have a validator", def.Key)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsPostgresPooling_QueryTimeoutIsLiveRead(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresPooling()
	require.Len(t, defs, 6)

	queryTimeoutDef := defs[5]
	assert.Equal(t, "postgres.query_timeout_sec", queryTimeoutDef.Key)
	assert.Equal(t, domain.ApplyLiveRead, queryTimeoutDef.ApplyBehavior,
		"query_timeout_sec must be live-read for dynamic tuning")
}

func TestMatcherKeyDefsPostgresPooling_OtherKeysAreBundleRebuild(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresPooling()

	for _, def := range defs {
		if def.Key == "postgres.query_timeout_sec" {
			continue // already tested separately
		}

		t.Run(def.Key, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior,
				"pooling key %q must be bundle-rebuild", def.Key)
		})
	}
}

func TestMatcherKeyDefsPostgresOperations_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgresOperations()

	require.Len(t, defs, 1)

	def := defs[0]
	assert.Equal(t, "postgres.migrations_path", def.Key)
	assert.Equal(t, domain.KindConfig, def.Kind)
	assert.Equal(t, "postgres", def.Group)
	assert.Equal(t, domain.ValueTypeString, def.ValueType)
	assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
	assert.True(t, def.MutableAtRuntime)
	assert.NotEmpty(t, def.Description)
}

func TestMatcherKeyDefsPostgres_NoSecretKeysExceptPasswords(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgres()
	secretKeys := []string{"postgres.primary_password", "postgres.replica_password"}

	for _, def := range defs {
		isPasswordKey := false

		for _, sk := range secretKeys {
			if def.Key == sk {
				isPasswordKey = true

				break
			}
		}

		if isPasswordKey {
			assert.True(t, def.Secret,
				"password key %q must be marked as Secret", def.Key)
		} else {
			assert.False(t, def.Secret,
				"non-password key %q must not be marked as Secret", def.Key)
		}
	}
}

func TestMatcherKeyDefsPostgres_AllKeysHaveGlobalScope(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgres()

	for _, def := range defs {
		require.Len(t, def.AllowedScopes, 1,
			"postgres key %q must have exactly one allowed scope", def.Key)
		assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0],
			"postgres key %q must have ScopeGlobal", def.Key)
	}
}

func TestMatcherKeyDefsPostgres_AllKeysHavePostgresComponent(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsPostgres()

	for _, def := range defs {
		assert.Equal(t, "postgres", def.Component,
			"postgres key %q must have 'postgres' component", def.Key)
	}
}
