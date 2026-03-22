//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// --- matcherKeyDefsRedisRabbitMQ ---

func TestMatcherKeyDefsRedisRabbitMQ_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRedisRabbitMQ()

	require.NotEmpty(t, defs, "matcherKeyDefsRedisRabbitMQ must return at least one key definition")
}

func TestMatcherKeyDefsRedisRabbitMQ_CombinesSubGroups(t *testing.T) {
	t.Parallel()

	redisCore := matcherKeyDefsRedisCore()
	redisRuntime := matcherKeyDefsRedisRuntime()
	rabbitConn := matcherKeyDefsRabbitMQConnection()
	rabbitHealth := matcherKeyDefsRabbitMQHealth()

	combined := matcherKeyDefsRedisRabbitMQ()

	assert.Len(t, combined, len(redisCore)+len(redisRuntime)+len(rabbitConn)+len(rabbitHealth),
		"matcherKeyDefsRedisRabbitMQ must combine redis core + redis runtime + rabbitmq connection + rabbitmq health")
}

// --- matcherKeyDefsRedisCore ---

func TestMatcherKeyDefsRedisCore_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRedisCore()

	expectedKeys := []string{
		"redis.host",
		"redis.master_name",
		"redis.password",
		"redis.db",
		"redis.protocol",
		"redis.tls",
		"redis.ca_cert",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "redis", def.Group)
			assert.Equal(t, "redis", def.Component)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsRedisCore_PasswordIsSecret(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRedisCore()

	var found bool

	for _, def := range defs {
		if def.Key == "redis.password" {
			found = true
			assert.True(t, def.Secret, "redis.password must be marked as secret")
			assert.Equal(t, domain.RedactFull, def.RedactPolicy, "redis.password must use full redaction")
		}
	}

	assert.True(t, found, "redis.password must exist")
}

func TestMatcherKeyDefsRedisCore_CACertIsSecret(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRedisCore()

	var found bool

	for _, def := range defs {
		if def.Key == "redis.ca_cert" {
			found = true
			assert.True(t, def.Secret, "redis.ca_cert must be marked as secret")
			assert.Equal(t, domain.RedactFull, def.RedactPolicy, "redis.ca_cert must use full redaction")
		}
	}

	assert.True(t, found, "redis.ca_cert must exist")
}

func TestMatcherKeyDefsRedisCore_DBHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRedisCore()

	var found bool

	for _, def := range defs {
		if def.Key == "redis.db" {
			found = true
			assert.NotNil(t, def.Validator, "redis.db must have a validator")
			assert.Equal(t, domain.ValueTypeInt, def.ValueType)
		}
	}

	assert.True(t, found, "redis.db must exist")
}

// --- matcherKeyDefsRedisRuntime ---

func TestMatcherKeyDefsRedisRuntime_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRedisRuntime()

	expectedKeys := []string{
		"redis.pool_size",
		"redis.min_idle_conn",
		"redis.read_timeout_ms",
		"redis.write_timeout_ms",
		"redis.dial_timeout_ms",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "redis", def.Group)
			assert.Equal(t, "redis", def.Component)
			assert.Equal(t, domain.ValueTypeInt, def.ValueType)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsRedisRuntime_AllHaveValidators(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRedisRuntime()

	for _, def := range defs {
		t.Run(def.Key+"_has_validator", func(t *testing.T) {
			t.Parallel()

			assert.NotNil(t, def.Validator, "%s must have a validator", def.Key)
		})
	}
}

// --- matcherKeyDefsRabbitMQConnection ---

func TestMatcherKeyDefsRabbitMQConnection_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRabbitMQConnection()

	expectedKeys := []string{
		"rabbitmq.uri",
		"rabbitmq.host",
		"rabbitmq.port",
		"rabbitmq.user",
		"rabbitmq.password",
		"rabbitmq.vhost",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "rabbitmq", def.Group)
			assert.Equal(t, "rabbitmq", def.Component)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsRabbitMQConnection_PasswordIsSecret(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRabbitMQConnection()

	var found bool

	for _, def := range defs {
		if def.Key == "rabbitmq.password" {
			found = true
			assert.True(t, def.Secret, "rabbitmq.password must be marked as secret")
			assert.Equal(t, domain.RedactFull, def.RedactPolicy, "rabbitmq.password must use full redaction")
		}
	}

	assert.True(t, found, "rabbitmq.password must exist")
}

// --- matcherKeyDefsRabbitMQHealth ---

func TestMatcherKeyDefsRabbitMQHealth_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRabbitMQHealth()

	expectedKeys := []string{
		"rabbitmq.health_url",
		"rabbitmq.allow_insecure_health_check",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "rabbitmq", def.Group)
			assert.Equal(t, "rabbitmq", def.Component)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsRabbitMQHealth_InsecureCheckIsBool(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRabbitMQHealth()

	var found bool

	for _, def := range defs {
		if def.Key == "rabbitmq.allow_insecure_health_check" {
			found = true
			assert.Equal(t, domain.ValueTypeBool, def.ValueType)
		}
	}

	assert.True(t, found, "rabbitmq.allow_insecure_health_check must exist")
}
