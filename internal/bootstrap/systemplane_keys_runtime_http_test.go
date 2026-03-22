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

func TestMatcherKeyDefsRuntimeHTTP_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRuntimeHTTP()

	require.NotEmpty(t, defs, "matcherKeyDefsRuntimeHTTP must return at least one key definition")
}

func TestMatcherKeyDefsRuntimeHTTP_CombinesAllSubGroups(t *testing.T) {
	t.Parallel()

	auth := matcherKeyDefsAuth()
	swagger := matcherKeyDefsSwagger()
	telemetry := matcherKeyDefsTelemetry()
	rateLimit := matcherKeyDefsRateLimit()

	combined := matcherKeyDefsRuntimeHTTP()

	expected := len(auth) + len(swagger) + len(telemetry) + len(rateLimit)
	assert.Len(t, combined, expected,
		"matcherKeyDefsRuntimeHTTP must combine auth + swagger + telemetry + rate_limit")
}

func TestMatcherKeyDefsAuth_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsAuth()

	require.Len(t, defs, 3)

	expectedKeys := []string{
		"auth.enabled",
		"auth.host",
		"auth.token_secret",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "auth", def.Group)
			assert.Equal(t, domain.ApplyBootstrapOnly, def.ApplyBehavior)
			assert.False(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsAuth_TokenSecretIsSecret(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsAuth()
	require.Len(t, defs, 3)

	tokenDef := defs[2]
	assert.Equal(t, "auth.token_secret", tokenDef.Key)
	assert.True(t, tokenDef.Secret, "token_secret must be marked as Secret")
	assert.Equal(t, domain.RedactFull, tokenDef.RedactPolicy, "token_secret must use RedactFull")
}

func TestMatcherKeyDefsSwagger_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsSwagger()

	require.Len(t, defs, 3)

	expectedKeys := []string{
		"swagger.enabled",
		"swagger.host",
		"swagger.schemes",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "swagger", def.Group)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.Equal(t, "http", def.Component)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsTelemetry_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTelemetry()

	require.Len(t, defs, 7)

	expectedKeys := []string{
		"telemetry.enabled",
		"telemetry.service_name",
		"telemetry.library_name",
		"telemetry.service_version",
		"telemetry.deployment_env",
		"telemetry.collector_endpoint",
		"telemetry.db_metrics_interval_sec",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "telemetry", def.Group)
			assert.Equal(t, domain.ApplyBootstrapOnly, def.ApplyBehavior,
				"all telemetry keys must be bootstrap-only")
			assert.False(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsTelemetry_DBMetricsHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTelemetry()
	require.Len(t, defs, 7)

	dbMetricsDef := defs[6]
	assert.Equal(t, "telemetry.db_metrics_interval_sec", dbMetricsDef.Key)
	assert.Equal(t, domain.ValueTypeInt, dbMetricsDef.ValueType)
	assert.NotNil(t, dbMetricsDef.Validator, "db_metrics_interval_sec must have a validator")
}

func TestMatcherKeyDefsRateLimit_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRateLimit()

	require.Len(t, defs, 7)

	expectedKeys := []string{
		"rate_limit.enabled",
		"rate_limit.max",
		"rate_limit.expiry_sec",
		"rate_limit.export_max",
		"rate_limit.export_expiry_sec",
		"rate_limit.dispatch_max",
		"rate_limit.dispatch_expiry_sec",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "rate_limit", def.Group)
			assert.Equal(t, domain.ApplyLiveRead, def.ApplyBehavior,
				"rate_limit key %q must be live-read", def.Key)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsRateLimit_IntKeysHaveValidators(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRateLimit()

	for _, def := range defs {
		if def.ValueType == domain.ValueTypeInt {
			t.Run(def.Key, func(t *testing.T) {
				t.Parallel()

				assert.NotNil(t, def.Validator,
					"integer rate_limit key %q must have a validator", def.Key)
			})
		}
	}
}

func TestMatcherKeyDefsRateLimit_NoSecretKeys(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsRateLimit()

	for _, def := range defs {
		assert.False(t, def.Secret,
			"rate_limit key %q must not be a secret", def.Key)
		assert.Equal(t, domain.RedactNone, def.RedactPolicy,
			"rate_limit key %q must have RedactNone policy", def.Key)
	}
}
