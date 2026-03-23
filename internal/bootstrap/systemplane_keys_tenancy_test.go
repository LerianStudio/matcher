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

// --- matcherKeyDefsTenancy ---

func TestMatcherKeyDefsTenancy_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancy()

	require.NotEmpty(t, defs, "matcherKeyDefsTenancy must return at least one key definition")
}

func TestMatcherKeyDefsTenancy_CombinesSubGroups(t *testing.T) {
	t.Parallel()

	defaults := matcherKeyDefsTenancyDefaults()
	connectivity := matcherKeyDefsTenancyConnectivity()
	resilience := matcherKeyDefsTenancyResilience()

	combined := matcherKeyDefsTenancy()

	assert.Len(t, combined, len(defaults)+len(connectivity)+len(resilience),
		"matcherKeyDefsTenancy must combine defaults + connectivity + resilience")
}

// --- matcherKeyDefsTenancyDefaults ---

func TestMatcherKeyDefsTenancyDefaults_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyDefaults()

	expectedKeys := []string{
		"tenancy.default_tenant_id",
		"tenancy.default_tenant_slug",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "tenancy", def.Group)
			assert.Equal(t, domain.ApplyBootstrapOnly, def.ApplyBehavior)
			assert.False(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsTenancyDefaults_AllImmutable(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyDefaults()

	for _, def := range defs {
		assert.False(t, def.MutableAtRuntime,
			"tenancy default %q must be immutable (bootstrap-only)", def.Key)
	}
}

// --- matcherKeyDefsTenancyConnectivity ---

func TestMatcherKeyDefsTenancyConnectivity_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyConnectivity()

	require.NotEmpty(t, defs)
}

func TestMatcherKeyDefsTenancyConnectivity_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyConnectivity()

	expectedKeys := []string{
		"tenancy.multi_tenant_enabled",
		"tenancy.multi_tenant_url",
		"tenancy.multi_tenant_environment",
		"tenancy.multi_tenant_max_tenant_pools",
		"tenancy.multi_tenant_idle_timeout_sec",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "tenancy", def.Group)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsTenancyConnectivity_EnabledIsBool(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyConnectivity()
	require.NotEmpty(t, defs)

	assert.Equal(t, "tenancy.multi_tenant_enabled", defs[0].Key)
	assert.Equal(t, domain.ValueTypeBool, defs[0].ValueType)
}

func TestMatcherKeyDefsTenancyConnectivity_IntFieldsHaveValidators(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyConnectivity()

	for _, def := range defs {
		if def.ValueType == domain.ValueTypeInt {
			t.Run(def.Key+"_has_validator", func(t *testing.T) {
				t.Parallel()

				assert.NotNil(t, def.Validator, "%s must have a validator", def.Key)
			})
		}
	}
}

// --- matcherKeyDefsTenancyResilience ---

func TestMatcherKeyDefsTenancyResilience_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyResilience()

	require.NotEmpty(t, defs)
}

func TestMatcherKeyDefsTenancyResilience_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyResilience()

	expectedKeys := []string{
		"tenancy.multi_tenant_circuit_breaker_threshold",
		"tenancy.multi_tenant_circuit_breaker_timeout_sec",
		"tenancy.multi_tenant_service_api_key",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "tenancy", def.Group)
			assert.Equal(t, domain.ApplyBundleRebuild, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsTenancyResilience_ServiceAPIKeyIsSecret(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyResilience()

	var found bool

	for _, def := range defs {
		if def.Key == "tenancy.multi_tenant_service_api_key" {
			found = true
			assert.True(t, def.Secret, "service API key must be marked as secret")
			assert.Equal(t, domain.RedactFull, def.RedactPolicy, "service API key must use full redaction")
		}
	}

	assert.True(t, found, "tenancy.multi_tenant_service_api_key must exist")
}

func TestMatcherKeyDefsTenancyResilience_IntFieldsHaveValidators(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsTenancyResilience()

	for _, def := range defs {
		if def.ValueType == domain.ValueTypeInt {
			t.Run(def.Key+"_has_validator", func(t *testing.T) {
				t.Parallel()

				assert.NotNil(t, def.Validator, "%s must have a validator", def.Key)
			})
		}
	}
}

