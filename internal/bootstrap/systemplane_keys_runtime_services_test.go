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

func TestMatcherKeyDefsInfrastructure_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsInfrastructure()

	require.NotEmpty(t, defs, "matcherKeyDefsInfrastructure must return at least one key definition")
}

func TestMatcherKeyDefsInfrastructure_CombinesAllSubGroups(t *testing.T) {
	t.Parallel()

	runtime := matcherKeyDefsInfrastructureRuntime()
	idempotency := matcherKeyDefsIdempotency()
	callbackRL := matcherKeyDefsCallbackRateLimit()
	fetcherCore := matcherKeyDefsFetcherCore()
	fetcherRuntime := matcherKeyDefsFetcherRuntime()
	m2m := matcherKeyDefsM2M()

	combined := matcherKeyDefsInfrastructure()

	expected := len(runtime) + len(idempotency) + len(callbackRL) + len(fetcherCore) + len(fetcherRuntime) + len(m2m)
	assert.Len(t, combined, expected,
		"matcherKeyDefsInfrastructure must combine all sub-group key defs")
}

func TestMatcherKeyDefsInfrastructureRuntime_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsInfrastructureRuntime()

	require.Len(t, defs, 2)

	tests := []struct {
		key      string
		behavior domain.ApplyBehavior
		mutable  bool
		group    string
	}{
		{
			key:      "infrastructure.connect_timeout_sec",
			behavior: domain.ApplyBootstrapOnly,
			mutable:  false,
			group:    "infrastructure",
		},
		{
			key:      "infrastructure.health_check_timeout_sec",
			behavior: domain.ApplyLiveRead,
			mutable:  true,
			group:    "infrastructure",
		},
	}

	for i, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, tt.key, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, tt.group, def.Group)
			assert.Equal(t, tt.behavior, def.ApplyBehavior)
			assert.Equal(t, tt.mutable, def.MutableAtRuntime)
			assert.Equal(t, domain.ValueTypeInt, def.ValueType)
			assert.NotNil(t, def.Validator, "integer key %q must have a validator", def.Key)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsIdempotency_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsIdempotency()

	require.Len(t, defs, 3)

	expectedKeys := []string{
		"idempotency.retry_window_sec",
		"idempotency.success_ttl_hours",
		"idempotency.hmac_secret",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			if def.Key == "idempotency.hmac_secret" {
				assert.Equal(t, domain.KindConfig, def.Kind)
				require.Len(t, def.AllowedScopes, 1)
				assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
			} else {
				assert.Equal(t, domain.KindSetting, def.Kind)
				assert.Equal(t, []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, def.AllowedScopes)
			}
			assert.Equal(t, "idempotency", def.Group)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsIdempotency_HMACSecretIsSecretAndImmutable(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsIdempotency()
	require.Len(t, defs, 3)

	hmacDef := defs[2]
	assert.Equal(t, "idempotency.hmac_secret", hmacDef.Key)
	assert.True(t, hmacDef.Secret, "hmac_secret must be marked as Secret")
	assert.Equal(t, domain.RedactFull, hmacDef.RedactPolicy, "hmac_secret must use RedactFull")
	assert.Equal(t, domain.ApplyBootstrapOnly, hmacDef.ApplyBehavior, "hmac_secret must be bootstrap-only")
	assert.False(t, hmacDef.MutableAtRuntime, "hmac_secret must be immutable at runtime")
}

func TestMatcherKeyDefsIdempotency_RetryWindowIsLiveRead(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsIdempotency()
	require.NotEmpty(t, defs)

	retryDef := defs[0]
	assert.Equal(t, domain.ApplyLiveRead, retryDef.ApplyBehavior)
	assert.True(t, retryDef.MutableAtRuntime)
}

func TestMatcherKeyDefsCallbackRateLimit_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsCallbackRateLimit()

	require.Len(t, defs, 1)

	def := defs[0]
	assert.Equal(t, "callback_rate_limit.per_minute", def.Key)
	assert.Equal(t, domain.KindSetting, def.Kind)
	assert.Equal(t, []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, def.AllowedScopes)
	assert.Equal(t, "callback_rate_limit", def.Group)
	assert.Equal(t, domain.ValueTypeInt, def.ValueType)
	assert.NotNil(t, def.Validator)
	assert.Equal(t, domain.ApplyLiveRead, def.ApplyBehavior)
	assert.True(t, def.MutableAtRuntime)
	assert.NotEmpty(t, def.Description)
}

func TestMatcherKeyDefsFetcherCore_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsFetcherCore()

	require.Len(t, defs, 4)

	expectedKeys := []string{
		"fetcher.enabled",
		"fetcher.url",
		"fetcher.allow_private_ips",
		"fetcher.health_timeout_sec",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "fetcher", def.Group)
			assert.NotEmpty(t, def.Description)
			assert.True(t, def.MutableAtRuntime, "fetcher core key %q must be mutable", def.Key)
		})
	}
}

func TestMatcherKeyDefsFetcherCore_EnabledIsBundleRebuildAndReconcile(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsFetcherCore()
	require.NotEmpty(t, defs)

	enabledDef := defs[0]
	assert.Equal(t, "fetcher.enabled", enabledDef.Key)
	assert.Equal(t, domain.ApplyBundleRebuildAndReconcile, enabledDef.ApplyBehavior)
}

func TestMatcherKeyDefsFetcherCore_URLHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsFetcherCore()
	require.True(t, len(defs) >= 2)

	urlDef := defs[1]
	assert.Equal(t, "fetcher.url", urlDef.Key)
	assert.NotNil(t, urlDef.Validator, "fetcher.url must have a URL validator")
}

func TestMatcherKeyDefsFetcherRuntime_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsFetcherRuntime()

	require.Len(t, defs, 5)

	expectedKeys := []string{
		"fetcher.request_timeout_sec",
		"fetcher.discovery_interval_sec",
		"fetcher.schema_cache_ttl_sec",
		"fetcher.extraction_poll_sec",
		"fetcher.extraction_timeout_sec",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "fetcher", def.Group)
			assert.Equal(t, domain.ValueTypeInt, def.ValueType)
			assert.NotNil(t, def.Validator, "integer key %q must have a validator", def.Key)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsFetcherRuntime_DiscoveryIntervalIsWorkerReconcile(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsFetcherRuntime()
	require.True(t, len(defs) >= 2)

	discoveryDef := defs[1]
	assert.Equal(t, "fetcher.discovery_interval_sec", discoveryDef.Key)
	assert.Equal(t, domain.ApplyWorkerReconcile, discoveryDef.ApplyBehavior)
}
