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

func TestMatcherKeyDefsAppServer_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsAppServer()

	require.NotEmpty(t, defs, "matcherKeyDefsAppServer must return at least one key definition")
}

func TestMatcherKeyDefsAppServer_CombinesAllSubGroups(t *testing.T) {
	t.Parallel()

	app := matcherKeyDefsApp()
	http := matcherKeyDefsServerHTTP()
	tls := matcherKeyDefsServerTLS()

	combined := matcherKeyDefsAppServer()

	assert.Len(t, combined, len(app)+len(http)+len(tls),
		"matcherKeyDefsAppServer must combine app + http + tls key defs")
}

func TestMatcherKeyDefsApp_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsApp()

	require.Len(t, defs, 2, "matcherKeyDefsApp must define exactly 2 keys")

	tests := []struct {
		name      string
		key       string
		group     string
		valueType domain.ValueType
		behavior  domain.ApplyBehavior
		mutable   bool
	}{
		{
			name:      "env_name",
			key:       "app.env_name",
			group:     "app",
			valueType: domain.ValueTypeString,
			behavior:  domain.ApplyBootstrapOnly,
			mutable:   false,
		},
		{
			name:      "log_level",
			key:       "app.log_level",
			group:     "app",
			valueType: domain.ValueTypeString,
			behavior:  domain.ApplyBundleRebuild,
			mutable:   true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, tt.key, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, tt.group, def.Group)
			assert.Equal(t, tt.valueType, def.ValueType)
			assert.Equal(t, tt.behavior, def.ApplyBehavior)
			assert.Equal(t, tt.mutable, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsServerHTTP_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsServerHTTP()

	require.NotEmpty(t, defs, "matcherKeyDefsServerHTTP must return key definitions")

	expectedKeys := []string{
		"server.address",
		"server.body_limit_bytes",
		"server.cors_allowed_origins",
		"server.cors_allowed_methods",
		"server.cors_allowed_headers",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "server", def.Group)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsServerHTTP_AddressIsBootstrapOnly(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsServerHTTP()
	require.NotEmpty(t, defs)

	// First key is server.address
	assert.Equal(t, domain.ApplyBootstrapOnly, defs[0].ApplyBehavior)
	assert.False(t, defs[0].MutableAtRuntime)
}

func TestMatcherKeyDefsServerHTTP_BodyLimitHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsServerHTTP()
	require.True(t, len(defs) >= 2, "need at least 2 server HTTP key defs")

	bodyLimitDef := defs[1]
	assert.Equal(t, "server.body_limit_bytes", bodyLimitDef.Key)
	assert.NotNil(t, bodyLimitDef.Validator, "body_limit_bytes must have a validator")
}

func TestMatcherKeyDefsServerTLS_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsServerTLS()

	expectedKeys := []string{
		"server.tls_cert_file",
		"server.tls_key_file",
		"server.tls_terminated_upstream",
		"server.trusted_proxies",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "server", def.Group)
			assert.Equal(t, domain.ApplyBootstrapOnly, def.ApplyBehavior)
			assert.False(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsServerTLS_AllAreImmutable(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsServerTLS()

	for _, def := range defs {
		assert.False(t, def.MutableAtRuntime,
			"TLS key %q must be immutable (bootstrap-only)", def.Key)
		assert.Equal(t, domain.ApplyBootstrapOnly, def.ApplyBehavior,
			"TLS key %q must be bootstrap-only", def.Key)
	}
}

func TestMatcherKeyDefsApp_LogLevelHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsApp()
	require.True(t, len(defs) >= 2)

	logLevelDef := defs[1]
	assert.Equal(t, "app.log_level", logLevelDef.Key)
	assert.NotNil(t, logLevelDef.Validator, "log_level must have a validator")
}

func TestMatcherKeyDefsApp_LogLevelHasComponentLogger(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsApp()
	require.True(t, len(defs) >= 2)

	logLevelDef := defs[1]
	assert.Equal(t, "logger", logLevelDef.Component)
}
