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

func defsByKey(defs []domain.KeyDef) map[string]domain.KeyDef {
	result := make(map[string]domain.KeyDef, len(defs))
	for _, def := range defs {
		result[def.Key] = def
	}

	return result
}

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
			behavior:  domain.ApplyLiveRead,
			mutable:   true,
		},
	}

	defsMap := defsByKey(defs)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def, ok := defsMap[tt.key]
			require.True(t, ok)
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

	type expected struct {
		key      string
		group    string
		behavior domain.ApplyBehavior
		mutable  bool
	}

	expectedKeys := []expected{
		{key: "server.address", group: "server", behavior: domain.ApplyBootstrapOnly, mutable: false},
		{key: "server.body_limit_bytes", group: "server", behavior: domain.ApplyBootstrapOnly, mutable: false},
		{key: "cors.allowed_origins", group: "cors", behavior: domain.ApplyLiveRead, mutable: true},
		{key: "cors.allowed_methods", group: "cors", behavior: domain.ApplyLiveRead, mutable: true},
		{key: "cors.allowed_headers", group: "cors", behavior: domain.ApplyLiveRead, mutable: true},
	}

	require.Len(t, defs, len(expectedKeys))
	defsMap := defsByKey(defs)

	for _, exp := range expectedKeys {
		t.Run(exp.key, func(t *testing.T) {
			t.Parallel()

			def, ok := defsMap[exp.key]
			require.True(t, ok)
			assert.Equal(t, exp.key, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, exp.group, def.Group)
			assert.Equal(t, exp.behavior, def.ApplyBehavior)
			assert.Equal(t, exp.mutable, def.MutableAtRuntime)
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

	def, ok := defsByKey(defs)["server.address"]
	require.True(t, ok)
	assert.Equal(t, domain.ApplyBootstrapOnly, def.ApplyBehavior)
	assert.False(t, def.MutableAtRuntime)
}

func TestMatcherKeyDefsServerHTTP_BodyLimitHasValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsServerHTTP()
	require.NotEmpty(t, defs)

	bodyLimitDef, ok := defsByKey(defs)["server.body_limit_bytes"]
	require.True(t, ok)
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

	defsMap := defsByKey(defs)

	for _, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def, ok := defsMap[expKey]
			require.True(t, ok)
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

	logLevelDef, ok := defsByKey(defs)["app.log_level"]
	require.True(t, ok)
	assert.Equal(t, "app.log_level", logLevelDef.Key)
	assert.NotNil(t, logLevelDef.Validator, "log_level must have a validator")
}

func TestMatcherKeyDefsApp_LogLevelHasComponentNone(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsApp()
	require.True(t, len(defs) >= 2)

	logLevelDef, ok := defsByKey(defs)["app.log_level"]
	require.True(t, ok)
	assert.Equal(t, domain.ComponentNone, logLevelDef.Component)
}
