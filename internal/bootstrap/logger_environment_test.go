// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	libZap "github.com/LerianStudio/lib-commons/v5/commons/zap"
	"github.com/stretchr/testify/assert"
)

func TestResolveLoggerEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		envName  string
		expected libZap.Environment
	}{
		{name: "production", envName: "production", expected: libZap.EnvironmentProduction},
		{name: "production mixed case", envName: "PrOdUcTiOn", expected: libZap.EnvironmentProduction},
		{name: "staging", envName: "staging", expected: libZap.EnvironmentStaging},
		{name: "unknown defaults to development", envName: "sandbox", expected: libZap.EnvironmentDevelopment},
		{name: "empty defaults to development", envName: "", expected: libZap.EnvironmentDevelopment},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := ResolveLoggerEnvironment(tt.envName)

			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestResolveLoggerLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{name: "debug", value: "debug", expected: "debug"},
		{name: "uppercase normalized", value: "WARN", expected: "warn"},
		{name: "whitespace trimmed", value: "  error ", expected: "error"},
		{name: "invalid falls back", value: "verbose", expected: defaultLoggerLevel},
		{name: "empty falls back", value: "", expected: defaultLoggerLevel},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, ResolveLoggerLevel(tt.value))
		})
	}
}

func TestIsProductionEnvironment(t *testing.T) {
	t.Parallel()

	assert.True(t, IsProductionEnvironment("production"))
	assert.True(t, IsProductionEnvironment(" PrOdUcTiOn "))
	assert.False(t, IsProductionEnvironment("staging"))
}

func TestDeploymentMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     string
		expected string
	}{
		{name: "saas", mode: "saas", expected: deploymentModeSaaS},
		{name: "byoc", mode: "byoc", expected: deploymentModeByoc},
		{name: "local", mode: "local", expected: deploymentModeLocal},
		{name: "mixed case", mode: "  SaAs  ", expected: deploymentModeSaaS},
		{name: "empty defaults to local", mode: "", expected: deploymentModeLocal},
		{name: "unknown defaults to local", mode: "dev", expected: deploymentModeLocal},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := AppConfig{Mode: tt.mode}
			assert.Equal(t, tt.expected, cfg.DeploymentMode())
		})
	}
}

// TestIsDevelopmentOrTestEnvironment exhaustively covers the helper that
// gates dev/test-only behavior. The function trims whitespace and lowercases
// the input, matching exactly "development" or "test" — every other value
// (including "dev", empty string, staging, production, UAT) must return false
// so the caller treats them as production-adjacent.
func TestIsDevelopmentOrTestEnvironment(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		envName string
		want    bool
	}{
		// Positive — explicit development/test names.
		{name: "development_lowercase_returns_true", envName: "development", want: true},
		{name: "test_lowercase_returns_true", envName: "test", want: true},

		// Case-insensitive normalization.
		{name: "Development_titlecase_returns_true", envName: "Development", want: true},
		{name: "DEVELOPMENT_uppercase_returns_true", envName: "DEVELOPMENT", want: true},
		{name: "Test_titlecase_returns_true", envName: "Test", want: true},
		{name: "TEST_uppercase_returns_true", envName: "TEST", want: true},
		{name: "TeSt_mixedcase_returns_true", envName: "TeSt", want: true},

		// Whitespace normalization.
		{name: "development_with_spaces_returns_true", envName: "  development  ", want: true},
		{name: "test_with_tabs_newlines_returns_true", envName: "\tTeSt\n", want: true},
		{name: "Development_with_trailing_space_returns_true", envName: "Development ", want: true},

		// Negative — "dev" is NOT equivalent to "development" here.
		{name: "dev_returns_false", envName: "dev", want: false},
		{name: "DEV_returns_false", envName: "DEV", want: false},
		{name: "dev_with_spaces_returns_false", envName: "  dev  ", want: false},

		// Negative — empty string is NOT considered dev (contrast with
		// isLocalDevelopmentEnvironment, which is scoped to HTTP permissiveness).
		{name: "empty_string_returns_false", envName: "", want: false},
		{name: "whitespace_only_returns_false", envName: "   ", want: false},

		// Negative — production / production-adjacent environments.
		{name: "production_returns_false", envName: "production", want: false},
		{name: "Production_returns_false", envName: "Production", want: false},
		{name: "staging_returns_false", envName: "staging", want: false},
		{name: "qa_returns_false", envName: "qa", want: false},
		{name: "uat_returns_false", envName: "uat", want: false},
		{name: "preview_returns_false", envName: "preview", want: false},
		{name: "prod_returns_false", envName: "prod", want: false},

		// Negative — adjacent / substring / typo values.
		{name: "testing_returns_false", envName: "testing", want: false},
		{name: "developmenting_returns_false", envName: "developmenting", want: false},
		{name: "random_value_returns_false", envName: "unknown-env", want: false},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := IsDevelopmentOrTestEnvironment(tc.envName)

			assert.Equal(t, tc.want, got,
				"IsDevelopmentOrTestEnvironment(%q) = %v, want %v", tc.envName, got, tc.want)
		})
	}
}
