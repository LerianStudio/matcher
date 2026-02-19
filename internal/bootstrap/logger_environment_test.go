//go:build unit

package bootstrap

import (
	"testing"

	libZap "github.com/LerianStudio/lib-uncommons/v2/uncommons/zap"
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
