// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests in this file complement coverage from fiber_server_test.go by testing
// health check helpers in isolation with table-driven patterns.

func TestNewHealthDependencies_AllNil(t *testing.T) {
	t.Parallel()

	deps := NewHealthDependencies(nil, nil, nil, nil, nil)

	assert.NotNil(t, deps)
	assert.Nil(t, deps.Postgres)
	assert.Nil(t, deps.PostgresReplica)
	assert.Nil(t, deps.Redis)
	assert.Nil(t, deps.RabbitMQ)
	assert.Nil(t, deps.ObjectStorage)

	// Verify defaults: Redis, replica, and object storage are optional.
	assert.True(t, deps.RedisOptional)
	assert.True(t, deps.PostgresReplicaOptional)
	assert.True(t, deps.ObjectStorageOptional)

	// Postgres and RabbitMQ are NOT optional by default.
	assert.False(t, deps.PostgresOptional)
	assert.False(t, deps.RabbitMQOptional)
}

func TestChecksToStringTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		checks   fiber.Map
		key      string
		expected string
	}{
		{
			name:     "nil_checks_returns_unknown",
			checks:   nil,
			key:      "database",
			expected: statusUnknown,
		},
		{
			name:     "missing_key_returns_unknown",
			checks:   fiber.Map{"redis": "ok"},
			key:      "database",
			expected: statusUnknown,
		},
		{
			name:     "non_string_value_returns_unknown",
			checks:   fiber.Map{"database": 42},
			key:      "database",
			expected: statusUnknown,
		},
		{
			name:     "returns_ok_value",
			checks:   fiber.Map{"database": "ok"},
			key:      "database",
			expected: "ok",
		},
		{
			name:     "returns_down_value",
			checks:   fiber.Map{"redis": "down"},
			key:      "redis",
			expected: "down",
		},
		{
			name:     "empty_string_key",
			checks:   fiber.Map{"": "ok"},
			key:      "",
			expected: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := checksToString(tt.checks, tt.key, nil)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldIncludeReadinessDetailsTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      *Config
		expected bool
	}{
		{
			name:     "nil_config_returns_false",
			cfg:      nil,
			expected: false,
		},
		{
			name:     "production_returns_false",
			cfg:      &Config{App: AppConfig{EnvName: "production"}},
			expected: false,
		},
		{
			name:     "development_returns_true",
			cfg:      &Config{App: AppConfig{EnvName: "development"}},
			expected: true,
		},
		{
			name:     "staging_returns_false",
			cfg:      &Config{App: AppConfig{EnvName: "staging"}},
			expected: false,
		},
		{
			name:     "test_returns_true",
			cfg:      &Config{App: AppConfig{EnvName: "test"}},
			expected: true,
		},
		{
			name:     "empty_env_returns_false",
			cfg:      &Config{App: AppConfig{EnvName: ""}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, shouldIncludeReadinessDetails(tt.cfg))
		})
	}
}

func TestReadinessHandler_UsesRuntimeConfigGetter(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{App: AppConfig{EnvName: "development"}}
	runtimeCfg := &Config{App: AppConfig{EnvName: "production"}}

	app := fiber.New()
	app.Get("/ready", readinessHandler(initialCfg, func() *Config { return runtimeCfg }, nil, nil, nil))

	req := httptest.NewRequest(http.MethodGet, "/ready", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.Nil(t, response.Checks)
}

func TestReadinessHandler_UsesRuntimeTimeoutFromConfigGetter(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{App: AppConfig{EnvName: "development"}, Infrastructure: InfrastructureConfig{HealthCheckTimeoutSec: 5}}
	runtimeCfg := &Config{App: AppConfig{EnvName: "development"}, Infrastructure: InfrastructureConfig{HealthCheckTimeoutSec: 1}}
	deps := &HealthDependencies{
		PostgresCheck: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	app := fiber.New()
	app.Get("/ready", readinessHandler(initialCfg, func() *Config { return runtimeCfg }, nil, deps, nil))

	started := time.Now()
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/ready", http.NoBody), 2000)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Less(t, time.Since(started), 2*time.Second)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.Equal(t, "degraded", response.Status)
	require.NotNil(t, response.Checks)
	assert.Equal(t, "down", response.Checks.Database)
}

func TestReadinessHandler_WhenDraining_ReturnsServiceUnavailable(t *testing.T) {
	t.Parallel()

	state := &readinessState{}
	state.beginDraining()

	app := fiber.New()
	app.Get("/ready", readinessHandler(&Config{App: AppConfig{EnvName: "development"}}, nil, state.isDraining, nil, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.Equal(t, "degraded", response.Status)
	assert.Nil(t, response.Checks)
}
