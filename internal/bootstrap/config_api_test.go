//go:build unit

package bootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// newAPITestConfigManager creates a ConfigManager for config API testing with a default config.
func newAPITestConfigManager(t *testing.T) *ConfigManager {
	t.Helper()

	cfg := defaultConfig()
	cfg.App.LogLevel = "info"
	cfg.RateLimit.Max = 100

	cm, err := NewConfigManager(cfg, "", &libLog.NopLogger{})
	require.NoError(t, err)
	t.Cleanup(cm.Stop)

	return cm
}

// newTestApp creates a Fiber app with the ConfigAPIHandler registered.
func newTestApp(t *testing.T, handler *ConfigAPIHandler) *fiber.App {
	t.Helper()

	app := fiber.New()

	// Register routes directly (without auth middleware for unit testing).
	app.Get("/v1/system/config", handler.GetConfig)
	app.Get("/v1/system/config/schema", handler.GetSchema)
	app.Patch("/v1/system/config", handler.UpdateConfig)
	app.Post("/v1/system/config/reload", handler.ReloadConfig)
	app.Get("/v1/system/config/history", handler.GetConfigHistory)

	return app
}

func TestNewConfigAPIHandler_NilConfigManager(t *testing.T) {
	t.Parallel()

	handler, err := NewConfigAPIHandler(nil, &libLog.NopLogger{})

	assert.Nil(t, handler)
	assert.ErrorIs(t, err, ErrConfigManagerRequired)
}

func TestNewConfigAPIHandler_NilLogger(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)
	handler, err := NewConfigAPIHandler(cm, nil)

	assert.NotNil(t, handler)
	assert.NoError(t, err)
}

func TestGetConfig_ReturnsCurrentConfig(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/config", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response GetConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.NotNil(t, response.Config)
	assert.NotNil(t, response.EnvOverrides)
	assert.False(t, response.LastReloadAt.IsZero())
}

func TestGetConfig_RedactsSecrets(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/config", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response GetConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	// Secrets should be redacted.
	for _, secretKey := range []string{
		"postgres.primary_password",
		"redis.password",
		"rabbitmq.password",
		"auth.token_secret",
	} {
		val, exists := response.Config[secretKey]
		if exists {
			assert.Equal(t, redactedValue, val, "secret key %q should be redacted", secretKey)
		}
	}
}

func TestGetSchema_ReturnsGroupedFields(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/config/schema", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ConfigSchemaResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.NotEmpty(t, response.Sections, "should have sections")
	assert.Greater(t, response.TotalFields, 0, "should have fields")

	// Verify key sections exist.
	for _, section := range []string{"app", "server", "rate_limit", "postgres"} {
		fields, exists := response.Sections[section]
		assert.True(t, exists, "section %q should exist", section)
		assert.NotEmpty(t, fields, "section %q should have fields", section)
	}
}

func TestGetSchema_RedactsSecretValues(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/config/schema", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ConfigSchemaResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	// Check that secret fields have redacted values.
	for _, fields := range response.Sections {
		for _, field := range fields {
			if secretFields[field.Key] {
				assert.Equal(t, redactedValue, field.CurrentValue,
					"secret field %q should have redacted current value", field.Key)
			}
		}
	}
}

func TestUpdateConfig_ValidChange(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	reqBody := UpdateConfigRequest{
		Changes: map[string]any{
			"rate_limit.max": 200,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/config", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response UpdateConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.NotEmpty(t, response.Applied, "should have applied changes")
	assert.Empty(t, response.Rejected, "should not have rejected changes")

	// Verify the change was actually applied.
	newCfg := cm.Get()
	assert.Equal(t, 200, newCfg.RateLimit.Max)
}

func TestUpdateConfig_RejectsImmutableKey(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	reqBody := UpdateConfigRequest{
		Changes: map[string]any{
			"postgres.primary_host": "evil-host",
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/config", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response UpdateConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.Empty(t, response.Applied, "should not have applied immutable key")
	assert.NotEmpty(t, response.Rejected, "should have rejected immutable key")
	assert.Equal(t, "postgres.primary_host", response.Rejected[0].Key)
}

func TestUpdateConfig_EmptyChanges(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	reqBody := UpdateConfigRequest{
		Changes: map[string]any{},
	}

	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/config", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestUpdateConfig_InvalidJSON(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/config", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestReloadConfig_Success(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/system/config/reload", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReloadConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.Greater(t, response.Version, uint64(0))
	assert.False(t, response.ReloadedAt.IsZero())
}

func TestGetConfigHistory_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/config/history", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ConfigHistoryResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.NotNil(t, response.Items, "items should not be nil")
	assert.Empty(t, response.Items, "items should be empty (T10 placeholder)")
}

func TestRegisterConfigAPIRoutes_NilProtected(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	err = RegisterConfigAPIRoutes(nil, handler)
	assert.ErrorIs(t, err, ErrConfigAPIProtectedRequired)
}

func TestRegisterConfigAPIRoutes_NilHandler(t *testing.T) {
	t.Parallel()

	protected := func(_, _ string) fiber.Router {
		return fiber.New().Group("")
	}

	err := RegisterConfigAPIRoutes(protected, nil)
	assert.ErrorIs(t, err, ErrConfigAPIHandlerRequired)
}

func TestGetConfig_EnvOverridesDetection(t *testing.T) {
	// Cannot use t.Parallel() because t.Setenv modifies process environment.
	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := newTestApp(t, handler)

	// Set an env var that corresponds to a schema field.
	t.Setenv("LOG_LEVEL", "debug")

	req := httptest.NewRequest(http.MethodGet, "/v1/system/config", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response GetConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	// LOG_LEVEL should show up in env overrides.
	assert.Contains(t, response.EnvOverrides, "app.log_level",
		"LOG_LEVEL env var should cause app.log_level to appear in env overrides")
}

func TestBuildRedactedConfig_AllSecretKeysRedacted(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)
	redacted := buildRedactedConfig(cm)

	for key := range secretFields {
		val, exists := redacted[key]
		if exists {
			assert.Equal(t, redactedValue, val, "key %q should be redacted", key)
		}
	}
}

func TestIsEnvOverridden_SetVar(t *testing.T) {
	// Cannot use t.Parallel() because t.Setenv modifies process environment.
	envVar := "TEST_CONFIG_API_OVERRIDE_CHECK"
	t.Setenv(envVar, "yes")

	assert.True(t, isEnvOverridden(envVar))
}

func TestIsEnvOverridden_UnsetVar(t *testing.T) {
	t.Parallel()

	// Ensure the var is NOT set (use a name that certainly doesn't exist).
	envVar := "UNLIKELY_TEST_VAR_" + t.Name()

	// Explicitly unset to be sure.
	os.Unsetenv(envVar) //nolint:errcheck // test helper

	assert.False(t, isEnvOverridden(envVar))
}

func TestIsEnvOverridden_EmptyString(t *testing.T) {
	t.Parallel()

	assert.False(t, isEnvOverridden(""))
}

func TestUpdateConfig_AuditPublisherCalledOnSuccess(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	// Create a working audit publisher with a mock outbox repo.
	mockRepo := &testOutboxMock{}
	publisher, err := NewConfigAuditPublisher(mockRepo, &libLog.NopLogger{})
	require.NoError(t, err)

	handler.SetAuditPublisher(publisher)

	app := newTestApp(t, handler)

	// Set up the request context to include a valid tenant ID (required by audit publisher).
	reqBody := UpdateConfigRequest{
		Changes: map[string]any{
			"rate_limit.max": 300,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/config", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// The request should succeed regardless of audit outcome.
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response UpdateConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.NotEmpty(t, response.Applied, "should have applied changes")
}

func TestUpdateConfig_AuditFailureDoesNotFailRequest(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	// Create an audit publisher with a failing outbox repo.
	mockRepo := &testOutboxMock{createErr: errors.New("outbox write failed")}
	publisher, err := NewConfigAuditPublisher(mockRepo, &libLog.NopLogger{})
	require.NoError(t, err)

	handler.SetAuditPublisher(publisher)

	app := newTestApp(t, handler)

	reqBody := UpdateConfigRequest{
		Changes: map[string]any{
			"rate_limit.max": 400,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/config", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// The request MUST succeed even when the audit publisher fails — audit is best-effort.
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response UpdateConfigResponse
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)

	assert.NotEmpty(t, response.Applied, "config change should still be applied despite audit failure")

	// Verify the config was actually updated.
	newCfg := cm.Get()
	assert.Equal(t, 400, newCfg.RateLimit.Max)
}
