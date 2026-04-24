//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_Defaults(t *testing.T) {
	unsetEnvVars := []string{
		"E2E_APP_URL",
		"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
		"REDIS_HOST",
		"RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_USER", "RABBITMQ_PASSWORD", "RABBITMQ_HEALTH_URL", "RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK",
		"E2E_STACK_CHECK_TIMEOUT", "E2E_REQUEST_TIMEOUT", "E2E_POLL_INTERVAL", "E2E_POLL_TIMEOUT",
		"DEFAULT_TENANT_ID", "DEFAULT_TENANT_SLUG",
	}

	// Clear each env var for the scope of this test. Consumers treat empty
	// string as "unset" (getEnv / getDurationEnv / getBoolEnv all gate on
	// val != ""). t.Setenv restores the prior value automatically when the
	// test ends.
	for _, key := range unsetEnvVars {
		t.Setenv(key, "")
	}

	cfg := LoadConfig()

	require.NotNil(t, cfg)
	assert.Equal(t, "http://localhost:4018", cfg.AppBaseURL)
	assert.Equal(t, "localhost", cfg.PostgresHost)
	assert.Equal(t, "5432", cfg.PostgresPort)
	assert.Equal(t, "matcher", cfg.PostgresUser)
	assert.Equal(t, "matcher_dev_password", cfg.PostgresPassword)
	assert.Equal(t, "matcher", cfg.PostgresDB)
	assert.Equal(t, "localhost:6379", cfg.RedisHost)
	assert.Equal(t, "localhost", cfg.RabbitMQHost)
	assert.Equal(t, "5672", cfg.RabbitMQPort)
	assert.Equal(t, "matcher_admin", cfg.RabbitMQUser)
	assert.Equal(t, "matcher_dev_password", cfg.RabbitMQPassword)
	assert.Equal(t, "http://localhost:15672", cfg.RabbitMQHealthURL)
	assert.False(t, cfg.RabbitMQAllowInsecureHealthCheck)
	assert.Equal(t, 10*time.Second, cfg.StackCheckTimeout)
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout)
	assert.Equal(t, 500*time.Millisecond, cfg.PollInterval)
	assert.Equal(t, 60*time.Second, cfg.PollTimeout)
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", cfg.DefaultTenantID)
	assert.Equal(t, "default", cfg.DefaultTenantSlug)
}

func TestLoadConfig_FromEnvironment(t *testing.T) {
	envVars := map[string]string{
		"E2E_APP_URL":       "http://custom:9090",
		"POSTGRES_HOST":     "custom-postgres",
		"POSTGRES_PORT":     "5433",
		"REDIS_HOST":        "custom-redis:6380",
		"DEFAULT_TENANT_ID": "custom-tenant-id",
	}

	for key, val := range envVars {
		t.Setenv(key, val)
	}

	cfg := LoadConfig()

	require.NotNil(t, cfg)
	assert.Equal(t, "http://custom:9090", cfg.AppBaseURL)
	assert.Equal(t, "custom-postgres", cfg.PostgresHost)
	assert.Equal(t, "5433", cfg.PostgresPort)
	assert.Equal(t, "custom-redis:6380", cfg.RedisHost)
	assert.Equal(t, "custom-tenant-id", cfg.DefaultTenantID)
}

func TestGetEnv_ReturnsDefaultWhenEmpty(t *testing.T) {
	t.Setenv("TEST_EMPTY_ENV_VAR", "")

	result := getEnv("TEST_EMPTY_ENV_VAR", "default-value")
	assert.Equal(t, "default-value", result)
}

func TestGetEnv_ReturnsValueWhenSet(t *testing.T) {
	t.Setenv("TEST_SET_ENV_VAR", "actual-value")

	result := getEnv("TEST_SET_ENV_VAR", "default-value")
	assert.Equal(t, "actual-value", result)
}

func TestGetDurationEnv_ReturnsDefaultWhenEmpty(t *testing.T) {
	t.Setenv("TEST_DURATION_EMPTY", "")

	result := getDurationEnv("TEST_DURATION_EMPTY", 5*time.Second)
	assert.Equal(t, 5*time.Second, result)
}

func TestGetDurationEnv_ParsesIntAsSeconds(t *testing.T) {
	t.Setenv("TEST_DURATION_INT", "30")

	result := getDurationEnv("TEST_DURATION_INT", 5*time.Second)
	assert.Equal(t, 30*time.Second, result)
}

func TestGetDurationEnv_ParsesDurationString(t *testing.T) {
	t.Setenv("TEST_DURATION_STRING", "2m30s")

	result := getDurationEnv("TEST_DURATION_STRING", 5*time.Second)
	assert.Equal(t, 2*time.Minute+30*time.Second, result)
}

func TestGetDurationEnv_ReturnsDefaultOnInvalidValue(t *testing.T) {
	t.Setenv("TEST_DURATION_INVALID", "not-a-duration")

	result := getDurationEnv("TEST_DURATION_INVALID", 5*time.Second)
	assert.Equal(t, 5*time.Second, result)
}

func TestGetBoolEnv_ReturnsDefaultWhenEmpty(t *testing.T) {
	t.Setenv("TEST_BOOL_EMPTY", "")

	result := getBoolEnv("TEST_BOOL_EMPTY", true)

	assert.True(t, result)
}

func TestGetBoolEnv_ParsesBoolean(t *testing.T) {
	t.Setenv("TEST_BOOL_SET", "false")

	result := getBoolEnv("TEST_BOOL_SET", true)

	assert.False(t, result)
}

func TestGetBoolEnv_ReturnsDefaultOnInvalidValue(t *testing.T) {
	t.Setenv("TEST_BOOL_INVALID", "not-bool")

	result := getBoolEnv("TEST_BOOL_INVALID", true)

	assert.True(t, result)
}

func TestE2EConfig_PostgresDSN(t *testing.T) {
	cfg := &E2EConfig{
		PostgresHost:     "dbhost",
		PostgresPort:     "5432",
		PostgresUser:     "dbuser",
		PostgresPassword: "dbpass",
		PostgresDB:       "dbname",
	}

	dsn := cfg.PostgresDSN()
	assert.Equal(t, "postgres://dbuser:dbpass@dbhost:5432/dbname?sslmode=disable", dsn)
}

func TestE2EConfig_RabbitMQURL(t *testing.T) {
	cfg := &E2EConfig{
		RabbitMQHost:     "mqhost",
		RabbitMQPort:     "5672",
		RabbitMQUser:     "mquser",
		RabbitMQPassword: "mqpass",
	}

	url := cfg.RabbitMQURL()
	assert.Equal(t, "amqp://mquser:mqpass@mqhost:5672/", url)
}
