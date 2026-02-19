//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// E2EConfig holds configuration for e2e tests loaded from environment variables.
type E2EConfig struct {
	// Application
	AppBaseURL string

	// PostgreSQL
	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string

	// Redis
	RedisHost string

	// RabbitMQ
	RabbitMQHost      string
	RabbitMQPort      string
	RabbitMQUser      string
	RabbitMQPassword  string
	RabbitMQHealthURL string

	// Timeouts
	StackCheckTimeout time.Duration
	RequestTimeout    time.Duration
	PollInterval      time.Duration
	PollTimeout       time.Duration

	// Test isolation
	DefaultTenantID   string
	DefaultTenantSlug string
}

// LoadConfig loads e2e configuration from environment variables with sensible defaults.
func LoadConfig() *E2EConfig {
	return &E2EConfig{
		// Application
		AppBaseURL: getEnv("E2E_APP_URL", "http://localhost:4018"),

		// PostgreSQL
		PostgresHost:     getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:     getEnv("POSTGRES_PORT", "5432"),
		PostgresUser:     getEnv("POSTGRES_USER", "matcher"),
		PostgresPassword: getEnv("POSTGRES_PASSWORD", "matcher_dev_password"),
		PostgresDB:       getEnv("POSTGRES_DB", "matcher"),

		// Redis
		RedisHost: getEnv("REDIS_HOST", "localhost:6379"),

		// RabbitMQ
		RabbitMQHost:      getEnv("RABBITMQ_HOST", "localhost"),
		RabbitMQPort:      getEnv("RABBITMQ_PORT", "5672"),
		RabbitMQUser:      getEnv("RABBITMQ_USER", "matcher_admin"),
		RabbitMQPassword:  getEnv("RABBITMQ_PASSWORD", "matcher_dev_password"),
		RabbitMQHealthURL: getEnv("RABBITMQ_HEALTH_URL", "http://localhost:15672"),

		// Timeouts
		StackCheckTimeout: getDurationEnv("E2E_STACK_CHECK_TIMEOUT", 10*time.Second),
		RequestTimeout:    getDurationEnv("E2E_REQUEST_TIMEOUT", 30*time.Second),
		PollInterval:      getDurationEnv("E2E_POLL_INTERVAL", 500*time.Millisecond),
		PollTimeout:       getDurationEnv("E2E_POLL_TIMEOUT", 60*time.Second),

		// Test isolation - default tenant ID must match server's DEFAULT_TENANT_ID
		DefaultTenantID:   getEnv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111"),
		DefaultTenantSlug: getEnv("DEFAULT_TENANT_SLUG", "default"),
	}
}

// PostgresDSN returns a PostgreSQL connection string.
func (c *E2EConfig) PostgresDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.PostgresUser, c.PostgresPassword, c.PostgresHost, c.PostgresPort, c.PostgresDB)
}

// RabbitMQURL returns an AMQP connection URL.
func (c *E2EConfig) RabbitMQURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/",
		c.RabbitMQUser, c.RabbitMQPassword, c.RabbitMQHost, c.RabbitMQPort)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getDurationEnv(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if seconds, err := strconv.Atoi(val); err == nil {
			return time.Duration(seconds) * time.Second
		}
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
