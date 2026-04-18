//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/LerianStudio/matcher/internal/auth"
	outboxServices "github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/matcher/tests/integration"
)

type serverHarnessBase struct {
	App              *fiber.App
	OutboxDispatcher *outboxServices.Dispatcher

	// t is the test context used for logging non-fatal diagnostics
	// (e.g. expected ack failures during teardown).
	t testing.TB

	// RabbitMQ consumer spy
	rabbitConn *amqp.Connection
	rabbitCh   *amqp.Channel
	spyQueue   string
	deliveries <-chan amqp.Delivery

	PostgresDSN       string
	RedisAddr         string
	RabbitMQHost      string
	RabbitMQPort      string
	RabbitMQHealthURL string
	Seed              integration.SeedData
}

// Do performs an HTTP request against the Fiber app.
func (sh *serverHarnessBase) Do(req *http.Request) (*http.Response, []byte, error) {
	resp, err := sh.App.Test(req, 30000) // 30 second timeout
	if err != nil {
		return nil, nil, fmt.Errorf("app.Test failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return resp, body, nil
}

// DoJSON performs an HTTP request with JSON body and tenant headers.
func (sh *serverHarnessBase) DoJSON(
	method, path string,
	payload any,
) (*http.Response, []byte, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal payload: %w", err)
		}
		body = bytes.NewReader(data)
	}

	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
		req.Header.Set("X-Idempotency-Key", uuid.New().String())
	}
	sh.setTenantHeaders(req)

	return sh.Do(req)
}

// DoMultipart performs a multipart file upload request.
func (sh *serverHarnessBase) DoMultipart(
	path string,
	fieldName, fileName string,
	fileContent []byte,
	formFields map[string]string,
) (*http.Response, []byte, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Add file
	part, err := writer.CreateFormFile(fieldName, fileName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err := part.Write(fileContent); err != nil {
		return nil, nil, fmt.Errorf("failed to write file content: %w", err)
	}

	// Add form fields
	for key, val := range formFields {
		if err := writer.WriteField(key, val); err != nil {
			return nil, nil, fmt.Errorf("failed to write field %s: %w", key, err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-Idempotency-Key", uuid.New().String())
	sh.setTenantHeaders(req)

	return sh.Do(req)
}

// DispatchOutboxOnce triggers a single dispatch cycle for pending outbox events.
func (sh *serverHarnessBase) DispatchOutboxOnce(ctx context.Context) int {
	if sh.OutboxDispatcher != nil {
		return sh.OutboxDispatcher.DispatchOnce(ctx)
	}
	return 0
}

// DispatchOutboxUntilEmpty dispatches outbox events until none remain pending.
func (sh *serverHarnessBase) DispatchOutboxUntilEmpty(ctx context.Context, maxIterations int) {
	for i := 0; i < maxIterations; i++ {
		processed := sh.DispatchOutboxOnce(ctx)
		if processed == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WaitForEvent waits for a RabbitMQ message matching the predicate.
func (sh *serverHarnessBase) WaitForEvent(
	ctx context.Context,
	match func(routingKey string, body []byte) bool,
) ([]byte, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case delivery, ok := <-sh.deliveries:
			if !ok {
				return nil, fmt.Errorf("delivery channel closed")
			}
			if match(delivery.RoutingKey, delivery.Body) {
				if err := delivery.Ack(false); err != nil {
					return nil, fmt.Errorf("failed to ack message: %w", err)
				}
				return delivery.Body, nil
			}
			// Non-matching message ack; queue is exclusive/auto-delete so error
			// is expected during teardown. Log rather than fail because callers
			// only care about the matched message.
			if ackErr := delivery.Ack(false); ackErr != nil {
				sh.t.Logf("ack non-matching event (routing_key=%s): %v", delivery.RoutingKey, ackErr)

				continue
			}
		}
	}
}

// WaitForEventWithTimeout waits for an event with a timeout.
func (sh *serverHarnessBase) WaitForEventWithTimeout(
	timeout time.Duration,
	match func(routingKey string, body []byte) bool,
) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return sh.WaitForEvent(ctx, match)
}

// ServerCtx returns a context with tenant ID set for server operations.
func (sh *serverHarnessBase) ServerCtx() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, auth.TenantIDKey, sh.Seed.TenantID.String())
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)
	return ctx
}

func (sh *serverHarnessBase) setEnvFromContainers(t *testing.T) error {
	t.Helper()

	// Parse PostgreSQL DSN
	pgURL, err := url.Parse(sh.PostgresDSN)
	if err != nil {
		return fmt.Errorf("failed to parse postgres DSN: %w", err)
	}

	pgHost, pgPort, _ := strings.Cut(pgURL.Host, ":")
	if pgPort == "" {
		pgPort = "5432"
	}
	pgUser := pgURL.User.Username()
	pgPass, _ := pgURL.User.Password()
	pgDB := strings.TrimPrefix(pgURL.Path, "/")

	// Parse Redis address
	redisURL, err := url.Parse(sh.RedisAddr)
	if err != nil {
		return fmt.Errorf("failed to parse redis address: %w", err)
	}
	redisHost := redisURL.Host

	// Set environment variables
	envVars := map[string]string{
		// Postgres
		"POSTGRES_HOST":     pgHost,
		"POSTGRES_PORT":     pgPort,
		"POSTGRES_USER":     pgUser,
		"POSTGRES_PASSWORD": pgPass,
		"POSTGRES_DB":       pgDB,
		"POSTGRES_SSLMODE":  "disable",

		// Non-empty value enables embedded migrations during bootstrap.
		"MIGRATIONS_PATH": "migrations",

		// Redis
		"REDIS_HOST":     redisHost,
		"REDIS_PASSWORD": "",
		"REDIS_TLS":      "false",

		// RabbitMQ
		"RABBITMQ_HOST":                        sh.RabbitMQHost,
		"RABBITMQ_PORT":                        sh.RabbitMQPort,
		"RABBITMQ_USER":                        "guest",
		"RABBITMQ_PASSWORD":                    "guest",
		"RABBITMQ_VHOST":                       "/",
		"RABBITMQ_URI":                         "amqp",
		"RABBITMQ_HEALTH_URL":                  sh.RabbitMQHealthURL,
		"RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK": "true",

		// Auth disabled for tests
		"AUTH_ENABLED":        "false",
		"PLUGIN_AUTH_ENABLED": "false",

		// Body limit: 110 MiB / 115343360 bytes to allow 100MB test files
		// through Fiber, while the application handler enforces a 100MB limit
		"HTTP_BODY_LIMIT_BYTES": "115343360",

		// Tenant defaults
		"DEFAULT_TENANT_ID":   sh.Seed.TenantID.String(),
		"DEFAULT_TENANT_SLUG": "default",

		// Disable telemetry for tests
		"ENABLE_TELEMETRY": "false",

		// Use development environment
		"ENV_NAME": "development",

		// Logging
		"LOG_LEVEL": "debug",

		// Rate limiting configured with high limits for tests
		"RATE_LIMIT_MAX":                 "10000",
		"RATE_LIMIT_EXPIRY_SEC":          "60",
		"EXPORT_RATE_LIMIT_MAX":          "1000",
		"EXPORT_RATE_LIMIT_EXPIRY_SEC":   "300",
		"DISPATCH_RATE_LIMIT_MAX":        "100",
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC": "60",

		// Connection pool limits (reduced for tests to prevent exhaustion)
		"POSTGRES_MAX_OPEN_CONNS": "5",
		"POSTGRES_MAX_IDLE_CONNS": "2",

		// Infrastructure connection timeout
		"INFRA_CONNECT_TIMEOUT_SEC": "30",

		// Disable background workers not needed in tests
		"ARCHIVAL_WORKER_ENABLED": "false",

		// Systemplane secret master key (well-known dev default)
		"SYSTEMPLANE_SECRET_MASTER_KEY": "+PnwgNy8bL3HGT1rOXp47PqyGcPywXH/epgmSVwPkL0=",
	}

	for key, val := range envVars {
		t.Setenv(key, val)
	}

	return nil
}

func (sh *serverHarnessBase) setTenantHeaders(req *http.Request) {
	req.Header.Set("X-Tenant-ID", sh.Seed.TenantID.String())
}

func (sh *serverHarnessBase) setupRabbitSpy(t *testing.T) error {
	t.Helper()

	amqpURL := fmt.Sprintf("amqp://guest:guest@%s:%s/", sh.RabbitMQHost, sh.RabbitMQPort)

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}
	sh.rabbitConn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	sh.rabbitCh = ch

	// Declare exchange (matching production)
	if err := ch.ExchangeDeclare(ExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Create exclusive auto-delete queue for this test
	queueName := fmt.Sprintf("test-spy-%s", uuid.New().String()[:8])
	q, err := ch.QueueDeclare(queueName, false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare spy queue: %w", err)
	}
	sh.spyQueue = q.Name

	// Bind to all relevant routing keys
	routingKeys := []string{
		RoutingKeyIngestionCompleted,
		RoutingKeyIngestionFailed,
		RoutingKeyMatchConfirmed,
	}
	for _, key := range routingKeys {
		if err := ch.QueueBind(q.Name, key, ExchangeName, false, nil); err != nil {
			return fmt.Errorf("failed to bind queue to %s: %w", key, err)
		}
	}

	// Start consuming
	deliveries, err := ch.Consume(q.Name, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}
	sh.deliveries = deliveries

	return nil
}

// RabbitMQConnection returns a lib-commons compatible RabbitMQ connection.
func (sh *serverHarnessBase) RabbitMQConnection() *libRabbitmq.RabbitMQConnection {
	return &libRabbitmq.RabbitMQConnection{
		ConnectionStringSource: fmt.Sprintf(
			"amqp://guest:guest@%s:%s/",
			sh.RabbitMQHost,
			sh.RabbitMQPort,
		),
		HealthCheckURL:           sh.RabbitMQHealthURL,
		Host:                     sh.RabbitMQHost,
		Port:                     sh.RabbitMQPort,
		User:                     "guest",
		Pass:                     "guest",
		AllowInsecureHealthCheck: true,
	}
}
