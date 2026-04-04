//go:build unit

package connectors_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/adapters/http/connectors"
	"github.com/LerianStudio/matcher/internal/exception/domain/services"
)

func TestHTTPConnector_DispatchToJira_Success(t *testing.T) {
	t.Parallel()

	var receivedMethod string

	var receivedPath string

	var receivedContentType string

	var receivedAuth string

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			receivedMethod = request.Method
			receivedPath = request.URL.Path
			receivedContentType = request.Header.Get("Content-Type")
			receivedAuth = request.Header.Get("Authorization")

			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusCreated)

			resp := map[string]string{"key": "PROJ-123"}
			_ = json.NewEncoder(writer).Encode(resp)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond

	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Jira: &connectors.JiraConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			BaseURL:    server.URL,
			AuthToken:  "test-token",
			ProjectKey: "PROJ",
			IssueType:  "Bug",
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target:   services.RoutingTargetJira,
		Queue:    "support",
		RuleName: "test-rule",
	}

	payload := []byte(
		`{"fields":{"project":{"key":"PROJ"},"summary":"Test","issuetype":{"name":"Bug"}}}`,
	)

	result, err := connector.Dispatch(context.Background(), "exc-123", decision, payload)

	require.NoError(t, err)
	require.Equal(t, services.RoutingTargetJira, result.Target)
	require.Equal(t, "PROJ-123", result.ExternalReference)
	require.True(t, result.Acknowledged)

	require.Equal(t, http.MethodPost, receivedMethod)
	require.Equal(t, "/rest/api/2/issue", receivedPath)
	require.Equal(t, "application/json", receivedContentType)
	require.Equal(t, "Bearer test-token", receivedAuth)
}

func TestHTTPConnector_Retry_On500ThenSuccess(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			count := attempts.Add(1)

			if count == 1 {
				writer.WriteHeader(http.StatusInternalServerError)
				_, _ = writer.Write([]byte("temporary error"))

				return
			}

			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusCreated)

			resp := map[string]string{"key": "PROJ-456"}
			_ = json.NewEncoder(writer).Encode(resp)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 3
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Jira: &connectors.JiraConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			BaseURL:    server.URL,
			AuthToken:  "test-token",
			ProjectKey: "PROJ",
			IssueType:  "Bug",
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetJira,
	}
	result, err := connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.NoError(t, err)
	require.Equal(t, "PROJ-456", result.ExternalReference)
	require.True(t, result.Acknowledged)
	require.Equal(t, int32(2), attempts.Load())
}

func TestHTTPConnector_NoRetry_On400(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte("bad request"))
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 3
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Jira: &connectors.JiraConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			BaseURL:    server.URL,
			AuthToken:  "test-token",
			ProjectKey: "PROJ",
			IssueType:  "Bug",
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetJira,
	}
	_, err = connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrNonRetryableStatus)
	require.Equal(t, int32(1), attempts.Load())
}

func TestHTTPConnector_Webhook_WithSignature(t *testing.T) {
	t.Parallel()

	var receivedSignature string

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			receivedSignature = request.Header.Get("X-Signature-256")

			writer.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL:          server.URL,
			SharedSecret: "my-secret",
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	result, err := connector.Dispatch(
		context.Background(),
		"exc-123",
		decision,
		[]byte(`{"test":"data"}`),
	)

	require.NoError(t, err)
	require.Equal(t, services.RoutingTargetWebhook, result.Target)
	require.True(t, result.Acknowledged)
	require.NotEmpty(t, receivedSignature)
	require.Contains(t, receivedSignature, "sha256=")
}

func TestHTTPConnector_WebhookTimeoutResolver_OverridesBaseTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			time.Sleep(20 * time.Millisecond)
			writer.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	baseTimeout := 5 * time.Millisecond
	maxRetries := 1
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true,
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &baseTimeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)
	connector.SetWebhookTimeoutResolver(func(context.Context) time.Duration { return 100 * time.Millisecond })

	decision := services.RoutingDecision{Target: services.RoutingTargetWebhook}
	result, err := connector.Dispatch(context.Background(), "exc-timeout-override", decision, []byte(`{"test":true}`))
	require.NoError(t, err)
	require.True(t, result.Acknowledged)
}

func TestHTTPConnector_Manual_NoDispatch(t *testing.T) {
	t.Parallel()

	config := connectors.ConnectorConfig{}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetManual,
	}

	result, err := connector.Dispatch(context.Background(), "exc-123", decision, nil)

	require.NoError(t, err)
	require.Equal(t, services.RoutingTargetManual, result.Target)
	require.True(t, result.Acknowledged)
	require.Empty(t, result.ExternalReference)
}

func TestHTTPConnector_ServiceNow_Unsupported(t *testing.T) {
	t.Parallel()

	config := connectors.ConnectorConfig{}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetServiceNow,
	}

	_, err = connector.Dispatch(context.Background(), "exc-123", decision, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrUnsupportedTarget)
}

func TestHTTPConnector_JiraNotConfigured(t *testing.T) {
	t.Parallel()

	config := connectors.ConnectorConfig{}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetJira,
	}

	_, err = connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrConnectorNotConfigured)
}

func TestHTTPConnector_WebhookNotConfigured(t *testing.T) {
	t.Parallel()

	config := connectors.ConnectorConfig{}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	_, err = connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrConnectorNotConfigured)
}

func TestHTTPConnector_RetryStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	var once sync.Once
	var requestCount atomic.Int32

	requestSeen := make(chan struct{})

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			requestCount.Add(1)
			writer.WriteHeader(http.StatusServiceUnavailable)
			once.Do(func() {
				close(requestSeen)
			})
		}),
	)
	defer server.Close()

	maxRetries := 3
	retryBackoff := 200 * time.Millisecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				MaxRetries:   &maxRetries,
				RetryBackoff: &retryBackoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-requestSeen
		cancel()
	}()

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	_, err = connector.Dispatch(ctx, "exc-123", decision, []byte(`{"test":"cancel"}`))
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), requestCount.Load(), "dispatch should stop while waiting retry backoff")
}

func TestHTTPConnector_TimeoutHonored(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			time.Sleep(50 * time.Millisecond)
			writer.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Millisecond
	maxRetries := 1
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:    &timeout,
				MaxRetries: &maxRetries,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	_, err = connector.Dispatch(
		context.Background(),
		"exc-123",
		decision,
		[]byte(`{"test":"timeout"}`),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestHTTPConnector_IdempotencyKeyHeader(t *testing.T) {
	t.Parallel()

	var receivedIdempotencyKey string

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			receivedIdempotencyKey = request.Header.Get("X-Idempotency-Key")

			writer.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
		Queue:  "exception-queue",
	}

	_, err = connector.Dispatch(
		context.Background(),
		"exc-456",
		decision,
		[]byte(`{"test":"data"}`),
	)
	require.NoError(t, err)

	require.NotEmpty(t, receivedIdempotencyKey)
	// Key format: dispatch:{target}:{exceptionID}[:{queue}]
	require.Equal(t, "dispatch:WEBHOOK:exc-456:exception-queue", receivedIdempotencyKey)
}

func TestHTTPConnector_IdempotencyKeyHeader_Jira(t *testing.T) {
	t.Parallel()

	var receivedIdempotencyKey string

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			receivedIdempotencyKey = request.Header.Get("X-Idempotency-Key")

			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusCreated)
			writer.Write([]byte(`{"key":"PROJ-123"}`))
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Jira: &connectors.JiraConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			BaseURL:   server.URL,
			AuthToken: "test-token",
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetJira,
		Queue:  "jira-queue",
	}

	_, err = connector.Dispatch(context.Background(), "exc-789", decision, []byte(`{}`))
	require.NoError(t, err)

	require.NotEmpty(t, receivedIdempotencyKey)
	// Key format: dispatch:{target}:{exceptionID}[:{queue}]
	require.Equal(t, "dispatch:JIRA:exc-789:jira-queue", receivedIdempotencyKey)
}

func TestHTTPConnector_IdempotencyKey_SameForRedispatch(t *testing.T) {
	t.Parallel()

	var keys []string
	var mu sync.Mutex

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			mu.Lock()
			keys = append(keys, request.Header.Get("X-Idempotency-Key"))
			mu.Unlock()

			writer.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	// First dispatch
	_, err = connector.Dispatch(context.Background(), "exc-dup", decision, []byte(`{}`))
	require.NoError(t, err)

	// Second dispatch — same exception, same target produces the same key
	// (at-most-once delivery within Redis TTL window)
	_, err = connector.Dispatch(context.Background(), "exc-dup", decision, []byte(`{}`))
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, keys, 2)
	require.Equal(t, keys[0], keys[1],
		"deterministic idempotency keys must be identical for the same exception and target")
}

func TestHTTPConnector_IdempotencyKey_DiffersForDifferentExceptions(t *testing.T) {
	t.Parallel()

	var keys []string
	var mu sync.Mutex

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			mu.Lock()
			keys = append(keys, request.Header.Get("X-Idempotency-Key"))
			mu.Unlock()

			writer.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond
	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	// Dispatch two different exceptions to the same target
	_, err = connector.Dispatch(context.Background(), "exc-aaa", decision, []byte(`{}`))
	require.NoError(t, err)

	_, err = connector.Dispatch(context.Background(), "exc-bbb", decision, []byte(`{}`))
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, keys, 2)
	require.NotEqual(t, keys[0], keys[1],
		"idempotency keys must differ for different exception IDs")
}

func TestHTTPConnector_ErrorBody_Truncated(t *testing.T) {
	t.Parallel()

	longBody := strings.Repeat("A", 500)

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte(longBody))
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond

	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	_, err = connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrNonRetryableStatus)
	require.Contains(t, err.Error(), "[TRUNCATED]")
	require.NotContains(t, err.Error(), longBody)
	require.Contains(t, err.Error(), strings.Repeat("A", 200)+" [TRUNCATED]")
}

func TestHTTPConnector_ErrorBody_BinaryStripped(t *testing.T) {
	t.Parallel()

	binaryBody := "error\x00\x01\x02message\nwith\rnewlines"

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte(binaryBody))
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond

	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	_, err = connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrNonRetryableStatus)

	errMsg := err.Error()
	require.NotContains(t, errMsg, "\x00")
	require.NotContains(t, errMsg, "\n")
	require.Contains(t, errMsg, "errormessagewithnewlines")
}

func TestHTTPConnector_ErrorBody_EmptyBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusBadRequest)
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond

	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	_, err = connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrNonRetryableStatus)
	require.Contains(t, err.Error(), "400")
	require.NotContains(t, err.Error(), "[TRUNCATED]")
}

func TestHTTPConnector_ErrorBody_RetryableStatusIncludesBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte("service temporarily unavailable"))
		}),
	)
	defer server.Close()

	timeout := 10 * time.Second
	maxRetries := 1
	backoff := time.Nanosecond

	config := connectors.ConnectorConfig{
		AllowPrivateIPs: true, // httptest servers bind to 127.0.0.1
		Webhook: &connectors.WebhookConnectorConfig{
			BaseConnectorConfig: connectors.BaseConnectorConfig{
				Timeout:      &timeout,
				MaxRetries:   &maxRetries,
				RetryBackoff: &backoff,
			},
			URL: server.URL,
		},
	}

	connector, err := connectors.NewHTTPConnector(config)
	require.NoError(t, err)

	decision := services.RoutingDecision{
		Target: services.RoutingTargetWebhook,
	}

	_, err = connector.Dispatch(context.Background(), "exc-123", decision, []byte(`{}`))

	require.Error(t, err)
	require.ErrorIs(t, err, connectors.ErrRetryableHTTPStatus)
	require.Contains(t, err.Error(), "service temporarily unavailable")
	require.Contains(t, err.Error(), "503")
}
