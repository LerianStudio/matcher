//go:build integration

package exception

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/adapters/http/connectors"
	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegrationExternalPushCallback_WebhookDispatch(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		var receivedPayload []byte
		var receivedSignature string
		var callCount int32

		webhookServer := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()

				atomic.AddInt32(&callCount, 1)
				receivedSignature = r.Header.Get("X-Signature-256")

				var err error
				receivedPayload, err = io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("failed to read webhook request body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)

					return
				}

				w.WriteHeader(http.StatusOK)
			}),
		)
		defer webhookServer.Close()

		maxRetries := 3
		config := connectors.ConnectorConfig{
			AllowPrivateIPs: true, // Required for integration tests using httptest.NewServer()
			Webhook: &connectors.WebhookConnectorConfig{
				BaseConnectorConfig: connectors.BaseConnectorConfig{
					MaxRetries: &maxRetries,
				},
				URL:          webhookServer.URL,
				SharedSecret: "test-secret-key",
			},
		}

		connector, err := connectors.NewHTTPConnector(config)
		require.NoError(t, err)

		exceptionID := uuid.New().String()
		decision := services.RoutingDecision{
			Target:   services.RoutingTargetWebhook,
			Queue:    "exception-queue",
			RuleName: "test-rule",
		}

		payload, err := json.Marshal(map[string]any{
			"exception_id": exceptionID,
			"severity":     "HIGH",
			"status":       "OPEN",
		})
		require.NoError(t, err)

		result, err := connector.Dispatch(ctx, exceptionID, decision, payload)
		require.NoError(t, err)
		require.True(t, result.Acknowledged)
		require.Equal(t, services.RoutingTargetWebhook, result.Target)

		require.Equal(t, int32(1), atomic.LoadInt32(&callCount))
		require.NotEmpty(t, receivedPayload)
		require.NotEmpty(t, receivedSignature, "HMAC signature should be present")
		require.Contains(t, receivedSignature, "sha256=")
	})
}

func TestIntegrationExternalPushCallback_JiraDispatch(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		var receivedPayload []byte
		var receivedAuth string

		jiraServer := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()

				receivedAuth = r.Header.Get("Authorization")

				var err error
				receivedPayload, err = io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("failed to read jira request body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)

					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"key":"PROJ-123"}`))
			}),
		)
		defer jiraServer.Close()

		maxRetries := 3
		config := connectors.ConnectorConfig{
			AllowPrivateIPs: true, // Required for integration tests using httptest.NewServer()
			Jira: &connectors.JiraConnectorConfig{
				BaseConnectorConfig: connectors.BaseConnectorConfig{
					MaxRetries: &maxRetries,
				},
				BaseURL:    jiraServer.URL,
				AuthToken:  "test-jira-token",
				ProjectKey: "OPS",
				IssueType:  "Task",
			},
		}

		connector, err := connectors.NewHTTPConnector(config)
		require.NoError(t, err)

		exceptionID := uuid.New().String()
		decision := services.RoutingDecision{
			Target:   services.RoutingTargetJira,
			Queue:    "OPS",
			RuleName: "critical-rule",
		}

		payload, err := json.Marshal(map[string]any{
			"fields": map[string]any{
				"project":     map[string]any{"key": "OPS"},
				"summary":     "Exception: " + exceptionID,
				"description": "Unmatched transaction requires review",
				"issuetype":   map[string]any{"name": "Task"},
			},
		})
		require.NoError(t, err)

		result, err := connector.Dispatch(ctx, exceptionID, decision, payload)
		require.NoError(t, err)
		require.True(t, result.Acknowledged)
		require.Equal(t, services.RoutingTargetJira, result.Target)
		require.Equal(t, "PROJ-123", result.ExternalReference)

		require.Equal(t, "Bearer test-jira-token", receivedAuth)
		require.NotEmpty(t, receivedPayload)
	})
}

func TestIntegrationExternalPushCallback_ManualRouting(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		config := connectors.ConnectorConfig{}
		connector, err := connectors.NewHTTPConnector(config)
		require.NoError(t, err)

		exceptionID := uuid.New().String()
		decision := services.RoutingDecision{
			Target:   services.RoutingTargetManual,
			RuleName: "manual-rule",
		}

		result, err := connector.Dispatch(ctx, exceptionID, decision, nil)
		require.NoError(t, err)
		require.True(t, result.Acknowledged)
		require.Equal(t, services.RoutingTargetManual, result.Target)
		require.Empty(t, result.ExternalReference)
	})
}

func TestIntegrationExternalPushCallback_IdempotencyGuarantee(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		idempotencyRepo := wireIdempotencyRepo(t, h)

		keyStr := "callback-" + uuid.New().String()
		key, err := exceptionVO.ParseIdempotencyKey(keyStr)
		require.NoError(t, err)

		acquired1, err := idempotencyRepo.TryAcquire(ctx, key)
		require.NoError(t, err)
		require.True(t, acquired1, "first acquire should succeed")

		acquired2, err := idempotencyRepo.TryAcquire(ctx, key)
		require.NoError(t, err)
		require.False(t, acquired2, "second acquire should fail (duplicate)")

		err = idempotencyRepo.MarkComplete(ctx, key, nil, 0)
		require.NoError(t, err)

		acquired3, err := idempotencyRepo.TryAcquire(ctx, key)
		require.NoError(t, err)
		require.False(t, acquired3, "acquire after complete should still fail")
	})
}

func TestIntegrationExternalPushCallback_RetryOnTransientFailure(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		var callCount int32

		webhookServer := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := atomic.AddInt32(&callCount, 1)
				if count < 3 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}

				w.WriteHeader(http.StatusOK)
			}),
		)
		defer webhookServer.Close()

		maxRetries := 5
		retryBackoff := 10 * time.Millisecond
		config := connectors.ConnectorConfig{
			AllowPrivateIPs: true, // Required for integration tests using httptest.NewServer()
			Webhook: &connectors.WebhookConnectorConfig{
				BaseConnectorConfig: connectors.BaseConnectorConfig{
					MaxRetries:   &maxRetries,
					RetryBackoff: &retryBackoff,
				},
				URL: webhookServer.URL,
			},
		}

		connector, err := connectors.NewHTTPConnector(config)
		require.NoError(t, err)

		exceptionID := uuid.New().String()
		decision := services.RoutingDecision{
			Target:   services.RoutingTargetWebhook,
			RuleName: "retry-rule",
		}

		result, err := connector.Dispatch(ctx, exceptionID, decision, []byte(`{"test":"retry"}`))
		require.NoError(t, err)
		require.True(t, result.Acknowledged)
		require.GreaterOrEqual(t, atomic.LoadInt32(&callCount), int32(3))
	})
}

func TestIntegrationExternalPushCallback_NonRetryableError(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		webhookServer := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid payload"}`))
			}),
		)
		defer webhookServer.Close()

		maxRetries := 3
		config := connectors.ConnectorConfig{
			AllowPrivateIPs: true, // Required for integration tests using httptest.NewServer()
			Webhook: &connectors.WebhookConnectorConfig{
				BaseConnectorConfig: connectors.BaseConnectorConfig{
					MaxRetries: &maxRetries,
				},
				URL: webhookServer.URL,
			},
		}

		connector, err := connectors.NewHTTPConnector(config)
		require.NoError(t, err)

		exceptionID := uuid.New().String()
		decision := services.RoutingDecision{
			Target:   services.RoutingTargetWebhook,
			RuleName: "bad-payload-rule",
		}

		_, err = connector.Dispatch(ctx, exceptionID, decision, []byte(`invalid`))
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-retryable")
	})
}
