//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigurationFlow_Integration(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		setProjectRoot(t)
		postgresHost, postgresPort := extractHostPort(t, h.PostgresDSN)
		redisAddr := extractRedisAddress(t, h.RedisAddr)

		// Setup environment for testing
		t.Setenv("ENV_NAME", "test")
		t.Setenv("SERVER_ADDRESS", ":18081") // Use a different port than startup test
		t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "30")
		t.Setenv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111")
		t.Setenv("DEFAULT_TENANT_SLUG", "default")
		t.Setenv("POSTGRES_HOST", postgresHost)
		t.Setenv("POSTGRES_PORT", postgresPort)
		t.Setenv("POSTGRES_USER", "matcher")
		t.Setenv("POSTGRES_PASSWORD", "matcher_test")
		t.Setenv("POSTGRES_DB", "matcher_test")
		t.Setenv("POSTGRES_SSLMODE", "disable")
		t.Setenv("MIGRATIONS_PATH", "migrations")
		t.Setenv("REDIS_HOST", redisAddr)
		t.Setenv("RABBITMQ_URI", "amqp")
		t.Setenv("RABBITMQ_HOST", h.RabbitMQHost)
		t.Setenv("RABBITMQ_PORT", h.RabbitMQPort)
		t.Setenv("RABBITMQ_HEALTH_URL", h.RabbitMQHealthURL)
		t.Setenv("RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK", "true")
		t.Setenv("RABBITMQ_USER", "guest")
		t.Setenv("RABBITMQ_PASSWORD", "guest")
		t.Setenv("RABBITMQ_VHOST", "/")
		t.Setenv("AUTH_ENABLED", "false")
		t.Setenv("ENABLE_TELEMETRY", "false")
		t.Setenv("LOG_LEVEL", "debug")
		t.Setenv("RATE_LIMIT_MAX", "1000")
		t.Setenv("RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
		t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "300")
		t.Setenv("DISPATCH_RATE_LIMIT_MAX", "100")
		t.Setenv("DISPATCH_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("OBJECT_STORAGE_ENDPOINT", "")
		t.Setenv("ARCHIVAL_WORKER_ENABLED", "false")

		// Initialize Service
		service, err := bootstrap.InitServersWithOptions(nil)
		require.NoError(t, err)
		require.NotNil(t, service)

		runErr := make(chan error, 1)

		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			service.Shutdown(ctx)

			select {
			case err := <-runErr:
				if err != nil {
					t.Logf("server run error: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Log("server did not shutdown within timeout")
			}
		})

		go func() {
			runErr <- service.Server.Run(nil)
		}()

		// Wait for readiness
		client := &http.Client{Timeout: 5 * time.Second}
		baseURL := "http://localhost:18081"

		var runErrValue error

		assert.Eventually(t, func() bool {
			if runErrValue != nil {
				return false
			}

			select {
			case err := <-runErr:
				runErrValue = err

				return false
			default:
			}

			return hasStatus(client, baseURL+"/ready", http.StatusOK)
		}, 30*time.Second, 200*time.Millisecond, "Server failed to start")

		select {
		case err := <-runErr:
			runErrValue = err
		default:
		}

		if runErrValue != nil {
			t.Fatalf("server run error: %v", runErrValue)
		}

		// --- Step 1: Create Reconciliation Context ---
		contextPayload := map[string]any{
			"name":     "E2E Test Context",
			"type":     "1:1",
			"interval": "0 * * * *",
		}
		contextResp := makeRequest(
			t,
			client,
			"POST",
			baseURL+"/v1/config/contexts",
			contextPayload,
			http.StatusCreated,
		)
		contextIDVal, ok := contextResp["id"].(string)
		require.True(t, ok, "Expected 'id' to be a string in response: %v", contextResp)
		require.NotEmpty(t, contextIDVal)
		contextID := contextIDVal
		t.Logf("Created Context: %s", contextID)
		// NOTE: t.Cleanup runs in LIFO order. This context deletion runs LAST
		// (registered first), after all children (field maps, rules, sources)
		// are cleaned up by their own t.Cleanup registrations below.
		t.Cleanup(func() {
			makeRequest(
				t,
				client,
				"DELETE",
				fmt.Sprintf("%s/v1/config/contexts/%s", baseURL, contextID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 2: Create Ledger Source ---
		ledgerPayload := map[string]any{
			"name": "Ledger Source",
			"type": "LEDGER",
			"config": map[string]any{
				"table": "journal_entries",
			},
		}
		ledgerResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf("%s/v1/config/contexts/%s/sources", baseURL, contextID),
			ledgerPayload,
			http.StatusCreated,
		)
		ledgerIDVal, ok := ledgerResp["id"].(string)
		require.True(t, ok, "Expected 'id' to be a string in response: %v", ledgerResp)
		require.NotEmpty(t, ledgerIDVal)
		ledgerID := ledgerIDVal
		t.Logf("Created Ledger Source: %s", ledgerID)
		t.Cleanup(func() {
			makeRequest(
				t,
				client,
				"DELETE",
				fmt.Sprintf("%s/v1/config/contexts/%s/sources/%s", baseURL, contextID, ledgerID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 3: Create Bank Source ---
		bankPayload := map[string]any{
			"name": "Bank Source",
			"type": "BANK",
			"config": map[string]any{
				"file_format": "mt940",
			},
		}
		bankResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf("%s/v1/config/contexts/%s/sources", baseURL, contextID),
			bankPayload,
			http.StatusCreated,
		)
		bankIDVal, ok := bankResp["id"].(string)
		require.True(t, ok, "Expected 'id' to be a string in response: %v", bankResp)
		require.NotEmpty(t, bankIDVal)
		bankID := bankIDVal
		t.Logf("Created Bank Source: %s", bankID)
		t.Cleanup(func() {
			makeRequest(
				t,
				client,
				"DELETE",
				fmt.Sprintf("%s/v1/config/contexts/%s/sources/%s", baseURL, contextID, bankID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 4: Create Field Map for Ledger ---
		ledgerMapPayload := map[string]any{
			"mapping": map[string]any{
				"amount":   "amount",
				"currency": "currency",
				"date":     "posting_date",
				"ref":      "transaction_id",
			},
		}
		ledgerMapResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf(
				"%s/v1/config/contexts/%s/sources/%s/field-maps",
				baseURL,
				contextID,
				ledgerID,
			),
			ledgerMapPayload,
			http.StatusCreated,
		)
		ledgerMapIDVal, ok := ledgerMapResp["id"].(string)
		require.True(t, ok, "Expected 'id' in field map response: %v", ledgerMapResp)
		t.Log("Created Field Map for Ledger")
		t.Cleanup(func() {
			makeRequest(
				t,
				client,
				"DELETE",
				fmt.Sprintf("%s/v1/config/field-maps/%s", baseURL, ledgerMapIDVal),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 5: Create Field Map for Bank ---
		bankMapPayload := map[string]any{
			"mapping": map[string]any{
				"amount":   "amt",
				"currency": "curr",
				"date":     "val_date",
				"ref":      "ref_id",
			},
		}
		bankMapResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf(
				"%s/v1/config/contexts/%s/sources/%s/field-maps",
				baseURL,
				contextID,
				bankID,
			),
			bankMapPayload,
			http.StatusCreated,
		)
		bankMapIDVal, ok := bankMapResp["id"].(string)
		require.True(t, ok, "Expected 'id' in field map response: %v", bankMapResp)
		t.Log("Created Field Map for Bank")
		t.Cleanup(func() {
			makeRequest(
				t,
				client,
				"DELETE",
				fmt.Sprintf("%s/v1/config/field-maps/%s", baseURL, bankMapIDVal),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 6: Create Match Rule ---
		rulePayload := map[string]any{
			"priority": 10,
			"type":     "EXACT",
			"config": map[string]any{
				"matchAmount":   true,
				"matchCurrency": true,
			},
		}
		ruleResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf("%s/v1/config/contexts/%s/rules", baseURL, contextID),
			rulePayload,
			http.StatusCreated,
		)
		ruleIDVal, ok := ruleResp["id"].(string)
		require.True(t, ok, "Expected 'id' to be a string in response: %v", ruleResp)
		require.NotEmpty(t, ruleIDVal)
		ruleID := ruleIDVal
		t.Logf("Created Match Rule: %s", ruleID)
		t.Cleanup(func() {
			makeRequest(
				t,
				client,
				"DELETE",
				fmt.Sprintf("%s/v1/config/contexts/%s/rules/%s", baseURL, contextID, ruleID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 7: List Rules and Verify ---
		listRulesResp := makeRequest(
			t,
			client,
			"GET",
			fmt.Sprintf("%s/v1/config/contexts/%s/rules", baseURL, contextID),
			nil,
			http.StatusOK,
		)
		itemsVal, ok := listRulesResp["items"].([]any)
		require.True(t, ok, "Expected 'items' to be a list in response: %v", listRulesResp)
		require.Len(t, itemsVal, 1)

		// --- Step 8: Get Context and Verify ---
		getContextResp := makeRequest(
			t,
			client,
			"GET",
			fmt.Sprintf("%s/v1/config/contexts/%s", baseURL, contextID),
			nil,
			http.StatusOK,
		)
		nameVal, ok := getContextResp["name"].(string)
		require.True(t, ok, "Expected 'name' to be a string in response: %v", getContextResp)
		require.Equal(t, "E2E Test Context", nameVal)
		t.Log("Verified Get Context")
	})
}

func makeRequest(
	t *testing.T,
	client *http.Client,
	method, url string,
	body any,
	expectedStatus int,
) map[string]any {
	t.Helper()

	var bodyReader io.Reader

	if body != nil {
		jsonBody, err := json.Marshal(body)
		require.NoError(t, err)

		bodyReader = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Failed to read response body")

	if resp.StatusCode != expectedStatus {
		t.Fatalf(
			"Unexpected status code for %s %s: got %d, want %d. Body: %s",
			method,
			url,
			resp.StatusCode,
			expectedStatus,
			string(bodyBytes),
		)
	}

	if len(bodyBytes) == 0 {
		return nil
	}

	var result map[string]any

	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		t.Fatalf("Failed to parse JSON response: %v. Body: %s", err, string(bodyBytes))
	}

	return result
}
