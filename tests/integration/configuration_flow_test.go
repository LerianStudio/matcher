//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	"github.com/LerianStudio/matcher/tests/integration/ratelimit"
)

func TestIntegration_Flow_ConfigurationFlow_Integration(t *testing.T) {
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
		t.Setenv("PLUGIN_AUTH_ENABLED", "false")
		t.Setenv("ENABLE_TELEMETRY", "false")
		t.Setenv("LOG_LEVEL", "debug")
		t.Setenv("RATE_LIMIT_MAX", "1000")
		t.Setenv("RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
		t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "300")
		t.Setenv("DISPATCH_RATE_LIMIT_MAX", "100")
		t.Setenv("DISPATCH_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("EXPORT_WORKER_ENABLED", "false")
		t.Setenv("CLEANUP_WORKER_ENABLED", "false")
		t.Setenv("OBJECT_STORAGE_ENDPOINT", "")
		t.Setenv("ARCHIVAL_WORKER_ENABLED", "false")
		t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "+PnwgNy8bL3HGT1rOXp47PqyGcPywXH/epgmSVwPkL0=")

		// Initialize Service
		service, err := bootstrap.InitServersWithOptions(nil)
		require.NoError(t, err)
		require.NotNil(t, service)

		// Override systemplane-registered rate-limit defaults with test-friendly
		// values. Without this, the registered compile-time defaults
		// (rate_limit.max=100) mask the env-based RATE_LIMIT_MAX=1000 and cause
		// sequential test requests to 429 after ~100 calls.
		require.NoError(t, ratelimit.OverrideRateLimitsForTests(context.Background(), service))

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

			return hasStatus(client, baseURL+"/readyz", http.StatusOK)
		}, 30*time.Second, 200*time.Millisecond, "Server failed to start")

		select {
		case err := <-runErr:
			runErrValue = err
		default:
		}

		if runErrValue != nil {
			t.Fatalf("server run error: %v", runErrValue)
		}

		db, err := sql.Open("pgx", h.PostgresDSN)
		require.NoError(t, err)
		defer db.Close()

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
			baseURL+"/v1/contexts",
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
				fmt.Sprintf("%s/v1/contexts/%s", baseURL, contextID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 2: Create Ledger Source ---
		ledgerPayload := map[string]any{
			"name": "Ledger Source",
			"type": "LEDGER",
			"side": "LEFT",
			"config": map[string]any{
				"table": "journal_entries",
			},
		}
		ledgerResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf("%s/v1/contexts/%s/sources", baseURL, contextID),
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
				fmt.Sprintf("%s/v1/contexts/%s/sources/%s", baseURL, contextID, ledgerID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 3: Create Bank Source ---
		bankPayload := map[string]any{
			"name": "Bank Source",
			"type": "BANK",
			"side": "RIGHT",
			"config": map[string]any{
				"file_format": "mt940",
			},
		}
		bankResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf("%s/v1/contexts/%s/sources", baseURL, contextID),
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
				fmt.Sprintf("%s/v1/contexts/%s/sources/%s", baseURL, contextID, bankID),
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
				"%s/v1/contexts/%s/sources/%s/field-maps",
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
				fmt.Sprintf("%s/v1/field-maps/%s", baseURL, ledgerMapIDVal),
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
				"%s/v1/contexts/%s/sources/%s/field-maps",
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
				fmt.Sprintf("%s/v1/field-maps/%s", baseURL, bankMapIDVal),
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
			fmt.Sprintf("%s/v1/contexts/%s/rules", baseURL, contextID),
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
				fmt.Sprintf("%s/v1/contexts/%s/rules/%s", baseURL, contextID, ruleID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 7: List Rules and Verify ---
		listRulesResp := makeRequest(
			t,
			client,
			"GET",
			fmt.Sprintf("%s/v1/contexts/%s/rules", baseURL, contextID),
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
			fmt.Sprintf("%s/v1/contexts/%s", baseURL, contextID),
			nil,
			http.StatusOK,
		)
		nameVal, ok := getContextResp["name"].(string)
		require.True(t, ok, "Expected 'name' to be a string in response: %v", getContextResp)
		require.Equal(t, "E2E Test Context", nameVal)
		t.Log("Verified Get Context")

		// --- Step 9: Create Fee Schedule (prerequisite for fee rules) ---
		schedulePayload := map[string]any{
			"name":             "E2E Test Schedule",
			"currency":         "USD",
			"applicationOrder": "PARALLEL",
			"roundingScale":    2,
			"roundingMode":     "HALF_UP",
			"items": []map[string]any{
				{
					"name":          "Processing Fee",
					"priority":      1,
					"structureType": "FLAT",
					"structure": map[string]any{
						"amount": "2.50",
					},
				},
			},
		}
		scheduleResp := makeRequest(
			t,
			client,
			"POST",
			baseURL+"/v1/fee-schedules",
			schedulePayload,
			http.StatusCreated,
		)
		scheduleIDVal, ok := scheduleResp["id"].(string)
		require.True(t, ok, "Expected 'id' to be a string in fee schedule response: %v", scheduleResp)
		require.NotEmpty(t, scheduleIDVal)
		scheduleID := scheduleIDVal
		t.Logf("Created Fee Schedule: %s", scheduleID)
		t.Cleanup(func() {
			makeRequest(
				t,
				client,
				"DELETE",
				fmt.Sprintf("%s/v1/fee-schedules/%s", baseURL, scheduleID),
				nil,
				http.StatusNoContent,
			)
		})

		// --- Step 10: Create Fee Rule ---
		feeRulePayload := map[string]any{
			"side":          "RIGHT",
			"feeScheduleId": scheduleID,
			"name":          "E2E Right-Side Rule",
			"priority":      0,
			"predicates": []map[string]any{
				{
					"field":    "institution",
					"operator": "EQUALS",
					"value":    "Itau",
				},
			},
		}
		feeRuleResp := makeRequest(
			t,
			client,
			"POST",
			fmt.Sprintf("%s/v1/config/contexts/%s/fee-rules", baseURL, contextID),
			feeRulePayload,
			http.StatusCreated,
		)
		feeRuleIDVal, ok := feeRuleResp["id"].(string)
		require.True(t, ok, "Expected 'id' to be a string in fee rule response: %v", feeRuleResp)
		require.NotEmpty(t, feeRuleIDVal)
		feeRuleID := feeRuleIDVal
		t.Logf("Created Fee Rule: %s", feeRuleID)
		feeRuleDeleted := false
		// NOTE: t.Cleanup is LIFO. This runs BEFORE the fee schedule cleanup
		// (registered earlier), ensuring correct FK deletion order.
		t.Cleanup(func() {
			if !feeRuleDeleted {
				makeRequest(
					t,
					client,
					"DELETE",
					fmt.Sprintf("%s/v1/config/fee-rules/%s", baseURL, feeRuleID),
					nil,
					http.StatusNoContent,
				)
			}
		})

		// Verify created values.
		require.Equal(t, "RIGHT", feeRuleResp["side"])
		require.Equal(t, "E2E Right-Side Rule", feeRuleResp["name"])
		require.Equal(t, scheduleID, feeRuleResp["feeScheduleId"])

		predicatesVal, ok := feeRuleResp["predicates"].([]any)
		require.True(t, ok, "Expected 'predicates' to be a list in response: %v", feeRuleResp)
		require.Len(t, predicatesVal, 1)

		// --- Step 11: Read back Fee Rule ---
		getFeeRuleResp := makeRequest(
			t,
			client,
			"GET",
			fmt.Sprintf("%s/v1/config/fee-rules/%s", baseURL, feeRuleID),
			nil,
			http.StatusOK,
		)
		require.Equal(t, feeRuleID, getFeeRuleResp["id"])
		require.Equal(t, "RIGHT", getFeeRuleResp["side"])
		require.Equal(t, "E2E Right-Side Rule", getFeeRuleResp["name"])
		t.Log("Verified Get Fee Rule")

		// --- Step 12: Update Fee Rule ---
		updateFeeRulePayload := map[string]any{
			"name":     "Updated Right-Side Rule",
			"side":     "LEFT",
			"priority": 5,
		}
		updateFeeRuleResp := makeRequest(
			t,
			client,
			"PATCH",
			fmt.Sprintf("%s/v1/config/fee-rules/%s", baseURL, feeRuleID),
			updateFeeRulePayload,
			http.StatusOK,
		)
		require.Equal(t, "Updated Right-Side Rule", updateFeeRuleResp["name"])
		require.Equal(t, "LEFT", updateFeeRuleResp["side"])

		// JSON numbers are float64.
		updatedPriority, ok := updateFeeRuleResp["priority"].(float64)
		require.True(t, ok, "Expected 'priority' to be a number: %v", updateFeeRuleResp)
		require.Equal(t, float64(5), updatedPriority)
		t.Log("Verified Update Fee Rule")

		// --- Step 13: List Fee Rules for Context ---
		listFeeRulesResp := makeListRequest(
			t,
			client,
			"GET",
			fmt.Sprintf("%s/v1/config/contexts/%s/fee-rules", baseURL, contextID),
			nil,
			http.StatusOK,
		)
		require.Len(t, listFeeRulesResp, 1)
		t.Log("Verified List Fee Rules")

		// --- Step 14: Delete Fee Rule ---
		makeRequest(
			t,
			client,
			"DELETE",
			fmt.Sprintf("%s/v1/config/fee-rules/%s", baseURL, feeRuleID),
			nil,
			http.StatusNoContent,
		)
		feeRuleDeleted = true
		t.Log("Deleted Fee Rule")

		contextUUID, err := uuid.Parse(contextID)
		require.NoError(t, err)
		scheduleUUID, err := uuid.Parse(scheduleID)
		require.NoError(t, err)
		ruleUUID, err := uuid.Parse(ruleID)
		require.NoError(t, err)
		bankUUID, err := uuid.Parse(bankID)
		require.NoError(t, err)

		cleanupVarianceHistory := attachVarianceHistoryReference(t, db, contextUUID, bankUUID, ruleUUID, scheduleUUID, "E2E Test Schedule")
		defer cleanupVarianceHistory()

		conflictResp := makeRequest(
			t,
			client,
			"DELETE",
			fmt.Sprintf("%s/v1/fee-schedules/%s", baseURL, scheduleID),
			nil,
			http.StatusConflict,
		)
		require.Equal(t, "Conflict", conflictResp["title"])
		require.Equal(t, "fee schedule is still in use", conflictResp["message"])
		t.Log("Verified Fee Schedule Delete Conflict From Variance History")
		cleanupVarianceHistory()

		// Verify deletion — GET should 404.
		makeRequest(
			t,
			client,
			"GET",
			fmt.Sprintf("%s/v1/config/fee-rules/%s", baseURL, feeRuleID),
			nil,
			http.StatusNotFound,
		)
		t.Log("Verified Fee Rule Deletion (404)")
	})
}

func attachVarianceHistoryReference(
	t *testing.T,
	db *sql.DB,
	contextID, sourceID, ruleID, scheduleID uuid.UUID,
	scheduleName string,
) func() {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	runID := uuid.New()
	groupID := uuid.New()
	transactionID := uuid.New()
	ingestionJobID := uuid.New()
	itemID := uuid.New()
	varianceID := uuid.New()
	createdAt := time.Now().UTC()

	_, err := db.ExecContext(ctx, `
		INSERT INTO match_runs (id, context_id, mode, status, stats)
		VALUES ($1, $2, 'COMMIT', 'COMPLETED', '{}'::jsonb)
	`, runID, contextID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO ingestion_jobs (id, context_id, source_id, status)
		VALUES ($1, $2, $3, 'COMPLETED')
	`, ingestionJobID, contextID, sourceID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status)
		VALUES ($1, $2, $3, $4, 100.00, 'USD', NOW(), 'COMPLETE', 'MATCHED')
	`, transactionID, ingestionJobID, sourceID, varianceID.String())
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
		VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
	`, groupID, contextID, runID, ruleID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
		VALUES ($1, $2, $3, 100.00, 'USD')
	`, itemID, groupID, transactionID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO match_fee_variances (
			id, context_id, run_id, match_group_id, transaction_id, fee_schedule_id, fee_schedule_name_snapshot,
			currency, expected_fee_amount, actual_fee_amount, delta,
			tolerance_abs, tolerance_percent, variance_type, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'USD', $8, $9, $10, $11, $12, $13, $14, $14)
	`,
		varianceID,
		contextID,
		runID,
		groupID,
		transactionID,
		scheduleID,
		scheduleName,
		decimal.RequireFromString("10.00"),
		decimal.RequireFromString("12.00"),
		decimal.RequireFromString("2.00"),
		decimal.RequireFromString("0.01"),
		decimal.RequireFromString("0.05"),
		"OVERCHARGE",
		createdAt,
	)
	require.NoError(t, err)

	cleaned := false

	return func() {
		if cleaned {
			return
		}
		cleaned = true

		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM match_fee_variances WHERE id = $1`, varianceID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM match_items WHERE id = $1`, itemID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM match_groups WHERE id = $1`, groupID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM transactions WHERE id = $1`, transactionID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM ingestion_jobs WHERE id = $1`, ingestionJobID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM match_runs WHERE id = $1`, runID)
	}
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

// makeListRequest is like makeRequest but for endpoints that return a JSON array.
func makeListRequest(
	t *testing.T,
	client *http.Client,
	method, url string,
	body any,
	expectedStatus int,
) []map[string]any {
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

	var result []map[string]any

	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		t.Fatalf("Failed to parse JSON array response: %v. Body: %s", err, string(bodyBytes))
	}

	return result
}
