//go:build integration

package exception

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	"github.com/LerianStudio/matcher/tests/integration"
	"github.com/LerianStudio/matcher/tests/integration/server"
)

//nolint:paralleltest,bodyclose // integration tests use shared server; body closed in doRequest
func TestIntegrationIdempotency_SameKeyReturnsCachedResponse(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) { //nolint:thelper
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := setupIdempotencyTestConfig(t, sh)

		idempotencyKey := uuid.New().String()
		csvContent := buildIdempotencyCSV(
			"IDEM-001",
			"100.00",
			"USD",
			"2026-01-15",
			"first request",
		)
		path := idempotencyUploadPath(seed.ContextID, seed.LedgerSourceID)

		resp1, body1, err := doMultipartWithIdempotencyKey(t, sh, path, csvContent, idempotencyKey)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp1.StatusCode,
			"first request should succeed: %s",
			string(body1),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 3)

		resp2, body2, err := doMultipartWithIdempotencyKey(t, sh, path, csvContent, idempotencyKey)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp2.StatusCode,
			"second request should return cached response: %s",
			string(body2),
		)
		require.Equal(
			t,
			"true",
			resp2.Header.Get("X-Idempotency-Replayed"),
			"response should have X-Idempotency-Replayed header",
		)
		require.Equal(t, string(body1), string(body2), "cached response body should match original")
	})
}

//nolint:paralleltest,bodyclose // integration tests use shared server; body closed in doRequest
func TestIntegrationIdempotency_DifferentKeysProcessedSeparately(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) { //nolint:thelper
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := setupIdempotencyTestConfig(t, sh)

		key1 := uuid.New().String()
		key2 := uuid.New().String()
		path := idempotencyUploadPath(seed.ContextID, seed.LedgerSourceID)

		csv1 := buildIdempotencyCSV("IDEM-KEY1-001", "100.00", "USD", "2026-01-15", "request 1")
		csv2 := buildIdempotencyCSV("IDEM-KEY2-001", "200.00", "EUR", "2026-01-16", "request 2")

		resp1, body1, err := doMultipartWithIdempotencyKey(t, sh, path, csv1, key1)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp1.StatusCode,
			"first key request should succeed: %s",
			string(body1),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 3)

		resp2, body2, err := doMultipartWithIdempotencyKey(t, sh, path, csv2, key2)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp2.StatusCode,
			"second key request should succeed: %s",
			string(body2),
		)
		require.Empty(
			t,
			resp2.Header.Get("X-Idempotency-Replayed"),
			"different key should not replay",
		)
		require.NotEqual(
			t,
			string(body1),
			string(body2),
			"different keys should produce different responses",
		)

		sh.DispatchOutboxUntilEmpty(ctx, 3)
	})
}

//nolint:paralleltest,bodyclose // integration tests use shared server; body closed in doRequest
func TestIntegrationIdempotency_HashBasedDeduplication(t *testing.T) {
	// NOTE: Hash-based deduplication cannot work with multipart/form-data requests
	// because each multipart.NewWriter generates a random boundary, making the
	// body hash different every time. This test uses JSON requests instead.
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) { //nolint:thelper
		seed := setupIdempotencyTestConfig(t, sh)

		// Use the matching run endpoint with identical JSON body for hash dedup test
		path := "/v1/matching/contexts/" + seed.ContextID.String() + "/run"
		payload := map[string]string{"mode": "DRY_RUN"}

		// First request without idempotency key - should process
		resp1, body1, err := doJSONWithoutIdempotencyKey(t, sh, http.MethodPost, path, payload)
		require.NoError(t, err)
		require.True(
			t,
			resp1.StatusCode == http.StatusAccepted || resp1.StatusCode == http.StatusOK,
			"first request should succeed: status=%d body=%s",
			resp1.StatusCode,
			string(body1),
		)

		// Second request with identical body - should replay cached response
		resp2, body2, err := doJSONWithoutIdempotencyKey(t, sh, http.MethodPost, path, payload)
		require.NoError(t, err)
		require.True(
			t,
			resp2.StatusCode == http.StatusAccepted || resp2.StatusCode == http.StatusOK,
			"second request should return cached: status=%d body=%s",
			resp2.StatusCode,
			string(body2),
		)
		require.Equal(
			t,
			"true",
			resp2.Header.Get("X-Idempotency-Replayed"),
			"same body hash should replay cached response",
		)
		require.Equal(
			t,
			string(body1),
			string(body2),
			"hash-deduplicated response should match original",
		)
	})
}

//nolint:paralleltest,bodyclose // integration tests use shared server; body closed in doRequest
func TestIntegrationIdempotency_FailedRequestAllowsRetry(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) { //nolint:thelper
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := setupIdempotencyTestConfig(t, sh)

		idempotencyKey := uuid.New().String()
		nonExistentSourceID := uuid.New()
		path := idempotencyUploadPath(seed.ContextID, nonExistentSourceID)
		csvContent := buildIdempotencyCSV(
			"FAIL-RETRY-001",
			"100.00",
			"USD",
			"2026-01-15",
			"will fail",
		)

		resp1, body1, err := doMultipartWithIdempotencyKey(t, sh, path, csvContent, idempotencyKey)
		require.NoError(t, err)
		require.True(
			t,
			resp1.StatusCode >= 400,
			"first request should fail with non-existent source: status=%d, body=%s",
			resp1.StatusCode,
			string(body1),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 3)

		validPath := idempotencyUploadPath(seed.ContextID, seed.LedgerSourceID)
		resp2, body2, err := doMultipartWithIdempotencyKey(
			t,
			sh,
			validPath,
			csvContent,
			idempotencyKey,
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp2.StatusCode,
			"retry after failure should succeed: %s",
			string(body2),
		)
		require.Empty(
			t,
			resp2.Header.Get("X-Idempotency-Replayed"),
			"retry should process as new request, not replay",
		)

		sh.DispatchOutboxUntilEmpty(ctx, 3)
	})
}

type idempotencyTestSeed struct {
	TenantID          uuid.UUID
	ContextID         uuid.UUID
	LedgerSourceID    uuid.UUID
	NonLedgerSourceID uuid.UUID
}

func setupIdempotencyTestConfig(t *testing.T, sh *server.ServerHarness) idempotencyTestSeed {
	t.Helper()

	ctx := sh.ServerCtx()
	conn := sh.Connection
	seed := sh.TestHarness.Seed

	return setupIdempotencyTestConfigWithConnection(t, ctx, conn, seed)
}

func setupIdempotencyTestConfigWithConnection(
	t *testing.T,
	ctx context.Context,
	conn *libPostgres.Client,
	seed integration.SeedData,
) idempotencyTestSeed {
	t.Helper()

	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(conn, nil)

	srcRepo, err := configSourceRepo.NewRepository(provider)
	require.NoError(t, err)
	fmRepo := configFieldMapRepo.NewRepository(provider)

	bankSrc, err := configEntities.NewReconciliationSource(
		ctx,
		seed.ContextID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "Idempotency Test Bank Source",
			Type:   configVO.SourceTypeBank,
			Side:   sharedfee.MatchingSideRight,
			Config: map[string]any{"format": "csv"},
		},
	)
	require.NoError(t, err)

	createdBankSrc, err := srcRepo.Create(ctx, bankSrc)
	require.NoError(t, err)

	mapping := map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}

	ledgerFM, err := configEntities.NewFieldMap(
		ctx,
		seed.ContextID,
		seed.SourceID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, ledgerFM)
	require.NoError(t, err)

	bankFM, err := configEntities.NewFieldMap(
		ctx,
		seed.ContextID,
		createdBankSrc.ID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, bankFM)
	require.NoError(t, err)

	return idempotencyTestSeed{
		TenantID:          seed.TenantID,
		ContextID:         seed.ContextID,
		LedgerSourceID:    seed.SourceID,
		NonLedgerSourceID: createdBankSrc.ID,
	}
}

func idempotencyUploadPath(contextID, sourceID uuid.UUID) string {
	return "/v1/imports/contexts/" + contextID.String() + "/sources/" + sourceID.String() + "/upload"
}

func buildIdempotencyCSV(externalID, amount, currency, date, description string) []byte {
	return []byte("id,amount,currency,date,description\n" +
		externalID + "," + amount + "," + currency + "," + date + "," + description + "\n")
}

func doMultipartWithIdempotencyKey(
	t *testing.T,
	sh *server.ServerHarness,
	path string,
	fileContent []byte,
	idempotencyKey string,
) (*http.Response, []byte, error) {
	t.Helper()

	req, err := buildMultipartRequest(path, fileContent)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("X-Idempotency-Key", idempotencyKey)
	req.Header.Set("X-Tenant-ID", sh.TestHarness.Seed.TenantID.String())

	return doRequest(sh, req)
}

func doJSONWithoutIdempotencyKey(
	t *testing.T,
	sh *server.ServerHarness,
	method, path string,
	payload any,
) (*http.Response, []byte, error) {
	t.Helper()

	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, nil, err
		}
		body = bytes.NewReader(data)
	}

	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", sh.TestHarness.Seed.TenantID.String())

	return doRequest(sh, req)
}

//nolint:wrapcheck // test helper, errors are handled by caller
func buildMultipartRequest(path string, fileContent []byte) (*http.Request, error) {
	var buf bytes.Buffer

	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("file", "ledger.csv")
	if err != nil {
		return nil, err
	}

	if _, err := part.Write(fileContent); err != nil {
		return nil, err
	}

	if err := writer.WriteField("format", "csv"); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	return req, nil
}

//nolint:wrapcheck // test helper, errors are handled by caller
func doRequest(sh *server.ServerHarness, req *http.Request) (*http.Response, []byte, error) {
	resp, err := sh.Service.GetApp().Test(req, 30000) //nolint:mnd // test timeout constant
	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, err
	}

	return resp, body, nil
}
