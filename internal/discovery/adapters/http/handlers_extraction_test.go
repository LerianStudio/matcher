//go:build unit

package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestStartExtraction_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	fixture.schemaRepo.schemas[conn.ID] = []*entities.DiscoveredSchema{{
		ID:           uuid.New(),
		ConnectionID: conn.ID,
		TableName:    "transactions",
		Columns: []entities.ColumnInfo{
			{Name: "id", Type: "uuid"},
			{Name: "amount", Type: "numeric"},
		},
	}}
	fixture.fetcherMock.submitJobID = "job-start-123"

	app := setupTestApp(t, fixture.handler)
	body, err := json.Marshal(dto.StartExtractionRequest{
		Tables: map[string]dto.ExtractionTableRequest{
			"transactions": {
				Columns: []string{"id", "amount"},
			},
		},
		StartDate: "2026-03-01",
		EndDate:   "2026-03-08",
		Filters:   &sharedPorts.ExtractionFilters{Equals: map[string]string{"currency": "USD"}},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+conn.ID.String()+"/extractions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var response dto.ExtractionRequestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, conn.ID, response.ConnectionID)
	assert.Equal(t, "2026-03-01", response.StartDate)
	assert.Equal(t, "2026-03-08", response.EndDate)
	assert.Equal(t, vo.ExtractionStatusSubmitted.String(), response.Status)
	assert.Len(t, response.Tables, 1)
	assert.Equal(t, []string{"id", "amount"}, response.Tables["transactions"].Columns)
	assert.Equal(t, 1, fixture.extractionRepo.createCount)
	assert.Equal(t, 1, fixture.extractionRepo.updateCount)
}

func TestStartExtraction_ConnectionNotFound(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	fixture.fetcherMock.submitJobID = "job-start-123"
	app := setupTestApp(t, fixture.handler)

	body, err := json.Marshal(dto.StartExtractionRequest{
		Tables: map[string]dto.ExtractionTableRequest{"transactions": {Columns: []string{"id"}}},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+uuid.New().String()+"/extractions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assertStructuredErrorResponse(t, resp, http.StatusNotFound, "not_found", "connection not found")
}

func TestStartExtraction_InvalidBody_ReturnsStructuredError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+conn.ID.String()+"/extractions", bytes.NewReader([]byte("{")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assertStructuredErrorResponse(t, resp, http.StatusBadRequest, "invalid_request", "invalid extraction request body")
}

func TestStartExtraction_InvalidDateRange_ReturnsStructuredError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	fixture.schemaRepo.schemas[conn.ID] = []*entities.DiscoveredSchema{{
		ID:           uuid.New(),
		ConnectionID: conn.ID,
		TableName:    "transactions",
		Columns:      []entities.ColumnInfo{{Name: "id", Type: "uuid"}},
	}}
	app := setupTestApp(t, fixture.handler)

	body, err := json.Marshal(dto.StartExtractionRequest{
		Tables:    map[string]dto.ExtractionTableRequest{"transactions": {Columns: []string{"id"}}},
		StartDate: "2026-03-10",
		EndDate:   "2026-03-01",
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+conn.ID.String()+"/extractions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assertStructuredErrorResponse(t, resp, http.StatusBadRequest, "invalid_request", "invalid extraction request: end date must be on or after start date")
}

func TestGetExtraction_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	extraction := fixture.seedExtraction(t, conn.ID)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/"+extraction.ID.String(), nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response dto.ExtractionRequestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, extraction.ID, response.ID)
	assert.Equal(t, conn.ID, response.ConnectionID)
	assert.Equal(t, extraction.StartDate, response.StartDate)
	assert.Equal(t, extraction.EndDate, response.EndDate)
	assert.Equal(t, extraction.Status.String(), response.Status)
}

func TestPollExtraction_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	extraction := fixture.seedExtraction(t, conn.ID)
	fixture.fetcherMock.jobStatus = &sharedPorts.ExtractionJobStatus{
		JobID:      extraction.FetcherJobID,
		Status:     "COMPLETE",
		ResultPath: "/tmp/result.csv",
	}

	app := setupTestApp(t, fixture.handler)
	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/extractions/"+extraction.ID.String()+"/poll", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response dto.ExtractionRequestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, vo.ExtractionStatusComplete.String(), response.Status)
	assert.Equal(t, 1, fixture.extractionRepo.updateCount)
}

func TestPollExtraction_FailedResponseSanitizesErrorMessage(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	extraction := fixture.seedExtraction(t, conn.ID)
	fixture.fetcherMock.jobStatus = &sharedPorts.ExtractionJobStatus{
		JobID:        extraction.FetcherJobID,
		Status:       "FAILED",
		ErrorMessage: "dial tcp 10.0.0.8:5432: connection refused",
	}

	app := setupTestApp(t, fixture.handler)
	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/extractions/"+extraction.ID.String()+"/poll", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response dto.ExtractionRequestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, vo.ExtractionStatusFailed.String(), response.Status)
	assert.Equal(t, entities.SanitizedExtractionFailureMessage, response.ErrorMessage)
	assert.Equal(t, 1, fixture.extractionRepo.updateCount)
}

func TestPollExtraction_InvalidID_ReturnsStructuredError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/extractions/not-a-uuid/poll", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)
	assertStructuredErrorResponse(t, resp, http.StatusBadRequest, "invalid_request", "invalid extraction ID")
}

func TestPollExtraction_FetcherUnavailable_Returns503(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	extraction := fixture.seedExtraction(t, conn.ID)
	fixture.fetcherMock.jobStatusErr = sharedPorts.ErrFetcherUnavailable

	app := setupTestApp(t, fixture.handler)
	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/extractions/"+extraction.ID.String()+"/poll", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)
	assertStructuredErrorResponse(t, resp, http.StatusServiceUnavailable, "discovery_fetcher_unavailable", "fetcher service unavailable")
}
