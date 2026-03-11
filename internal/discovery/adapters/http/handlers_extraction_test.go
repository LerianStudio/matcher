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
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestStartExtraction_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	fixture.fetcherMock.submitJobID = "job-start-123"

	app := setupTestApp(t, fixture.handler)
	body, err := json.Marshal(dto.StartExtractionRequest{
		Tables: map[string]any{
			"transactions": map[string]any{
				"columns": []string{"id", "amount"},
			},
		},
		StartDate: "2026-03-01",
		EndDate:   "2026-03-08",
		Filters:   map[string]any{"currency": "USD"},
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
	assert.Equal(t, "job-start-123", response.FetcherJobID)
	assert.Equal(t, vo.ExtractionStatusSubmitted.String(), response.Status)
	assert.Len(t, response.Tables, 1)
	assert.Equal(t, 1, fixture.extractionRepo.createCount)
	assert.Equal(t, 1, fixture.extractionRepo.updateCount)
}

func TestStartExtraction_ConnectionNotFound(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	fixture.fetcherMock.submitJobID = "job-start-123"
	app := setupTestApp(t, fixture.handler)

	body, err := json.Marshal(dto.StartExtractionRequest{Tables: map[string]any{"transactions": true}})
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
	assert.Equal(t, extraction.FetcherJobID, response.FetcherJobID)
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
	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/extractions/"+extraction.ID.String()+"/poll", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response dto.ExtractionRequestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, vo.ExtractionStatusComplete.String(), response.Status)
	assert.Equal(t, "/tmp/result.csv", response.ResultPath)
	assert.Equal(t, 1, fixture.extractionRepo.updateCount)
}

func TestPollExtraction_InvalidID_ReturnsStructuredError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/extractions/not-a-uuid/poll", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assertStructuredErrorResponse(t, resp, http.StatusBadRequest, "invalid_request", "invalid extraction ID")
}
