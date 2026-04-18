//go:build unit

package fetcher

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetcherExtractionSubmitRequest_Marshal(t *testing.T) {
	t.Parallel()

	req := fetcherExtractionSubmitRequest{
		DataRequest: fetcherDataRequest{
			MappedFields: map[string]map[string][]string{
				"prod-db": {
					"transactions": {"id", "amount"},
				},
			},
			Filters: map[string]map[string]map[string]fetcherFilterCondition{
				"prod-db": {
					"transactions": {
						"currency": {Eq: []any{"USD"}},
					},
				},
			},
		},
		Metadata: map[string]any{"source": "src-1"},
	}

	data, err := json.Marshal(req)

	require.NoError(t, err)

	var roundTrip fetcherExtractionSubmitRequest

	err = json.Unmarshal(data, &roundTrip)

	require.NoError(t, err)
	require.Contains(t, roundTrip.DataRequest.MappedFields, "prod-db")
	assert.Equal(t, []string{"id", "amount"}, roundTrip.DataRequest.MappedFields["prod-db"]["transactions"])
	assert.Equal(t, "src-1", roundTrip.Metadata["source"])

	require.Contains(t, roundTrip.DataRequest.Filters, "prod-db")
	require.Contains(t, roundTrip.DataRequest.Filters["prod-db"], "transactions")
	assert.Equal(t, []any{"USD"}, roundTrip.DataRequest.Filters["prod-db"]["transactions"]["currency"].Eq)
}

func TestFetcherExtractionSubmitRequest_OmitsEmptyFilters(t *testing.T) {
	t.Parallel()

	req := fetcherExtractionSubmitRequest{
		DataRequest: fetcherDataRequest{
			MappedFields: map[string]map[string][]string{},
		},
		Metadata: map[string]any{"source": "src-1"},
	}

	data, err := json.Marshal(req)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "filters")
}

func TestFetcherExtractionSubmitResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{"jobId": "job-12345"}`

	var resp fetcherExtractionSubmitResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "job-12345", resp.JobID)
}

func TestFetcherExtractionStatusResponse_Unmarshal_Running(t *testing.T) {
	t.Parallel()

	raw := `{"id": "job-1", "status": "RUNNING"}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "job-1", resp.ID)
	assert.Equal(t, "RUNNING", resp.Status)
	assert.Empty(t, resp.ResultPath)
}

func TestFetcherExtractionStatusResponse_Unmarshal_Complete(t *testing.T) {
	t.Parallel()

	raw := `{"id": "job-2", "status": "COMPLETE", "resultPath": "/data/job-2.json", "resultHmac": "abc", "requestHash": "xyz"}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", resp.Status)
	assert.Equal(t, "/data/job-2.json", resp.ResultPath)
	assert.Equal(t, "abc", resp.ResultHmac)
	assert.Equal(t, "xyz", resp.RequestHash)
}

func TestFetcherExtractionStatusResponse_Unmarshal_Failed(t *testing.T) {
	t.Parallel()

	raw := `{"id": "job-3", "status": "FAILED", "metadata": {"error": "timeout"}}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "FAILED", resp.Status)
	assert.Equal(t, "timeout", resp.Metadata["error"])
	assert.Empty(t, resp.ResultPath)
}

func TestFetcherExtractionStatusResponse_OmitsEmptyOptionalFields(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{
		ID:     "job-1",
		Status: "RUNNING",
	}

	data, err := json.Marshal(resp)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "resultPath")
	assert.NotContains(t, string(data), "resultHmac")
	assert.NotContains(t, string(data), "completedAt")
}

func TestFetcherExtractionStatusResponse_Unmarshal_WithMappedFieldsEcho(t *testing.T) {
	t.Parallel()

	raw := `{
		"id": "job-echo",
		"status": "COMPLETE",
		"resultPath": "/data/job-echo.json",
		"mappedFields": {
			"prod-db": {
				"public.transactions": ["id", "amount", "currency"]
			}
		},
		"filters": {
			"prod-db": {
				"public.transactions": {
					"currency": {"eq": ["USD"]},
					"amount":   {"gt": [100], "lte": [9999]}
				}
			}
		}
	}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "job-echo", resp.ID)
	assert.Equal(t, "COMPLETE", resp.Status)
	assert.Equal(t, "/data/job-echo.json", resp.ResultPath)

	require.Contains(t, resp.MappedFields, "prod-db")
	require.Contains(t, resp.MappedFields["prod-db"], "public.transactions")
	assert.Equal(t, []string{"id", "amount", "currency"}, resp.MappedFields["prod-db"]["public.transactions"])
}

func TestFetcherExtractionStatusResponse_Unmarshal_WithoutMappedFieldsEcho(t *testing.T) {
	t.Parallel()

	raw := `{"id": "job-noecho", "status": "RUNNING"}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Nil(t, resp.MappedFields)
}

func TestFetcherExtractionStatusResponse_OmitsEmptyEchoFields(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{
		ID:     "job-1",
		Status: "RUNNING",
	}

	data, err := json.Marshal(resp)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "mappedFields")
}
