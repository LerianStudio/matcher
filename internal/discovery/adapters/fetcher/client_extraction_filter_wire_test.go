//go:build unit

package fetcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestSubmitExtractionJob_TypedFilterConditions_WireFormat(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]json.RawMessage
		err := json.NewDecoder(r.Body).Decode(&raw)
		require.NoError(t, err)

		var dataReq map[string]json.RawMessage
		err = json.Unmarshal(raw["dataRequest"], &dataReq)
		require.NoError(t, err)

		expectedFilters := `{"prod-db":{"transactions":{"status":{"eq":["active"]},"amount":{"gt":[100],"lte":[9999]}}}}`
		assert.JSONEq(t, expectedFilters, string(dataReq["filters"]))

		resp := fetcherExtractionSubmitResponse{JobID: "job-typed"}
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)

	input := sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{
			"prod-db": {"transactions": {"id", "amount", "status"}},
		},
		Filters: map[string]map[string]map[string]any{
			"prod-db": {
				"transactions": {
					"status": map[string]any{"eq": []any{"active"}},
					"amount": map[string]any{"gt": []any{float64(100)}, "lte": []any{float64(9999)}},
				},
			},
		},
		Metadata: map[string]any{"source": "src-typed"},
	}

	jobID, err := client.SubmitExtractionJob(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, "job-typed", jobID)
}

func TestConvertPortFiltersToTypedFilters_NormalizesTypedValues(t *testing.T) {
	t.Parallel()

	originalPortFilters := map[string]map[string]map[string]any{
		"prod-db": {
			"transactions": {
				"currency": map[string]any{"eq": []string{"USD", "EUR"}},
				"amount":   map[string]any{"gt": []int{100}, "lte": []int64{9999}},
				"status":   "active",
			},
		},
	}

	typed := convertPortFiltersToTypedFilters(originalPortFilters)

	require.Contains(t, typed, "prod-db")
	require.Contains(t, typed["prod-db"], "transactions")

	currencyTyped := typed["prod-db"]["transactions"]["currency"]
	assert.Equal(t, []any{"USD", "EUR"}, currencyTyped.Eq)

	amountTyped := typed["prod-db"]["transactions"]["amount"]
	assert.Equal(t, []any{100}, amountTyped.Gt)
	assert.Equal(t, []any{int64(9999)}, amountTyped.Lte)

	statusTyped := typed["prod-db"]["transactions"]["status"]
	assert.Equal(t, []any{"active"}, statusTyped.Eq)
}

func TestConvertPortFiltersToTypedFilters_NilInput(t *testing.T) {
	t.Parallel()

	assert.Nil(t, convertPortFiltersToTypedFilters(nil))
}
