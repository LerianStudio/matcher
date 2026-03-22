//go:build unit

package ports

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractionParams_ZeroValue(t *testing.T) {
	t.Parallel()

	var params ExtractionParams

	assert.Empty(t, params.StartDate)
	assert.Empty(t, params.EndDate)
	assert.Nil(t, params.Filters)
}

func TestExtractionParams_WithValues(t *testing.T) {
	t.Parallel()

	params := ExtractionParams{
		StartDate: "2024-01-01",
		EndDate:   "2024-12-31",
		Filters:   &ExtractionFilters{Equals: map[string]string{"key": "value"}},
	}

	assert.Equal(t, "2024-01-01", params.StartDate)
	assert.Equal(t, "2024-12-31", params.EndDate)
	require.NotNil(t, params.Filters)
	assert.Equal(t, "value", params.Filters.Equals["key"])
}

func TestExtractionParams_EmptyFilters(t *testing.T) {
	t.Parallel()

	params := ExtractionParams{
		StartDate: "2024-06-01",
		EndDate:   "2024-06-30",
		Filters:   &ExtractionFilters{Equals: map[string]string{}},
	}

	assert.NotNil(t, params.Filters)
	assert.Empty(t, params.Filters.Equals)
}

func TestExtractionFilters_UnmarshalJSON_RejectsUnknownKeys(t *testing.T) {
	t.Parallel()

	var filters ExtractionFilters
	err := json.Unmarshal([]byte(`{"unsupported":"value"}`), &filters)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFilters_UnmarshalJSON_RejectsNonStringEqualsValues(t *testing.T) {
	t.Parallel()

	var filters ExtractionFilters
	err := json.Unmarshal([]byte(`{"equals":{"currency":123}}`), &filters)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFilters_RoundTripMapConversion(t *testing.T) {
	t.Parallel()

	original := &ExtractionFilters{Equals: map[string]string{"currency": "USD"}}
	raw := original.ToMap()
	require.NotNil(t, raw)

	converted, err := ExtractionFiltersFromMap(raw)
	require.NoError(t, err)
	require.NotNil(t, converted)
	assert.Equal(t, original.Equals, converted.Equals)
}
