// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

func TestExtractionFilters_UnmarshalJSON_NullInput(t *testing.T) {
	t.Parallel()

	var filters ExtractionFilters
	err := json.Unmarshal([]byte("null"), &filters)

	require.NoError(t, err)
	assert.Nil(t, filters.Equals)
}

func TestExtractionFilters_UnmarshalJSON_EmptyData(t *testing.T) {
	t.Parallel()

	var filters ExtractionFilters
	err := json.Unmarshal([]byte(""), &filters)

	// Empty input is rejected by Go's JSON decoder before reaching UnmarshalJSON.
	require.Error(t, err)
}

func TestExtractionFilters_ToMap_NilReceiver(t *testing.T) {
	t.Parallel()

	var filters *ExtractionFilters

	result := filters.ToMap()

	assert.Nil(t, result)
}

func TestExtractionFilters_UnmarshalJSON_WhitespaceKeysTrimmed(t *testing.T) {
	t.Parallel()

	var filters ExtractionFilters
	err := json.Unmarshal([]byte(`{"equals":{"  currency  ":"  USD  "}}`), &filters)

	require.NoError(t, err)
	assert.Equal(t, "USD", filters.Equals["currency"])
	assert.Empty(t, filters.Equals["  currency  "])
}

func TestExtractionFilters_UnmarshalJSON_BlankKeyRejected(t *testing.T) {
	t.Parallel()

	var filters ExtractionFilters
	err := json.Unmarshal([]byte(`{"equals":{"  ":"value"}}`), &filters)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFilters_ToMap_ProducesFetcherWireFormat(t *testing.T) {
	t.Parallel()

	filters := &ExtractionFilters{Equals: map[string]string{"currency": "USD", "status": "active"}}
	result := filters.ToMap()

	require.NotNil(t, result)
	assert.Len(t, result, 2)

	// Each column maps to {"eq": [value]}, matching Fetcher's FilterCondition struct.
	currencyCond, ok := result["currency"].(map[string]any)
	require.True(t, ok, "currency condition must be a map")
	assert.Equal(t, []any{"USD"}, currencyCond["eq"])

	statusCond, ok := result["status"].(map[string]any)
	require.True(t, ok, "status condition must be a map")
	assert.Equal(t, []any{"active"}, statusCond["eq"])
}

func TestExtractionFilters_ToMap_EmptyEquals(t *testing.T) {
	t.Parallel()

	filters := &ExtractionFilters{Equals: map[string]string{}}
	result := filters.ToMap()

	assert.Nil(t, result)
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

func TestExtractionFiltersFromMap_MissingOperator(t *testing.T) {
	t.Parallel()

	raw := map[string]any{"currency": map[string]any{"gt": []any{"100"}}}
	_, err := ExtractionFiltersFromMap(raw)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFiltersFromMap_NonSliceOperator(t *testing.T) {
	t.Parallel()

	raw := map[string]any{"currency": map[string]any{"eq": "USD"}}
	_, err := ExtractionFiltersFromMap(raw)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFiltersFromMap_EmptySlice(t *testing.T) {
	t.Parallel()

	raw := map[string]any{"currency": map[string]any{"eq": []any{}}}
	_, err := ExtractionFiltersFromMap(raw)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFiltersFromMap_NonStringValue(t *testing.T) {
	t.Parallel()

	raw := map[string]any{"currency": map[string]any{"eq": []any{123}}}
	_, err := ExtractionFiltersFromMap(raw)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFiltersFromMap_NotAnObject(t *testing.T) {
	t.Parallel()

	raw := map[string]any{"currency": "USD"}
	_, err := ExtractionFiltersFromMap(raw)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidExtractionFilters)
}

func TestExtractionFiltersFromMap_LegacyEqualsFormat(t *testing.T) {
	t.Parallel()

	t.Run("legacy single value", func(t *testing.T) {
		t.Parallel()

		raw := map[string]any{"equals": map[string]any{"currency": "USD"}}
		result, err := ExtractionFiltersFromMap(raw)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "USD", result.Equals["currency"])
	})

	t.Run("legacy multi value takes first", func(t *testing.T) {
		t.Parallel()

		raw := map[string]any{"equals": map[string]any{"currency": []any{"USD", "EUR"}}}
		result, err := ExtractionFiltersFromMap(raw)

		require.NoError(t, err)
		require.NotNil(t, result)
		// Single-value model: takes first string from array
		assert.Equal(t, "USD", result.Equals["currency"])
	})

	t.Run("new format still works", func(t *testing.T) {
		t.Parallel()

		raw := map[string]any{"currency": map[string]any{"eq": []any{"USD"}}}
		result, err := ExtractionFiltersFromMap(raw)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "USD", result.Equals["currency"])
	})

	t.Run("ambiguous case treated as new format", func(t *testing.T) {
		t.Parallel()

		// More than one top-level key including "equals" => new format (column names)
		raw := map[string]any{
			"equals": map[string]any{"eq": []any{"active"}},
			"status": map[string]any{"eq": []any{"pending"}},
		}
		result, err := ExtractionFiltersFromMap(raw)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "active", result.Equals["equals"])
		assert.Equal(t, "pending", result.Equals["status"])
	})

	t.Run("legacy empty map returns nil", func(t *testing.T) {
		t.Parallel()

		raw := map[string]any{"equals": map[string]any{}}
		result, err := ExtractionFiltersFromMap(raw)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("legacy blank keys skipped", func(t *testing.T) {
		t.Parallel()

		raw := map[string]any{"equals": map[string]any{"  ": "USD", "currency": "EUR"}}
		result, err := ExtractionFiltersFromMap(raw)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "EUR", result.Equals["currency"])
		assert.Len(t, result.Equals, 1)
	})

	t.Run("legacy non-string values skipped", func(t *testing.T) {
		t.Parallel()

		raw := map[string]any{"equals": map[string]any{"amount": 42, "currency": "USD"}}
		result, err := ExtractionFiltersFromMap(raw)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "USD", result.Equals["currency"])
		assert.Len(t, result.Equals, 1)
	})
}
