// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetcherFilterCondition_Marshal_EqOnly(t *testing.T) {
	t.Parallel()

	fc := fetcherFilterCondition{Eq: []any{"USD", "EUR"}}

	data, err := json.Marshal(fc)

	require.NoError(t, err)
	assert.JSONEq(t, `{"eq":["USD","EUR"]}`, string(data))
}

func TestFetcherFilterCondition_Marshal_MultipleOperators(t *testing.T) {
	t.Parallel()

	fc := fetcherFilterCondition{
		Eq:      []any{"active"},
		Gt:      []any{100},
		Between: []any{"2026-01-01", "2026-12-31"},
		In:      []any{"USD", "EUR", "GBP"},
	}

	data, err := json.Marshal(fc)

	require.NoError(t, err)

	var roundTrip fetcherFilterCondition

	err = json.Unmarshal(data, &roundTrip)

	require.NoError(t, err)
	assert.Equal(t, fc.Eq, roundTrip.Eq)
	assert.Len(t, roundTrip.Gt, 1)
	assert.Equal(t, fc.Between, roundTrip.Between)
	assert.Equal(t, fc.In, roundTrip.In)
	assert.Nil(t, roundTrip.Lt)
	assert.Nil(t, roundTrip.Lte)
	assert.Nil(t, roundTrip.Gte)
	assert.Nil(t, roundTrip.Nin)
	assert.Nil(t, roundTrip.Ne)
	assert.Nil(t, roundTrip.Like)
}

func TestFetcherFilterCondition_Marshal_EmptyOmitsAllFields(t *testing.T) {
	t.Parallel()

	fc := fetcherFilterCondition{}

	data, err := json.Marshal(fc)

	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(data))
}

func TestFetcherFilterCondition_Unmarshal_FromFetcherWire(t *testing.T) {
	t.Parallel()

	raw := `{
		"eq": ["USD"],
		"gt": [100],
		"gte": [50],
		"lt": [1000],
		"lte": [999],
		"between": ["2026-01-01", "2026-12-31"],
		"in": ["a", "b"],
		"nin": ["c"],
		"ne": ["deleted"],
		"like": ["%pattern%"]
	}`

	var fc fetcherFilterCondition

	err := json.Unmarshal([]byte(raw), &fc)

	require.NoError(t, err)
	assert.Equal(t, []any{"USD"}, fc.Eq)
	assert.Len(t, fc.Gt, 1)
	assert.Len(t, fc.Gte, 1)
	assert.Len(t, fc.Lt, 1)
	assert.Len(t, fc.Lte, 1)
	assert.Equal(t, []any{"2026-01-01", "2026-12-31"}, fc.Between)
	assert.Equal(t, []any{"a", "b"}, fc.In)
	assert.Equal(t, []any{"c"}, fc.Nin)
	assert.Equal(t, []any{"deleted"}, fc.Ne)
	assert.Equal(t, []any{"%pattern%"}, fc.Like)
}

func TestFetcherFilterCondition_RoundTrip(t *testing.T) {
	t.Parallel()

	original := fetcherFilterCondition{
		Eq:  []any{"USD"},
		Gt:  []any{float64(100)},
		In:  []any{"a", "b", "c"},
		Ne:  []any{"deleted"},
		Lte: []any{float64(9999)},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored fetcherFilterCondition

	err = json.Unmarshal(data, &restored)

	require.NoError(t, err)
	assert.Equal(t, original.Eq, restored.Eq)
	assert.Equal(t, original.Gt, restored.Gt)
	assert.Equal(t, original.In, restored.In)
	assert.Equal(t, original.Ne, restored.Ne)
	assert.Equal(t, original.Lte, restored.Lte)
	assert.Nil(t, restored.Lt)
	assert.Nil(t, restored.Gte)
	assert.Nil(t, restored.Between)
	assert.Nil(t, restored.Nin)
	assert.Nil(t, restored.Like)
}

func TestFilterConditionFromMap_AllOperators(t *testing.T) {
	t.Parallel()

	m := map[string]any{
		"eq":   []any{"USD"},
		"gt":   []any{float64(100)},
		"gte":  []any{float64(50)},
		"lt":   []any{float64(1000)},
		"lte":  []any{float64(999)},
		"in":   []any{"a", "b"},
		"nin":  []any{"c"},
		"ne":   []any{"deleted"},
		"like": []any{"%x%"},
	}

	fc := filterConditionFromMap(m)

	assert.Equal(t, []any{"USD"}, fc.Eq)
	assert.Equal(t, []any{float64(100)}, fc.Gt)
	assert.Equal(t, []any{float64(50)}, fc.Gte)
	assert.Equal(t, []any{float64(1000)}, fc.Lt)
	assert.Equal(t, []any{float64(999)}, fc.Lte)
	assert.Equal(t, []any{"a", "b"}, fc.In)
	assert.Equal(t, []any{"c"}, fc.Nin)
	assert.Equal(t, []any{"deleted"}, fc.Ne)
	assert.Equal(t, []any{"%x%"}, fc.Like)
}

func TestFilterConditionFromMap_IgnoresUnknownKeys(t *testing.T) {
	t.Parallel()

	m := map[string]any{
		"eq":       []any{"val"},
		"futurism": []any{"x"},
	}

	fc := filterConditionFromMap(m)

	assert.Equal(t, []any{"val"}, fc.Eq)
	assert.Nil(t, fc.Gt)
}

func TestFilterConditionFromMap_NormalizesScalarValues(t *testing.T) {
	t.Parallel()

	m := map[string]any{
		"eq": "not-a-slice",
		"gt": float64(42),
	}

	fc := filterConditionFromMap(m)

	assert.Equal(t, []any{"not-a-slice"}, fc.Eq)
	assert.Equal(t, []any{float64(42)}, fc.Gt)
}

func TestFilterConditionFromMap_NormalizesTypedSlices(t *testing.T) {
	t.Parallel()

	m := map[string]any{
		"eq":  []string{"USD"},
		"in":  []int{1, 2},
		"gte": []int64{10},
	}

	restored := filterConditionFromMap(m)

	assert.Equal(t, []any{"USD"}, restored.Eq)
	assert.Equal(t, []any{1, 2}, restored.In)
	assert.Equal(t, []any{int64(10)}, restored.Gte)
	assert.Nil(t, restored.Gt)
	assert.Nil(t, restored.Lt)
}
