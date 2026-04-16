// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlattenFetcherJSON_EmptyObject_ProducesEmptyArray(t *testing.T) {
	t.Parallel()

	out, err := FlattenFetcherJSON(strings.NewReader(`{}`), 0)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)
	require.JSONEq(t, `[]`, string(data))
}

func TestFlattenFetcherJSON_SingleDatasourceSingleTable_PreservesRows(t *testing.T) {
	t.Parallel()

	input := `{"ds-1":{"table-1":[{"id":"a","v":1},{"id":"b","v":2}]}}`

	out, err := FlattenFetcherJSON(strings.NewReader(input), 0)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(data, &rows))
	require.Len(t, rows, 2)
	require.Equal(t, "a", rows[0]["id"])
	require.Equal(t, "b", rows[1]["id"])
}

func TestFlattenFetcherJSON_MultiDatasourceMultiTable_OrdersDeterministically(t *testing.T) {
	t.Parallel()

	// Source order intentionally scrambles datasource and table keys to
	// prove the flattener imposes lexicographic ordering.
	input := `{
		"ds-z": {"table-b": [{"k":"zb1"}], "table-a": [{"k":"za1"}]},
		"ds-a": {"table-b": [{"k":"ab1"},{"k":"ab2"}], "table-a": [{"k":"aa1"}]}
	}`

	out, err := FlattenFetcherJSON(strings.NewReader(input), 0)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(data, &rows))

	// Expected order: ds-a/table-a, ds-a/table-b (2 rows, original order),
	// ds-z/table-a, ds-z/table-b.
	require.Len(t, rows, 5)
	require.Equal(t, "aa1", rows[0]["k"])
	require.Equal(t, "ab1", rows[1]["k"])
	require.Equal(t, "ab2", rows[2]["k"])
	require.Equal(t, "za1", rows[3]["k"])
	require.Equal(t, "zb1", rows[4]["k"])
}

func TestFlattenFetcherJSON_RowValuesPreservedVerbatim(t *testing.T) {
	t.Parallel()

	// Include nested objects, arrays, numbers and nulls to prove we never
	// re-marshal rows (which would reorder keys or canonicalise numbers).
	input := `{"ds":{"t":[{"nested":{"x":1,"y":[1,2,3]},"n":null,"f":3.14}]}}`

	out, err := FlattenFetcherJSON(strings.NewReader(input), 0)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)

	// Row bytes must appear inside the flattened array exactly as encoded
	// in the source payload (between [ and ]).
	trimmed := bytes.TrimSpace(data)
	require.Equal(t, byte('['), trimmed[0])
	require.Equal(t, byte(']'), trimmed[len(trimmed)-1])

	innerRow := `{"nested":{"x":1,"y":[1,2,3]},"n":null,"f":3.14}`
	require.Contains(t, string(trimmed), innerRow)
}

func TestFlattenFetcherJSON_NilReader_ReturnsMalformedError(t *testing.T) {
	t.Parallel()

	out, err := FlattenFetcherJSON(nil, 0)
	require.Nil(t, out)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFetcherShapeMalformed))
}

func TestFlattenFetcherJSON_MalformedJSON_ReturnsMalformedError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input string
	}{
		{"root is array", `[1,2,3]`},
		{"truncated object", `{"ds":{"t":[{"a":1}`},
		{"datasource value not object", `{"ds":"not-an-object"}`},
		{"table value not array", `{"ds":{"t":"not-an-array"}}`},
		{"row is not object or primitive valid", `{"ds":{"t":[{"a":}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := FlattenFetcherJSON(strings.NewReader(tc.input), 0)
			require.Nil(t, out)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrFetcherShapeMalformed),
				"expected ErrFetcherShapeMalformed, got: %v", err)
		})
	}
}

func TestFlattenFetcherJSON_EmptyTable_ProducesNoRows(t *testing.T) {
	t.Parallel()

	out, err := FlattenFetcherJSON(strings.NewReader(`{"ds":{"t":[]}}`), 0)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)
	require.JSONEq(t, `[]`, string(data))
}
