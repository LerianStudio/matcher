// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

// fetcherShape is the generator payload: a deterministic, well-typed slice
// of datasources so quick.Check sees structure (not opaque strings) when
// shrinking. Keeping it as a slice-of-structs (instead of a map) means the
// shape is reproducible in corpora and diff-friendly on failure output.
type fetcherShape struct {
	Datasources []datasourceShape
}

type datasourceShape struct {
	Key    string
	Tables []tableShape
}

type tableShape struct {
	Key  string
	Rows []rowShape
}

// rowShape uses a small named-field payload so marshalled rows remain
// compact and the per-row identity survives a roundtrip through
// encoding/json with byte-stable output.
type rowShape struct {
	N     int    `json:"n"`
	DS    string `json:"ds,omitempty"`
	Table string `json:"table,omitempty"`
	Order int    `json:"order,omitempty"`
}

// Generate implements quick.Generator, producing shapes bounded at 0–4
// datasources, 0–3 tables, 0–5 rows. Unique keys per level (datasources
// and tables) are enforced because the production shape is a nested map
// where duplicate keys would be collapsed by the decoder anyway.
func (fetcherShape) Generate(rand *rand.Rand, _ int) reflect.Value {
	dsCount := rand.Intn(5) // 0–4
	dsKeys := uniqueKeys(rand, dsCount, "ds")

	ds := make([]datasourceShape, 0, dsCount)

	for _, dsKey := range dsKeys {
		tableCount := rand.Intn(4) // 0–3
		tableKeys := uniqueKeys(rand, tableCount, "t")

		tables := make([]tableShape, 0, tableCount)

		for _, tk := range tableKeys {
			rowCount := rand.Intn(6) // 0–5
			rows := make([]rowShape, 0, rowCount)

			for i := 0; i < rowCount; i++ {
				rows = append(rows, rowShape{N: i})
			}

			tables = append(tables, tableShape{Key: tk, Rows: rows})
		}

		ds = append(ds, datasourceShape{Key: dsKey, Tables: tables})
	}

	return reflect.ValueOf(fetcherShape{Datasources: ds})
}

// uniqueKeys picks N distinct short keys with a stable prefix. The key
// space is large enough (base36, 4 chars) to make a collision between
// generated keys vanishingly unlikely; the set guarantees distinctness
// regardless.
func uniqueKeys(rand *rand.Rand, n int, prefix string) []string {
	keys := make(map[string]struct{}, n)
	out := make([]string, 0, n)

	for len(out) < n {
		k := fmt.Sprintf("%s-%04x", prefix, rand.Intn(1<<16))
		if _, dup := keys[k]; dup {
			continue
		}

		keys[k] = struct{}{}

		out = append(out, k)
	}

	return out
}

// marshalShape emits the shape in the exact nested {ds:{table:[rows]}}
// JSON the flattener expects. Using encoding/json with a map guarantees
// the shape can be re-decoded by the function under test. Iteration
// order of Go maps is randomised by the runtime — this is a feature
// here: it stress-tests the lexicographic-sort invariants.
func marshalShape(s fetcherShape) ([]byte, int) {
	nested := make(map[string]map[string][]rowShape, len(s.Datasources))
	total := 0

	for _, ds := range s.Datasources {
		tables := make(map[string][]rowShape, len(ds.Tables))

		for _, t := range ds.Tables {
			tables[t.Key] = t.Rows
			total += len(t.Rows)
		}

		nested[ds.Key] = tables
	}

	raw, err := json.Marshal(nested)
	if err != nil {
		panic(err) // unreachable: rowShape is JSON-safe by construction
	}

	return raw, total
}

// flattenAndDecode runs the function under test and decodes its output
// into a slice of raw messages so tests can assert on per-row bytes.
func flattenAndDecode(t *testing.T, raw []byte) []json.RawMessage {
	t.Helper()

	out, err := FlattenFetcherJSON(bytes.NewReader(raw), 0)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)

	var rows []json.RawMessage

	require.NoError(t, json.Unmarshal(data, &rows))

	return rows
}

// TestProperty_FlattenFetcherJSON_RowCountPreservation asserts Prop 1:
// len(flatten(input)) == sum of rows across all tables in all datasources.
func TestProperty_FlattenFetcherJSON_RowCountPreservation(t *testing.T) {
	t.Parallel()

	prop := func(s fetcherShape) bool {
		raw, expected := marshalShape(s)

		out, err := FlattenFetcherJSON(bytes.NewReader(raw), 0)
		if err != nil {
			return false
		}

		data, readErr := io.ReadAll(out)
		if readErr != nil {
			return false
		}

		var rows []json.RawMessage
		if err := json.Unmarshal(data, &rows); err != nil {
			return false
		}

		return len(rows) == expected
	}

	require.NoError(t, quick.Check(prop, nil))
}

// TestProperty_FlattenFetcherJSON_DatasourceLexicographicOrder asserts
// Prop 2: datasources emit in lexicographic order. Each datasource has
// one table with one row encoding the datasource key; the decoded "ds"
// field sequence must match sort.Strings on datasource keys.
func TestProperty_FlattenFetcherJSON_DatasourceLexicographicOrder(t *testing.T) {
	t.Parallel()

	prop := func(rawKeys []string) bool {
		keys := sanitizedUniqueKeys(rawKeys, 8)
		if len(keys) < 2 {
			return true // not enough to order; trivially passes
		}

		nested := make(map[string]map[string][]rowShape, len(keys))
		for _, k := range keys {
			nested[k] = map[string][]rowShape{
				"only": {{N: 0, DS: k}},
			}
		}

		raw, err := json.Marshal(nested)
		if err != nil {
			return false
		}

		rows := flattenAndDecode(t, raw)
		if len(rows) != len(keys) {
			return false
		}

		sorted := append([]string(nil), keys...)
		sort.Strings(sorted)

		for i, r := range rows {
			var row rowShape
			if err := json.Unmarshal(r, &row); err != nil {
				return false
			}

			if row.DS != sorted[i] {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(prop, nil))
}

// TestProperty_FlattenFetcherJSON_TableLexicographicOrder asserts Prop 3:
// within a single datasource, tables emit in table-key-sorted order.
func TestProperty_FlattenFetcherJSON_TableLexicographicOrder(t *testing.T) {
	t.Parallel()

	prop := func(rawKeys []string) bool {
		keys := sanitizedUniqueKeys(rawKeys, 8)
		if len(keys) < 2 {
			return true
		}

		tables := make(map[string][]rowShape, len(keys))
		for _, k := range keys {
			tables[k] = []rowShape{{N: 0, Table: k}}
		}

		raw, err := json.Marshal(map[string]map[string][]rowShape{
			"single-ds": tables,
		})
		if err != nil {
			return false
		}

		rows := flattenAndDecode(t, raw)
		if len(rows) != len(keys) {
			return false
		}

		sorted := append([]string(nil), keys...)
		sort.Strings(sorted)

		for i, r := range rows {
			var row rowShape
			if err := json.Unmarshal(r, &row); err != nil {
				return false
			}

			if row.Table != sorted[i] {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(prop, nil))
}

// TestProperty_FlattenFetcherJSON_RowOrderPreservedWithinTable asserts
// Prop 4: rows within a single table emit in source order [0..N-1].
func TestProperty_FlattenFetcherJSON_RowOrderPreservedWithinTable(t *testing.T) {
	t.Parallel()

	prop := func(n uint8) bool {
		count := int(n) % 32 // bound to keep test fast; invariant is size-independent
		rows := make([]rowShape, count)

		for i := range rows {
			rows[i] = rowShape{Order: i, N: i}
		}

		raw, err := json.Marshal(map[string]map[string][]rowShape{
			"ds": {"t": rows},
		})
		if err != nil {
			return false
		}

		decoded := flattenAndDecode(t, raw)
		if len(decoded) != count {
			return false
		}

		for i, r := range decoded {
			var row rowShape
			if err := json.Unmarshal(r, &row); err != nil {
				return false
			}

			if row.Order != i {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(prop, nil))
}

// TestProperty_FlattenFetcherJSON_VerbatimRowPreservation asserts Prop 5:
// each emitted row is semantically equal to its source. We compare via
// canonical JSON (marshal-then-compare) to avoid false negatives from
// whitespace or key-order differences introduced by the decoder.
func TestProperty_FlattenFetcherJSON_VerbatimRowPreservation(t *testing.T) {
	t.Parallel()

	prop := func(s fetcherShape) bool {
		raw, _ := marshalShape(s)

		decoded := flattenAndDecode(t, raw)

		// Build the expected sequence in the same lexicographic order the
		// flattener emits: sort ds, then sort tables, then preserve row order.
		dsSorted := append([]datasourceShape(nil), s.Datasources...)
		sort.Slice(dsSorted, func(i, j int) bool { return dsSorted[i].Key < dsSorted[j].Key })

		cursor := 0

		for _, ds := range dsSorted {
			ts := append([]tableShape(nil), ds.Tables...)
			sort.Slice(ts, func(i, j int) bool { return ts[i].Key < ts[j].Key })

			for _, t := range ts {
				for _, row := range t.Rows {
					if cursor >= len(decoded) {
						return false
					}

					expected, err := json.Marshal(row)
					if err != nil {
						return false
					}

					if !jsonSemanticEqual(expected, decoded[cursor]) {
						return false
					}

					cursor++
				}
			}
		}

		return cursor == len(decoded)
	}

	require.NoError(t, quick.Check(prop, nil))
}

// TestProperty_FlattenFetcherJSON_EmptyIdentity is Prop 6: `{}` → `[]`
// with no error. Encoded as a standalone subtest in the property suite
// so a human reviewer can find it adjacent to the property list.
func TestProperty_FlattenFetcherJSON_EmptyIdentity(t *testing.T) {
	t.Parallel()

	out, err := FlattenFetcherJSON(strings.NewReader(`{}`), 0)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)
	require.JSONEq(t, `[]`, string(data))
}

// sanitizedUniqueKeys trims, filters empty, and uniques the generated key
// slice. Caps at maxN to keep quick.Check iterations fast.
func sanitizedUniqueKeys(in []string, maxN int) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))

	for _, k := range in {
		// Reject empty or structural-char keys to keep JSON marshalling
		// well-formed without overconstraining the generator.
		trimmed := strings.TrimSpace(k)
		if trimmed == "" {
			continue
		}

		if _, dup := seen[trimmed]; dup {
			continue
		}

		seen[trimmed] = struct{}{}

		out = append(out, trimmed)

		if len(out) >= maxN {
			break
		}
	}

	return out
}

// jsonSemanticEqual compares two JSON payloads by decoding to generic
// values, which ignores key order and whitespace and captures the
// "same keys, same values" intent of verbatim preservation.
func jsonSemanticEqual(a, b []byte) bool {
	var va, vb any

	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}

	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}

	return reflect.DeepEqual(va, vb)
}
