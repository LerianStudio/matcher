// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fetcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

// ErrFetcherShapeMalformed indicates the Fetcher extraction payload could not
// be decoded as the expected nested {datasource: {table: [rows]}} shape.
var ErrFetcherShapeMalformed = errors.New("malformed fetcher extraction payload")

// FlattenFetcherJSON parses the Fetcher-shaped nested object
//
//	{"datasource-1":{"table-1":[{row},...], "table-2":[{row}]}, ...}
//
// and re-emits a flat JSON array of rows that Matcher's generic JSON parser
// consumes. The implementation materialises the full extraction in memory once
// (as a nested map for deterministic lexicographic ordering) and returns a
// rewound buffer — callers MUST size-bound their Fetcher payloads upstream
// (see FETCHER_MAX_EXTRACTION_BYTES config landing in T-003).
//
// Invariants:
//   - datasource keys are iterated in lexicographic order
//   - table keys within a datasource are iterated in lexicographic order
//   - rows preserve their source order within each table
//   - an empty extraction `{}` produces an empty flat array `[]` (not an error)
//   - row values are copied verbatim; no coercion or transformation happens
func FlattenFetcherJSON(in io.Reader) (io.Reader, error) {
	if in == nil {
		return nil, ErrFetcherShapeMalformed
	}

	decoder := json.NewDecoder(in)

	tok, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("%w: reading opening token: %w", ErrFetcherShapeMalformed, err)
	}

	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("%w: expected root object, got %v", ErrFetcherShapeMalformed, tok)
	}

	datasources, err := decodeDatasources(decoder)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := writeFlatArray(&buf, datasources); err != nil {
		return nil, err
	}

	return &buf, nil
}

// datasourceRows captures a single datasource's tables as a map from table
// name to its raw row messages. Using json.RawMessage preserves byte-for-byte
// row fidelity and avoids a second unmarshal pass.
type datasourceRows map[string][]json.RawMessage

// decodeDatasources reads the datasource-level object stream until the
// matching closing brace, decoding each datasource into its own table map.
func decodeDatasources(decoder *json.Decoder) (map[string]datasourceRows, error) {
	datasources := make(map[string]datasourceRows)

	for decoder.More() {
		dsKey, err := readStringKey(decoder, "datasource key")
		if err != nil {
			return nil, err
		}

		tables, err := decodeTables(decoder)
		if err != nil {
			return nil, fmt.Errorf("%w: datasource %q: %w", ErrFetcherShapeMalformed, dsKey, err)
		}

		datasources[dsKey] = tables
	}

	if err := expectClosingDelim(decoder, '}', "root"); err != nil {
		return nil, err
	}

	return datasources, nil
}

// decodeTables reads one datasource's {table: [rows]} object into a map.
func decodeTables(decoder *json.Decoder) (datasourceRows, error) {
	if err := expectOpeningDelim(decoder, '{', "datasource value"); err != nil {
		return nil, err
	}

	tables := make(datasourceRows)

	for decoder.More() {
		tableKey, err := readStringKey(decoder, "table key")
		if err != nil {
			return nil, err
		}

		rows, err := decodeRows(decoder)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", tableKey, err)
		}

		tables[tableKey] = rows
	}

	if err := expectClosingDelim(decoder, '}', "datasource"); err != nil {
		return nil, err
	}

	return tables, nil
}

// decodeRows reads a `[row, row, ...]` array, preserving each row's raw JSON.
func decodeRows(decoder *json.Decoder) ([]json.RawMessage, error) {
	if err := expectOpeningDelim(decoder, '[', "rows"); err != nil {
		return nil, err
	}

	rows := make([]json.RawMessage, 0)

	for decoder.More() {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("decoding row: %w", err)
		}

		rows = append(rows, raw)
	}

	if err := expectClosingDelim(decoder, ']', "rows"); err != nil {
		return nil, err
	}

	return rows, nil
}

// writeFlatArray emits the collected rows as a single JSON array in the
// stable lexicographic ordering required by the domain invariants.
func writeFlatArray(buf *bytes.Buffer, datasources map[string]datasourceRows) error {
	buf.WriteByte('[')

	first := true

	for _, dsKey := range sortedKeys(datasources) {
		tables := datasources[dsKey]

		for _, tableKey := range sortedTableKeys(tables) {
			for _, row := range tables[tableKey] {
				if !first {
					buf.WriteByte(',')
				}

				if _, err := buf.Write(row); err != nil {
					return fmt.Errorf("write flattened row: %w", err)
				}

				first = false
			}
		}
	}

	buf.WriteByte(']')

	return nil
}

// sortedKeys returns the datasource keys in lexicographic order.
func sortedKeys(m map[string]datasourceRows) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// sortedTableKeys returns the table keys in lexicographic order.
func sortedTableKeys(m datasourceRows) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// readStringKey consumes the next token expecting it to be an object key
// (a string); `role` names the expected position for error messages.
func readStringKey(decoder *json.Decoder, role string) (string, error) {
	tok, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("%w: reading %s: %w", ErrFetcherShapeMalformed, role, err)
	}

	key, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("%w: expected %s string, got %v", ErrFetcherShapeMalformed, role, tok)
	}

	return key, nil
}

// expectOpeningDelim reads the next token and asserts it is the given open delimiter.
func expectOpeningDelim(decoder *json.Decoder, want json.Delim, role string) error {
	tok, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: reading %s opening: %w", ErrFetcherShapeMalformed, role, err)
	}

	delim, ok := tok.(json.Delim)
	if !ok || delim != want {
		return fmt.Errorf("%w: expected %s %q, got %v", ErrFetcherShapeMalformed, role, want, tok)
	}

	return nil
}

// expectClosingDelim reads the next token and asserts it is the given close delimiter.
func expectClosingDelim(decoder *json.Decoder, want json.Delim, role string) error {
	tok, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: reading %s closing: %w", ErrFetcherShapeMalformed, role, err)
	}

	delim, ok := tok.(json.Delim)
	if !ok || delim != want {
		return fmt.Errorf("%w: expected %s closing %q, got %v", ErrFetcherShapeMalformed, role, want, tok)
	}

	return nil
}
