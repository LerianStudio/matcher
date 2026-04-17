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

// ErrFetcherPayloadTooLarge indicates the extraction payload exceeded the
// configured size cap (FETCHER_MAX_EXTRACTION_BYTES). Distinct from
// ErrFetcherShapeMalformed because it is a DoS-defense signal, not a shape
// violation: the bytes may be well-formed but simply too large to materialise
// in memory.
var ErrFetcherPayloadTooLarge = errors.New("fetcher extraction payload exceeds size cap")

// ErrFetcherPayloadTooDeep indicates the nested JSON exceeded the maximum
// allowed depth. Deeply nested JSON can exhaust Go's encoding/json recursion
// stack; we refuse it explicitly rather than let runtime panic on overflow.
var ErrFetcherPayloadTooDeep = errors.New("fetcher extraction payload nesting too deep")

const (
	// DefaultMaxExtractionBytes is the conservative default cap on Fetcher
	// extraction payload size: 2 GiB. Operators can tighten via
	// FETCHER_MAX_EXTRACTION_BYTES when deployments have smaller working set
	// budgets. 2 GiB matches Fetcher's S3 single-object limit for the primary
	// artifact format and leaves headroom for the orchestrator's transient
	// buffering without OOM-killing the pod.
	DefaultMaxExtractionBytes int64 = 2 << 30

	// MaxExtractionDepth is the hard cap on nesting depth inside a Fetcher
	// payload. Fetcher's emit contract is always datasource→table→row-array→
	// row→fields, so the legitimate maximum is 4 levels (counting the root
	// object). 1000 is an operations-friendly ceiling that still catches
	// depth bombs well before encoding/json runs out of stack.
	MaxExtractionDepth = 1000
)

// FlattenFetcherJSON parses the Fetcher-shaped nested object
//
//	{"datasource-1":{"table-1":[{row},...], "table-2":[{row}]}, ...}
//
// and re-emits a flat JSON array of rows that Matcher's generic JSON parser
// consumes. The implementation materialises the full extraction in memory once
// (as a nested map for deterministic lexicographic ordering) and returns a
// rewound buffer.
//
// Hardening applied in T-003:
//   - maxBytes caps the amount of input bytes read. Anything beyond returns
//     ErrFetcherPayloadTooLarge. maxBytes<=0 falls back to
//     DefaultMaxExtractionBytes.
//   - depth tracking in decodeDatasources / decodeTables / decodeRows refuses
//     nested payloads deeper than MaxExtractionDepth with
//     ErrFetcherPayloadTooDeep.
//   - decoder.UseNumber() preserves numeric precision instead of coercing to
//     float64 (matches ingestion/adapters/parsers/json_parser.go convention
//     so round-tripping through the bridge doesn't lose precision).
//
// Invariants:
//   - datasource keys are iterated in lexicographic order
//   - table keys within a datasource are iterated in lexicographic order
//   - rows preserve their source order within each table
//   - an empty extraction `{}` produces an empty flat array `[]` (not an error)
//   - row values are copied verbatim; no coercion or transformation happens
func FlattenFetcherJSON(in io.Reader, maxBytes int64) (io.Reader, error) {
	if in == nil {
		return nil, ErrFetcherShapeMalformed
	}

	if maxBytes <= 0 {
		maxBytes = DefaultMaxExtractionBytes
	}

	// LimitReader + 1 sentinel byte: if the decoder consumes maxBytes and the
	// underlying stream still has more, that extra byte trips the limit and
	// we surface ErrFetcherPayloadTooLarge. A bare io.LimitReader would
	// silently truncate at the limit; wrapping it with capReader lets us
	// distinguish legitimate EOF from truncation.
	limited := newCapReader(in, maxBytes)

	decoder := json.NewDecoder(limited)
	decoder.UseNumber()

	tok, err := decoder.Token()
	if err != nil {
		if errors.Is(err, errPayloadExceeded) {
			return nil, fmt.Errorf("%w: reading opening token", ErrFetcherPayloadTooLarge)
		}

		return nil, fmt.Errorf("%w: reading opening token: %w", ErrFetcherShapeMalformed, err)
	}

	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("%w: expected root object, got %v", ErrFetcherShapeMalformed, tok)
	}

	datasources, err := decodeDatasources(decoder, 1)
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
// depth is the current nesting depth (1 = inside root object).
func decodeDatasources(decoder *json.Decoder, depth int) (map[string]datasourceRows, error) {
	if depth > MaxExtractionDepth {
		return nil, fmt.Errorf("%w: datasource level depth %d", ErrFetcherPayloadTooDeep, depth)
	}

	datasources := make(map[string]datasourceRows)

	for decoder.More() {
		dsKey, err := readStringKey(decoder, "datasource key")
		if err != nil {
			return nil, err
		}

		tables, err := decodeTables(decoder, depth+1)
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
// depth is the current nesting depth (2 = inside a datasource object).
func decodeTables(decoder *json.Decoder, depth int) (datasourceRows, error) {
	if depth > MaxExtractionDepth {
		return nil, fmt.Errorf("%w: table level depth %d", ErrFetcherPayloadTooDeep, depth)
	}

	if err := expectOpeningDelim(decoder, '{', "datasource value"); err != nil {
		return nil, err
	}

	tables := make(datasourceRows)

	for decoder.More() {
		tableKey, err := readStringKey(decoder, "table key")
		if err != nil {
			return nil, err
		}

		rows, err := decodeRows(decoder, depth+1)
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
// depth is the current nesting depth (3 = inside a table array).
func decodeRows(decoder *json.Decoder, depth int) ([]json.RawMessage, error) {
	if depth > MaxExtractionDepth {
		return nil, fmt.Errorf("%w: row level depth %d", ErrFetcherPayloadTooDeep, depth)
	}

	if err := expectOpeningDelim(decoder, '[', "rows"); err != nil {
		return nil, err
	}

	rows := make([]json.RawMessage, 0)

	for decoder.More() {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, errPayloadExceeded) {
				return nil, fmt.Errorf("%w: decoding row", ErrFetcherPayloadTooLarge)
			}

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
		if errors.Is(err, errPayloadExceeded) {
			return "", fmt.Errorf("%w: reading %s", ErrFetcherPayloadTooLarge, role)
		}

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
		if errors.Is(err, errPayloadExceeded) {
			return fmt.Errorf("%w: reading %s opening", ErrFetcherPayloadTooLarge, role)
		}

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
		if errors.Is(err, errPayloadExceeded) {
			return fmt.Errorf("%w: reading %s closing", ErrFetcherPayloadTooLarge, role)
		}

		return fmt.Errorf("%w: reading %s closing: %w", ErrFetcherShapeMalformed, role, err)
	}

	delim, ok := tok.(json.Delim)
	if !ok || delim != want {
		return fmt.Errorf("%w: expected %s closing %q, got %v", ErrFetcherShapeMalformed, role, want, tok)
	}

	return nil
}

// errPayloadExceeded is the sentinel returned by capReader when the byte
// budget is exhausted. Callers unwrap it into ErrFetcherPayloadTooLarge at
// the public boundary.
var errPayloadExceeded = errors.New("fetcher payload byte budget exceeded")

// capReader wraps an io.Reader and returns errPayloadExceeded when more than
// maxBytes have been read. Unlike a bare io.LimitReader, capReader
// distinguishes "stream ended at exactly maxBytes" (legitimate) from "stream
// wanted to produce byte #maxBytes+1" (truncation) by reading a single
// sentinel byte after the budget is spent.
type capReader struct {
	r         io.Reader
	remaining int64
	exceeded  bool
}

func newCapReader(r io.Reader, maxBytes int64) *capReader {
	return &capReader{r: r, remaining: maxBytes}
}

func (cr *capReader) Read(buf []byte) (int, error) {
	if cr.exceeded {
		return 0, errPayloadExceeded
	}

	if cr.remaining <= 0 {
		// Budget exhausted. Peek one byte to see if the stream wanted more.
		peek := make([]byte, 1)

		n, err := cr.r.Read(peek)
		if n > 0 {
			cr.exceeded = true

			return 0, errPayloadExceeded
		}

		// Stream ended exactly at the budget — legitimate EOF.
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}

		if err != nil {
			return 0, fmt.Errorf("cap reader peek: %w", err)
		}

		return 0, io.EOF
	}

	readSize := int64(len(buf))
	if readSize > cr.remaining {
		readSize = cr.remaining
	}

	bytesRead, err := cr.r.Read(buf[:readSize])
	cr.remaining -= int64(bytesRead)

	if err != nil {
		if errors.Is(err, io.EOF) {
			return bytesRead, io.EOF
		}

		return bytesRead, fmt.Errorf("cap reader read: %w", err)
	}

	return bytesRead, nil
}
