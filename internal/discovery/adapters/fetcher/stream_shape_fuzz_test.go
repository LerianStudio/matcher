// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher_test

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
)

// maxFuzzInputBytes bounds each fuzz iteration's input to keep the parser
// pipeline from allocating unbounded buffers under adversarial input. The
// cap is intentionally generous so legitimate Fetcher payloads (which fit
// entirely in memory before flattening anyway) still exercise the full
// decoder state machine.
const maxFuzzInputBytes = 64 * 1024

// FuzzFlattenFetcherJSON asserts the three core properties of the flattener
// under arbitrary byte input:
//
//  1. It never panics.
//  2. It either returns an error OR produces a reader that itself reads
//     without panicking all the way to EOF.
//  3. When the flattener reports success, the emitted bytes parse cleanly
//     as a JSON array of raw messages. This is the invariant downstream
//     generic parsers depend on: if we hand them garbage, ingestion fails
//     silently with empty row counts.
//
// Seed corpus covers: empty object, happy path nested shape, empty tables,
// multi-row tables, unicode keys/values, malformed JSON, null root, and
// completely empty bytes.
func FuzzFlattenFetcherJSON(f *testing.F) {
	// Valid shapes the production path emits.
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"ds":{"tbl":[{"k":"v"}]}}`))
	f.Add([]byte(`{"ds1":{"tbl1":[],"tbl2":[]}}`))
	f.Add([]byte(`{"ds":{"tbl":[{"a":1},{"a":2},{"a":3}]}}`))

	// Unicode keys and values — Fetcher extractions may carry localized data.
	f.Add([]byte("{\"\u00e9\":{\"\u00e7\":[{\"k\u00f1\":\"v\u00fc\"}]}}"))

	// Malformed / adversarial inputs the flattener must reject cleanly.
	f.Add([]byte(`[`))      // opens a non-object root
	f.Add([]byte(`null`))   // valid JSON but wrong root type
	f.Add([]byte(``))       // empty bytes — decoder returns io.EOF
	f.Add([]byte(`{`))      // truncated root object
	f.Add([]byte(`{"ds":`)) // truncated datasource value

	f.Fuzz(func(t *testing.T, data []byte) {
		// Bound input size; the contract is defined for in-memory payloads.
		// Anything larger stresses the test harness, not the function.
		if len(data) > maxFuzzInputBytes {
			data = data[:maxFuzzInputBytes]
		}

		out, err := fetcher.FlattenFetcherJSON(bytes.NewReader(data))
		if err != nil {
			// Rejection is a documented outcome — nothing else to assert.
			return
		}

		// Success path: output must be a non-nil reader, drain without panic,
		// and the drained bytes must themselves parse as a JSON array whose
		// elements are raw JSON messages.
		if out == nil {
			t.Fatalf("FlattenFetcherJSON returned nil reader and nil error")
		}

		flat, readErr := io.ReadAll(out)
		if readErr != nil {
			t.Fatalf("draining flattener output panicked/errored: %v", readErr)
		}

		var rows []json.RawMessage
		if jsonErr := json.Unmarshal(flat, &rows); jsonErr != nil {
			t.Fatalf(
				"flattener claimed success but emitted non-array output: err=%v, out=%q",
				jsonErr,
				truncateForLog(string(flat)),
			)
		}
	})
}

// truncateForLog keeps fuzz failure logs scannable when the emitted output
// is large. 256 bytes is enough to triage most shape violations without
// drowning the test console in a multi-megabyte payload.
func truncateForLog(value string) string {
	const limit = 256
	if len(value) <= limit {
		return value
	}

	return value[:limit] + "...<truncated>"
}
