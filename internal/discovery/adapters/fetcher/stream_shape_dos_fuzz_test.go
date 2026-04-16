// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
)

// FuzzFlattenFetcherJSON_DoSGuard asserts the T-003 P2 hardening: no input
// — no matter how cleverly constructed — can bypass the size/depth caps
// and consume unbounded resources. The fuzzer rotates through three
// pathological shapes:
//
//  1. Depth bombs: deeply-nested wrapper objects.
//  2. Size bombs: long value strings that inflate past the cap.
//  3. Random byte noise: mostly rejected as shape-malformed but we still
//     want to prove the parser never panics.
//
// Success conditions:
//   - Never panics.
//   - Either returns an error OR returns a reader that we can drain to EOF
//     without panicking, and whose length respects the cap we passed in.
//   - When the cap is tiny (1 KB), oversized input reliably surfaces
//     ErrFetcherPayloadTooLarge rather than succeeding with truncated bytes.
func FuzzFlattenFetcherJSON_DoSGuard(f *testing.F) {
	// Seed corpus exercises the three pathological shapes.
	seeds := []struct {
		depth int
		size  int
		noise string
	}{
		{depth: 10, size: 100, noise: ""},
		{depth: 200, size: 1000, noise: ""},
		{depth: 1200, size: 50, noise: ""}, // beyond MaxExtractionDepth
		{depth: 2, size: 50_000, noise: ""},
		{depth: 2, size: 10_000_000, noise: ""}, // big size bomb
		{depth: 0, size: 0, noise: `{"ds":{"t":[{}]}}`},
		{depth: 5, size: 100, noise: `\x00\xff\xee\xdd`},
	}

	for _, s := range seeds {
		f.Add(s.depth, s.size, s.noise)
	}

	f.Fuzz(func(t *testing.T, depth int, size int, noise string) {
		// Clamp to reasonable ranges so the fuzz harness itself does not
		// eat all memory building the payload.
		if depth < 0 {
			depth = 0
		}

		if depth > 2000 {
			depth = 2000
		}

		if size < 0 {
			size = 0
		}

		if size > 1_000_000 {
			size = 1_000_000
		}

		payload := buildFuzzPayload(depth, size, noise)

		// Use a tight cap so the size guard fires deterministically on
		// oversized input.
		const capBytes = 4096

		out, err := fetcher.FlattenFetcherJSON(strings.NewReader(payload), capBytes)

		if err != nil {
			// Any error is legitimate — hostile input earns a rejection.
			return
		}

		if out == nil {
			t.Fatalf("flatten returned (nil, nil) — must return either an error or a usable reader")
		}

		// Drain the output; must not panic, must not produce more bytes
		// than the cap allows (shape-flattened bytes can be equal but not
		// larger).
		drained, drainErr := io.ReadAll(out)
		if drainErr != nil {
			t.Fatalf("drain flatten output: %v", drainErr)
		}

		if len(drained) > capBytes*2 {
			// Flattening is a subtractive transform (removes wrapper
			// brackets/keys, keeps rows), so output should never exceed
			// the input cap by more than a constant factor. Catch runaway
			// expansion explicitly.
			t.Fatalf("flatten output %d bytes exceeds 2x cap %d", len(drained), capBytes)
		}

		if size > capBytes && !errors.Is(err, fetcher.ErrFetcherPayloadTooLarge) {
			// If the input was bigger than the cap but we didn't error,
			// that's an exceedance of the DoS guarantee.
			if len(payload) > capBytes {
				t.Fatalf("oversized input %d bytes accepted without ErrFetcherPayloadTooLarge (cap=%d)",
					len(payload), capBytes)
			}
		}
	})
}

// buildFuzzPayload constructs a pathological input string for a given
// (depth, size) combination. The "datasource→table→rows" Fetcher shape is
// honoured loosely; hostile inputs may produce shape-malformed output
// which is itself a legitimate rejection path.
func buildFuzzPayload(depth, size int, noise string) string {
	var buf bytes.Buffer

	// Build nested datasources (shape violation for depth > 1, which is
	// what makes this a depth-bomb seed).
	buf.WriteString(`{"ds":`)

	for i := 0; i < depth; i++ {
		buf.WriteString(`{"inner":`)
	}

	buf.WriteString(`{"t":[{"k":"`)
	buf.WriteString(strings.Repeat("X", size))
	buf.WriteString(`"}]}`)

	for i := 0; i < depth; i++ {
		buf.WriteString(`}`)
	}

	buf.WriteString(`}`)

	if noise != "" {
		buf.WriteString(noise)
	}

	return buf.String()
}
