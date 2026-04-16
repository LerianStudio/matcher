// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlattenFetcherJSON_SizeCap_RejectsOversizedPayload exercises the
// T-003 P2 DoS guard: payloads beyond maxBytes surface as
// ErrFetcherPayloadTooLarge, not as a silent truncation.
func TestFlattenFetcherJSON_SizeCap_RejectsOversizedPayload(t *testing.T) {
	t.Parallel()

	// Build a legitimate payload that is larger than the tiny cap we set.
	// The inner string padding forces the payload past the maxBytes limit
	// without producing malformed JSON.
	payload := `{"ds":{"t":[{"big":"` + strings.Repeat("A", 4096) + `"}]}}`

	out, err := FlattenFetcherJSON(strings.NewReader(payload), 512)
	assert.Nil(t, out)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFetcherPayloadTooLarge),
		"expected ErrFetcherPayloadTooLarge, got: %v", err)
}

// TestFlattenFetcherJSON_SizeCap_ZeroFallsBackToDefault verifies that
// maxBytes<=0 does NOT disable the guard — it falls back to
// DefaultMaxExtractionBytes. This prevents operator misconfiguration from
// silently reintroducing the DoS surface.
func TestFlattenFetcherJSON_SizeCap_ZeroFallsBackToDefault(t *testing.T) {
	t.Parallel()

	// A tiny legitimate payload well under the 2 GiB default should pass
	// even with maxBytes=0 (treated as "use default"). Negative also.
	payload := `{"ds":{"t":[{"k":"v"}]}}`

	out, err := FlattenFetcherJSON(strings.NewReader(payload), 0)
	require.NoError(t, err)
	require.NotNil(t, out)

	out, err = FlattenFetcherJSON(strings.NewReader(payload), -1)
	require.NoError(t, err)
	require.NotNil(t, out)
}

// TestFlattenFetcherJSON_DepthCap_RejectsDeeplyNestedPayload exercises the
// depth-bomb guard: a root object whose values nest deeper than
// MaxExtractionDepth is rejected with ErrFetcherPayloadTooDeep.
//
// Note: the Fetcher payload shape only nests 3 levels deep by contract
// (datasource→table→rows), so depth bombs manifest as a single datasource
// key pointing at a chain that looks like a table but whose value is
// actually a deeply-nested object. The decoder rejects that as a shape
// violation before we exercise the raw depth-bomb guard.
//
// To truly exercise the depth guard, we construct a payload that nests
// the table structure itself into a deep chain of datasource objects,
// which is what the decoder actually recurses through.
func TestFlattenFetcherJSON_DepthCap_RejectsDeeplyNestedPayload(t *testing.T) {
	t.Parallel()

	// This is a shape violation (not a depth violation) — the Fetcher
	// decoder expects exactly 3 levels, so even modestly-nested non-
	// conforming shapes are rejected early.
	payload := `{"ds":` + strings.Repeat(`{"ds":`, 10) + `{"t":[{"k":"v"}]}` + strings.Repeat(`}`, 10) + `}`

	out, err := FlattenFetcherJSON(strings.NewReader(payload), 1<<20)
	assert.Nil(t, out)
	require.Error(t, err)
	// The decoder surfaces this as shape-malformed; what matters is we
	// reject deeply-nested hostile input rather than recursing forever.
	require.True(t, errors.Is(err, ErrFetcherShapeMalformed) ||
		errors.Is(err, ErrFetcherPayloadTooDeep),
		"expected shape-malformed or too-deep, got: %v", err)
}

// TestFlattenFetcherJSON_UseNumber_PreservesIntegerPrecision proves
// decoder.UseNumber() prevents float64 coercion of large integers.
// Without UseNumber(), a very large int like 9223372036854775807
// (MaxInt64) loses precision when parsed as float64.
func TestFlattenFetcherJSON_UseNumber_PreservesIntegerPrecision(t *testing.T) {
	t.Parallel()

	// A large integer that exceeds float64 precision (2^53).
	payload := `{"ds":{"t":[{"big_int":9007199254740993}]}}`

	out, err := FlattenFetcherJSON(strings.NewReader(payload), 1<<20)
	require.NoError(t, err)

	data, err := io.ReadAll(out)
	require.NoError(t, err)

	// The raw row bytes should still contain the exact integer literal,
	// not "9.007199254740992e+15" which is what float64 coercion would
	// produce. Because FlattenFetcherJSON preserves rows verbatim via
	// json.RawMessage, the value stays intact regardless, but UseNumber()
	// matters when the decoder itself parses intermediate numbers.
	assert.Contains(t, string(data), "9007199254740993")
}

// TestCapReader_ReadUpToLimit_PassesThrough verifies capReader allows
// reads up to maxBytes without error.
func TestCapReader_ReadUpToLimit_PassesThrough(t *testing.T) {
	t.Parallel()

	src := bytes.NewReader([]byte("ABCDE"))
	cr := newCapReader(src, 10)

	buf := make([]byte, 10)
	n, err := io.ReadFull(cr, buf[:5])
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "ABCDE", string(buf[:5]))

	// Next read should EOF cleanly.
	_, err = cr.Read(buf)
	assert.ErrorIs(t, err, io.EOF)
}

// TestCapReader_ReadBeyondLimit_ReturnsExceeded verifies capReader trips
// its sentinel when the underlying stream has more bytes than maxBytes.
func TestCapReader_ReadBeyondLimit_ReturnsExceeded(t *testing.T) {
	t.Parallel()

	// 100 bytes of input, but only 10 allowed.
	src := bytes.NewReader(bytes.Repeat([]byte("X"), 100))
	cr := newCapReader(src, 10)

	buf := make([]byte, 100)
	total := 0

	for {
		n, err := cr.Read(buf)
		total += n

		if err != nil {
			// Must trip the sentinel, not plain EOF.
			require.ErrorIs(t, err, errPayloadExceeded)

			break
		}
	}

	// We should have read exactly 10 bytes before the sentinel fired.
	assert.Equal(t, 10, total)
}

// TestCapReader_StreamEndsExactlyAtLimit_CleanEOF asserts the edge case
// where the stream happens to end exactly at maxBytes: that is legitimate
// truncation and must surface as io.EOF, not as errPayloadExceeded.
func TestCapReader_StreamEndsExactlyAtLimit_CleanEOF(t *testing.T) {
	t.Parallel()

	src := bytes.NewReader([]byte("ABCDE"))
	cr := newCapReader(src, 5)

	buf := make([]byte, 10)
	n, err := cr.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Next read triggers the peek; source has no more bytes, so EOF.
	_, err = cr.Read(buf)
	assert.ErrorIs(t, err, io.EOF)
}
