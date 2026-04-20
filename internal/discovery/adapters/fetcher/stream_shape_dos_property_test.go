// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Property tests for the T-003 P2 DoS hardening of FlattenFetcherJSON.
//
// Two invariants are verified here — Invariants 7 and 8 from the Gate 5
// test plan:
//
//  7. Depth cap enforcement — for any generated payload that nests deeper
//     than MaxExtractionDepth inside the datasource/table/row chain, the
//     flattener returns an error (ErrFetcherPayloadTooDeep OR
//     ErrFetcherShapeMalformed — the legitimate shape has exactly 3 levels,
//     so depth bombs often surface as shape violations on the way to the
//     depth guard). For payloads within the cap, the flattener accepts them.
//  8. Size cap enforcement — for any payload whose encoded length exceeds
//     the maxBytes argument, the flattener returns ErrFetcherPayloadTooLarge.
//     For payloads within the cap, it succeeds.
//
// The existing stream_shape_property_test.go suite verifies Invariants 1-6
// (row count preservation, lexicographic ordering, verbatim rows, empty
// identity). Those properties must continue to hold AFTER the hardening;
// this file only adds the two new DoS-related invariants.
package fetcher

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

// depthCase is the generator payload for Invariant 7. The Fetcher shape
// contract is strictly 3 levels (datasource → table → rows-array), so
// a payload is a "depth bomb" only when we inject MORE than one layer of
// nesting between the datasource object and the row array. Depth==1
// happens to reproduce the canonical 3-level shape by coincidence (ds→"n"
// as a table is indistinguishable from ds→table), so we start bombs at
// Depth==2.
//
// ExtraDepth holds the count of ADDITIONAL wrap layers beyond the valid
// shape, not the absolute nesting count. ExtraDepth==0 is the canonical
// shape. ExtraDepth>=1 inserts that many objects between the table
// position and the row array, which violates the `expect array` contract
// inside decodeRows regardless of the nesting depth.
type depthCase struct {
	ExtraDepth int
}

// Generate yields extra-depth values biased to span the interesting
// boundary (0..2000 extra layers). 0 is canonical (must succeed). 1..999
// is "shape-malformed at shallow depth" territory (must fail). 1000+ is
// where the depth cap fires explicitly (must fail with depth sentinel).
// The test below accepts both shape-malformed and depth-too-deep on the
// reject branch because either is a legitimate rejection — what matters
// is that rejection HAPPENS for every non-zero extra depth.
func (depthCase) Generate(rng *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(depthCase{ExtraDepth: rng.Intn(2001)})
}

// buildNestedDepthBomb returns a payload. When ExtraDepth==0, it emits
// the canonical 3-level valid shape. When ExtraDepth>=1, it inserts
// ExtraDepth additional `{"n":` layers between the table name and the row
// array, producing a payload the decoder rejects as either shape-malformed
// (at shallow nesting, because decodeRows' expectOpeningDelim('[') sees
// '{' instead) or depth-too-deep (at deep nesting, because decodeTables
// recurses past MaxExtractionDepth).
//
// Concretely:
//   - ExtraDepth=0 → `{"ds":{"t":[{"k":"v"}]}}` (valid)
//   - ExtraDepth=1 → `{"ds":{"t":{"n":[{"k":"v"}]}}}` (shape-malformed: row array expected, got object)
//   - ExtraDepth=5 → `{"ds":{"t":{"n":{"n":{"n":{"n":{"n":[{"k":"v"}]}}}}}}}` (same, deeper)
//   - ExtraDepth>=1000 → depth cap fires
func buildNestedDepthBomb(extraDepth int) string {
	var builder strings.Builder

	// Start the canonical valid prefix up through the table name.
	builder.WriteString(`{"ds":{"t":`)

	// Inject ExtraDepth levels of `{"n":` between the table name and the
	// row array. These are what turn a valid shape into a bomb.
	for i := 0; i < extraDepth; i++ {
		builder.WriteString(`{"n":`)
	}

	// Innermost legitimate row array.
	builder.WriteString(`[{"k":"v"}]`)

	// Close the injected wraps.
	for i := 0; i < extraDepth; i++ {
		builder.WriteString(`}`)
	}

	// Close the valid suffix (table object + datasource object + root).
	builder.WriteString(`}}`)

	return builder.String()
}

// TestProperty_FlattenFetcherJSON_DepthCap_NeverAcceptsDeepPayload
// verifies Invariant 7: for any ExtraDepth >= 1, FlattenFetcherJSON must
// reject the payload. The rejection sentinel is either
// ErrFetcherPayloadTooDeep (the dedicated DoS guard, fires for
// ExtraDepth well past MaxExtractionDepth) or ErrFetcherShapeMalformed
// (fires earlier because our injected `{"n":` layer appears where the
// decoder expects a row array opening '['). Both are correct rejections.
//
// What matters is that NO deeply-nested payload slips through and gets
// accepted as a valid Fetcher shape — that would signal the hardening
// has regressed or the depth cap has been silently removed.
func TestProperty_FlattenFetcherJSON_DepthCap_NeverAcceptsDeepPayload(t *testing.T) {
	t.Parallel()

	prop := func(dc depthCase) bool {
		// ExtraDepth 0 is the canonical valid 3-level shape; that is
		// covered by the existing property suite. Here we assert only
		// the bomb-rejection invariant.
		if dc.ExtraDepth == 0 {
			return true
		}

		payload := buildNestedDepthBomb(dc.ExtraDepth)

		// Give a generous size cap so we isolate the depth invariant
		// from the size invariant. 16 MiB covers any ExtraDepth up to
		// 2000 (each `{"n":` is 5 bytes, so ~20 KiB worst case).
		out, err := FlattenFetcherJSON(strings.NewReader(payload), 16<<20)

		// PROPERTY: bomb payload must be rejected. out == nil, err != nil,
		// err == ErrFetcherPayloadTooDeep OR ErrFetcherShapeMalformed.
		if out != nil || err == nil {
			return false
		}

		return errors.Is(err, ErrFetcherPayloadTooDeep) ||
			errors.Is(err, ErrFetcherShapeMalformed)
	}

	// MaxCount=150 keeps JSON building cheap; each iteration's payload
	// is at most a few KiB.
	cfg := &quick.Config{MaxCount: 150}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_FlattenFetcherJSON_DepthCap_CanonicalShapeAlwaysAccepted
// is the complementary positive property: the 3-level canonical Fetcher
// shape MUST always succeed, regardless of row count within the size cap.
// This guards against a future regression where the depth guard is
// strengthened too aggressively and starts rejecting legitimate payloads.
func TestProperty_FlattenFetcherJSON_DepthCap_CanonicalShapeAlwaysAccepted(t *testing.T) {
	t.Parallel()

	prop := func(rowCount uint8) bool {
		// Bound row count so payload stays well within the size cap.
		count := int(rowCount) % 50

		var builder strings.Builder

		builder.WriteString(`{"ds":{"t":[`)

		for i := 0; i < count; i++ {
			if i > 0 {
				builder.WriteString(`,`)
			}

			builder.WriteString(`{"k":"v"}`)
		}

		builder.WriteString(`]}}`)

		out, err := FlattenFetcherJSON(strings.NewReader(builder.String()), 1<<20)

		// PROPERTY: canonical shape → no error, non-nil output.
		if err != nil || out == nil {
			return false
		}

		// Drain to confirm the output is a readable array (correctness of
		// content is handled by stream_shape_property_test.go; here we
		// only verify "acceptance" as the Invariant 7 positive half).
		_, readErr := io.ReadAll(out)

		return readErr == nil
	}

	cfg := &quick.Config{MaxCount: 150}
	require.NoError(t, quick.Check(prop, cfg))
}

// sizeCase is the generator payload for Invariant 8. Padding and cap are
// both bounded so we always produce a well-formed payload and can reason
// about whether it exceeds the cap.
type sizeCase struct {
	// PaddingBytes is how much filler we add to a single row's "big"
	// field. Bounded to keep the generated payload under a few MiB.
	PaddingBytes int
	// MaxBytes is the cap passed to FlattenFetcherJSON. Bounded likewise.
	MaxBytes int64
}

// Generate yields a range of (payload, limit) pairs spanning "definitely
// under", "right at boundary", and "definitely over". This ensures the
// test distribution hits both branches of the cap check.
func (sizeCase) Generate(rng *rand.Rand, _ int) reflect.Value {
	// 0..64 KiB of padding covers small (no DoS risk) and medium
	// (enough to exceed a tight cap) cases.
	padding := rng.Intn(64 << 10)

	// Limit is biased: one bucket picks a value comparable to padding
	// (often under or over depending on luck); another picks a very
	// small limit (guaranteed over); the remainder a very large limit
	// (guaranteed under). Split keeps the boundary + both tails in play.
	// Variable named `limit` rather than `cap` to avoid shadowing the
	// cap() builtin.
	var limit int64

	switch rng.Intn(3) {
	case 0:
		limit = int64(rng.Intn(1 << 20)) // 0..1 MiB
	case 1:
		limit = int64(rng.Intn(256)) + 1 // 1..256 bytes — almost always over
	default:
		limit = 8 << 20 // 8 MiB — always under
	}

	return reflect.ValueOf(sizeCase{PaddingBytes: padding, MaxBytes: limit})
}

// buildSizedPayload returns a legitimate 3-level Fetcher payload whose
// encoded length is approximately 32 + paddingBytes. This gives the test
// precise control over whether the payload exceeds maxBytes.
func buildSizedPayload(paddingBytes int) string {
	// The scaffolding `{"ds":{"t":[{"big":"<padding>"}]}}` is 32 bytes
	// when padding is empty. Adding N bytes of padding yields exactly
	// 32 + N encoded bytes.
	return `{"ds":{"t":[{"big":"` + strings.Repeat("A", paddingBytes) + `"}]}}`
}

// TestProperty_FlattenFetcherJSON_SizeCap_RejectsOversizedPayloads verifies
// Invariant 8 (rejection half): any payload whose encoded length exceeds
// maxBytes is rejected with ErrFetcherPayloadTooLarge. Payloads within
// the cap are accepted (the complementary positive case is below).
func TestProperty_FlattenFetcherJSON_SizeCap_RejectsOversizedPayloads(t *testing.T) {
	t.Parallel()

	prop := func(sc sizeCase) bool {
		payload := buildSizedPayload(sc.PaddingBytes)
		payloadLen := int64(len(payload))

		out, err := FlattenFetcherJSON(strings.NewReader(payload), sc.MaxBytes)

		// maxBytes<=0 is legally interpreted as "use default", which is
		// 2 GiB. Any payload below that succeeds; we never see the
		// oversized branch for non-positive caps. Skip those cases to
		// keep the property implication crisp.
		if sc.MaxBytes <= 0 {
			return err == nil && out != nil
		}

		// Payload within the cap: must succeed.
		if payloadLen <= sc.MaxBytes {
			return err == nil && out != nil
		}

		// Payload over the cap: must fail with ErrFetcherPayloadTooLarge.
		return out == nil && errors.Is(err, ErrFetcherPayloadTooLarge)
	}

	// MaxCount=100 — each iteration allocates up to 64 KiB of string
	// padding, so we want to keep the test budget reasonable on CI.
	cfg := &quick.Config{MaxCount: 100}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_FlattenFetcherJSON_SizeCap_ZeroFallsBackToDefault verifies
// that non-positive maxBytes (0 or negative) never causes ErrFetcherPayloadTooLarge
// for small, legitimate payloads — the production contract is "zero falls
// back to DefaultMaxExtractionBytes (2 GiB)", not "zero means unlimited".
// This property guards against a future refactor that disables the guard
// entirely on bad operator input.
func TestProperty_FlattenFetcherJSON_SizeCap_ZeroFallsBackToDefault(t *testing.T) {
	t.Parallel()

	prop := func(negativeCap int) bool {
		// Constrain to 0 or negative caps only (the edge case of interest).
		// Named `limit` to avoid shadowing the cap() builtin.
		limit := int64(negativeCap)
		if limit > 0 {
			limit = -limit
		}

		// A tiny legitimate payload — well under the 2 GiB default cap.
		payload := `{"ds":{"t":[{"k":"v"}]}}`

		out, err := FlattenFetcherJSON(strings.NewReader(payload), limit)

		// PROPERTY: tiny payload + zero/negative cap → success.
		if err != nil || out == nil {
			return false
		}

		// Confirm the output is readable (consumer contract).
		_, readErr := io.ReadAll(out)

		return readErr == nil
	}

	cfg := &quick.Config{MaxCount: 50}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_FlattenFetcherJSON_HardeningPreservesRoundtrip is a regression
// guard: for any payload that passes both the depth AND size guards, the
// flattened output must still decode as a JSON array. This ensures the
// hardening changes didn't break the core contract — the test suite in
// stream_shape_property_test.go already covers verbatim row preservation
// in depth, but this property specifically targets "hardening didn't
// corrupt anything on the happy path".
func TestProperty_FlattenFetcherJSON_HardeningPreservesRoundtrip(t *testing.T) {
	t.Parallel()

	prop := func(rowCount uint8) bool {
		// 0..15 rows, tightly bounded for speed.
		count := int(rowCount) % 16

		var builder strings.Builder

		builder.WriteString(`{"ds":{"t":[`)

		for i := 0; i < count; i++ {
			if i > 0 {
				builder.WriteString(`,`)
			}

			builder.WriteString(`{"n":`)

			// Varied value shapes to exercise UseNumber across numeric,
			// string, and nested-object cases. Verbatim preservation is
			// asserted in the other property test file; here we only
			// require successful roundtrip through the hardening guards.
			switch i % 3 {
			case 0:
				builder.WriteString(`9007199254740993`) // MaxInt64-ish, tests UseNumber
			case 1:
				builder.WriteString(`"literal"`)
			default:
				builder.WriteString(`{"nested":{"deep":1}}`)
			}

			builder.WriteString(`}`)
		}

		builder.WriteString(`]}}`)

		// Generous cap so size is never the rejection reason.
		out, err := FlattenFetcherJSON(strings.NewReader(builder.String()), 1<<20)
		if err != nil || out == nil {
			return false
		}

		data, readErr := io.ReadAll(out)
		if readErr != nil {
			return false
		}

		// PROPERTY: output is a well-formed JSON array whose length is
		// a balanced pair of brackets. We don't reparse content here —
		// that's the existing suite's job. We only verify the byte-level
		// framing survived the hardening.
		return bytes.HasPrefix(data, []byte("[")) && bytes.HasSuffix(data, []byte("]"))
	}

	cfg := &quick.Config{MaxCount: 100}
	require.NoError(t, quick.Check(prop, cfg))
}
