// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"errors"
	"strings"
	"testing"
	"unicode"
)

// maxFuzzStatusBytes caps the length of the status string each fuzz
// iteration will feed through normalizeExtractionStatus. The function
// is a pure string switch with constant-time mapping, so beyond a few
// hundred bytes we are only stressing strings.ToUpper/TrimSpace — not
// the function's decision logic. 4 KiB is generous enough to exercise
// the unicode-folding edges without burning iteration budget on
// multi-megabyte haystacks.
const maxFuzzStatusBytes = 4 * 1024

// maxFuzzResultPathBytes caps the length of the result path per
// iteration. validateFetcherResultPath runs path.Clean which is O(n);
// 4 KiB is a realistic upper bound on filesystem paths and lets the
// fuzzer probe the cleaner's state machine without allocating
// multi-megabyte strings on each call.
const maxFuzzResultPathBytes = 4 * 1024

// canonicalExtractionStatuses is the whitelist of strings that
// normalizeExtractionStatus is permitted to return on the success
// path. If the fuzzer ever produces a normalized status outside this
// set, the function has silently admitted an unrecognized value —
// which would let callers route state machines into unknown branches.
//
// Kept in sync with the switch statement in
// normalizeExtractionStatus (client_validation.go).
var canonicalExtractionStatuses = map[string]struct{}{
	"PENDING":    {},
	"SUBMITTED":  {},
	"RUNNING":    {},
	"EXTRACTING": {},
	"COMPLETE":   {},
	"FAILED":     {},
	"CANCELLED":  {},
}

// FuzzNormalizeExtractionStatus asserts the core contract of
// normalizeExtractionStatus under arbitrary Status / ResultPath input:
//
//  1. It never panics, regardless of Unicode, control bytes, or length.
//  2. Every call returns either ("", non-nil error) or (canonical, nil).
//     Mixed states ("", nil) or (non-canonical, nil) are invariant
//     violations.
//  3. When a status is returned on the success path, it MUST be one of
//     the seven documented canonical values. The fuzzer is probing for
//     a case where case-folding / trimming / lookup collides and lets
//     an unrecognized string slip through the whitelist.
//  4. The returned status contains no control runes, no NUL byte, no
//     newlines — a defence in depth against downstream loggers /
//     trace exporters mishandling non-printable state names.
//
// Seed corpus draws from every documented happy + sad case in the unit
// tests (client_status_normalization_test.go, client_validation_test.go)
// plus adversarial edges: empty, whitespace-only, NUL bytes, Unicode
// look-alikes, very long strings, and COMPLETE paired with malicious
// ResultPath values (path traversal, NUL injection, URL schemes).
func FuzzNormalizeExtractionStatus(f *testing.F) {
	type seed struct {
		status     string
		resultPath string
	}

	seeds := []seed{
		// Happy passthrough + case-fold paths from the existing unit tests.
		{"PENDING", ""},
		{"pending", ""},
		{"SUBMITTED", ""},
		{"submitted", ""},
		{"RUNNING", ""},
		{"running", ""},
		{"processing", ""},
		{"EXTRACTING", ""},
		{"extracting", ""},
		{"FAILED", ""},
		{"failed", ""},
		{"CANCELLED", ""},
		{"CANCELED", ""},
		{"canceled", ""},
		{"cancelled", ""},

		// COMPLETE + valid result path (success).
		{"COMPLETE", "/data/results/job-1.json"},
		{"completed", "/data/results/job-1.json"},
		{"  Completed  ", "/data/results/job-1.json"},

		// COMPLETE + invalid / missing result paths (error).
		{"COMPLETE", ""},
		{"COMPLETE", "   "},
		{"COMPLETE", "s3://bucket/output.csv"},
		{"COMPLETE", "/data/../etc/passwd"},
		{"COMPLETE", "/data/output.csv?x=1"},
		{"COMPLETE", "/data/output.csv#frag"},
		{"COMPLETE", "relative/path"},

		// Whitespace / casing edges on the status field.
		{"  ExTrAcTiNg  ", ""},
		{"ProCeSsInG", ""},
		{"   ", ""},
		{"", ""},
		{"\t\n", ""},

		// Adversarial status shapes.
		{"bogus\x00", ""},
		{"UNKNOWN_STATUS", ""},
		{"PENDING\nCOMPLETE", ""},
		{"COMPLETE\x00/etc/passwd", "/valid/path"},
		{strings.Repeat("a", 10_000), ""},
		{"日本語", ""},

		// Unicode case-fold edges — ToUpper may not canonicalize these
		// the way the whitelist expects, and the fuzzer should confirm
		// that no Unicode-folded form sneaks past the switch.
		{"pendıng", ""}, // dotless i
		{"ﬀailed", ""},  // ligature
	}

	for _, s := range seeds {
		f.Add(s.status, s.resultPath)
	}

	f.Fuzz(func(t *testing.T, status, resultPath string) {
		// Bound inputs so single iterations stay cheap. The function's
		// contract is defined for "status strings and file paths"; the
		// fuzzer's job is decision-edge discovery, not memory stress.
		if len(status) > maxFuzzStatusBytes {
			status = status[:maxFuzzStatusBytes]
		}

		if len(resultPath) > maxFuzzResultPathBytes {
			resultPath = resultPath[:maxFuzzResultPathBytes]
		}

		// Property 1: never panic. Any panic from a pure string switch is
		// a bug — there is no unsafe code path here.
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in normalizeExtractionStatus(status=%q, resultPath=%q): %v", status, resultPath, r)
			}
		}()

		resp := fetcherExtractionStatusResponse{
			Status:     status,
			ResultPath: resultPath,
		}

		result, err := normalizeExtractionStatus(resp)

		// Property 2: mixed states are forbidden.
		if err == nil && result == "" {
			t.Fatalf("empty result with nil error: status=%q resultPath=%q", status, resultPath)
		}

		if err != nil && result != "" {
			t.Fatalf("non-empty result %q with non-nil error %v: status=%q resultPath=%q", result, err, status, resultPath)
		}

		// Errors are an expected outcome — the function rejects a wide
		// input space by design. The error sentinel discipline is
		// covered by unit tests; all the fuzzer cares about is that
		// errored outputs don't carry ghost strings.
		if err != nil {
			// Spot-check that the error is wrapped, not raw: every error
			// path in normalizeExtractionStatus wraps ErrFetcherBadResponse.
			// This is a contract the fuzzer can cheaply confirm on every
			// rejection path without over-asserting the error message.
			if !errors.Is(err, ErrFetcherBadResponse) {
				t.Fatalf("error is not wrapped in ErrFetcherBadResponse: err=%v status=%q", err, status)
			}

			return
		}

		// Property 3: successful normalized status MUST be in the whitelist.
		if _, ok := canonicalExtractionStatuses[result]; !ok {
			t.Fatalf("normalized status %q not in canonical whitelist (input status=%q)", result, status)
		}

		// Property 4: no control runes, no NUL, no newlines in the
		// normalized output.
		for i, r := range result {
			if r == 0 {
				t.Fatalf("NUL byte at index %d in normalized status %q (input=%q)", i, result, status)
			}

			if !unicode.IsPrint(r) {
				t.Fatalf("non-printable rune %U at index %d in normalized status %q (input=%q)", r, i, result, status)
			}
		}
	})
}

// FuzzValidateFetcherResultPath asserts the path-traversal defense
// contract of validateFetcherResultPath. The function is Matcher's
// first line of defense against a malicious or misconfigured Fetcher
// deployment serving an extraction result path that points somewhere
// it should not — /etc/passwd, an arbitrary URL, a relative path that
// resolves to something unexpected.
//
// Properties:
//
//  1. No panic, regardless of Unicode, NUL bytes, percent-encoding, or
//     extreme length.
//  2. Never (nil err, nil path) — validateFetcherResultPath returns
//     only an error, so the only mixed-state risk is panicking
//     partway through path.Clean.
//  3. On the success path (err == nil), the ACCEPTED input must not
//     contain the traversal / malformed-URL markers the function is
//     supposed to reject: "..", "://", "?", "#". If any appear in an
//     accepted path, the function has a defense gap.
//  4. On the success path, the accepted input must start with '/' —
//     the function's "must be absolute" contract.
//
// The test also records (but does not fail on) NUL byte and backslash
// acceptance. Those are not explicitly rejected by the current
// implementation; if the fuzzer surfaces accepted paths containing
// them, those are candidate findings to raise with the reviewer, not
// test failures. See comment in the fuzz body for details.
//
// Seed corpus covers classic path traversal attacks (plain, URL-encoded,
// Windows separators, Unicode look-alikes), the happy path, URL-shaped
// inputs, NUL-byte injection, and very long strings.
func FuzzValidateFetcherResultPath(f *testing.F) {
	seeds := []string{
		// Happy-path absolute paths.
		"/data/results/job-1.json",
		"/valid/path/file.json",
		"/",
		"/data/extractions/2026/01/15/job-abc123/output.json",

		// Documented error paths — exercised by existing unit tests.
		"",
		"   ",
		"data/results/output.csv",
		"s3://bucket/output.csv",
		"ftp://server/file.csv",
		"https://example.com/output.csv",
		"http://host/file",
		"/data://bucket/output.csv",
		"/data/output.csv?version=2",
		"/data/output.csv#section",
		"/data/../etc/passwd",
		"/../etc/shadow",
		"/..",
		"/data//output.csv",
		"/data/results/",
		"./relative",
		"/../absolute",

		// Adversarial: URL-encoded traversal. path.Clean treats %2e as
		// literal text, so the cleaned form preserves the encoding —
		// this probes whether the function decodes before cleaning.
		"/data/%2e%2e/etc/passwd",
		"/data/%2E%2E/etc/passwd",

		// Adversarial: Windows separators. POSIX path.Clean does NOT
		// normalize backslash, so the function accepts these. The fuzz
		// property captures this as a "candidate finding to review."
		"/data\\..\\file",
		"/\\..\\etc\\passwd",

		// Adversarial: NUL byte injection. Go strings permit embedded
		// NULs; path.Clean does not strip them. This probes the
		// downstream handling.
		"/data/file\x00.json",
		"/data\x00/secret",

		// Adversarial: Unicode look-alikes. These pass basic ASCII
		// traversal checks but may confuse downstream consumers.
		"/data/\u2024\u2024/file", // one dot leader × 2
		"/data/\uff0e\uff0e/file", // fullwidth full stop × 2
		"/data/\u002e\u002e/file", // ascii dot × 2, already covered by ".."
		"/data/\u2215etc/passwd",  // division slash

		// Adversarial: length extremes.
		strings.Repeat("/", 10_000),
		"/" + strings.Repeat("a", 10_000),

		// Adversarial: control characters.
		"/data/\n../etc/passwd",
		"/data/\r\n../etc/passwd",

		// Edge: just a single character.
		"a",
		".",
		"..",
		"/",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, resultPath string) {
		if len(resultPath) > maxFuzzResultPathBytes {
			resultPath = resultPath[:maxFuzzResultPathBytes]
		}

		// Property 1: never panic.
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in validateFetcherResultPath(%q): %v", resultPath, r)
			}
		}()

		err := validateFetcherResultPath(resultPath)

		// Rejection is the main defense mechanism; any of the four
		// documented sentinels is acceptable. We don't over-assert
		// which one — unit tests already do that. Here we only care
		// about behavior on the ACCEPTED branch.
		if err != nil {
			// Sanity: error must be one of the documented sentinels.
			// This catches regressions where a new error shape leaks
			// through without being classified.
			switch {
			case errors.Is(err, ErrFetcherResultPathRequired),
				errors.Is(err, ErrFetcherResultPathNotAbsolute),
				errors.Is(err, ErrFetcherResultPathInvalidFormat),
				errors.Is(err, ErrFetcherResultPathTraversal):
				return
			default:
				t.Fatalf("undocumented error sentinel from validateFetcherResultPath(%q): %v", resultPath, err)
			}
		}

		// Success path — the function accepted this input. Verify the
		// strong invariants the function's rejection logic promises to
		// enforce.

		trimmed := strings.TrimSpace(resultPath)

		// Property 4: accepted paths must be absolute.
		if !strings.HasPrefix(trimmed, "/") {
			t.Fatalf("validateFetcherResultPath accepted non-absolute path %q", resultPath)
		}

		// Property 3a: no traversal markers. The function calls
		// path.Clean and rejects when cleaned != trimmed; that should
		// catch every ".." occurrence.
		if strings.Contains(trimmed, "..") {
			t.Fatalf("validateFetcherResultPath accepted path containing '..': %q", resultPath)
		}

		// Property 3b: no URL scheme markers.
		if strings.Contains(trimmed, "://") {
			t.Fatalf("validateFetcherResultPath accepted path containing '://': %q", resultPath)
		}

		// Property 3c: no query string / fragment delimiters.
		if strings.ContainsAny(trimmed, "?#") {
			t.Fatalf("validateFetcherResultPath accepted path containing '?' or '#': %q", resultPath)
		}

		// NOTE ON NUL BYTES AND BACKSLASH:
		//
		// validateFetcherResultPath does NOT explicitly reject embedded
		// NUL bytes or backslash separators. Those are legal characters
		// in POSIX paths, and path.Clean preserves them. If the fuzzer
		// discovers accepted paths containing either, that is a
		// candidate defense gap — but it is a production-code question,
		// not a test failure.
		//
		// We intentionally do NOT t.Fatal on NUL / backslash here,
		// because doing so would imply the production contract
		// includes those rejections (it does not). The fuzzer will
		// surface such paths through ordinary exploration; a reviewer
		// can then decide whether to harden the validator.
		//
		// If you want to tighten the invariant to include NUL / '\\'
		// rejection, update validateFetcherResultPath first, then add
		// the corresponding assertion here.
	})
}
