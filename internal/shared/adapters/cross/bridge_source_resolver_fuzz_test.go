// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// FuzzBridgeSourceResolution asserts the T-003 bridge resolver never panics
// and never bypasses parameterised SQL regardless of how hostile the
// connection-id string payload is. Although the public port accepts a
// typed uuid.UUID, the fuzzer deliberately generates raw strings that a
// caller might coerce through uuid.Parse (e.g. when forwarding from a
// Fetcher webhook or a debug admin CLI). This keeps the attack surface
// honest: if any upstream ever routes user-controlled text into this
// adapter, the resolver must still:
//
//  1. Never panic.
//  2. Either return a typed result+nil error OR a wrapped error — no
//     partially populated BridgeSourceTarget.
//  3. Bind the string via the driver's $1 placeholder rather than
//     interpolating into the SQL literal.
//
// The sqlmock below asserts the parameterised query exactly, so any attempt
// to concatenate the fuzzed string into the SQL text would fail the
// ExpectationsWereMet contract — we rely on that as the "no SQL injection"
// proof.
//
// Success conditions:
//   - Never panics for any input, including NUL bytes, control chars,
//     classic injection payloads, and >10KB strings.
//   - For inputs that parse as a valid UUID, the resolver either returns a
//     populated BridgeSourceTarget (happy-path mock) or
//     ErrBridgeSourceUnresolvable (empty-row mock) — both non-panicking.
//   - For inputs that fail uuid.Parse, the test skips the adapter call
//     (upstream would have rejected earlier) and proves that the parser
//     itself never panics on the fuzzed string.
func FuzzBridgeSourceResolution(f *testing.F) {
	// Seeds cover: empty string, SQL injection payload, NUL bytes, control
	// chars, an oversized string, a unicode variant, and a UUID-shaped
	// non-UUID ("looks like one, isn't"). The uuid.Parse library is
	// strict about the canonical 36-char hyphenated form plus a handful
	// of permissible alternate shapes, so each seed below exercises a
	// distinct rejection path.
	seeds := []string{
		"",
		"'; DROP TABLE reconciliation_sources; --",
		"\x00\x01\x02connection-id",
		"\u202Emalicious-unicode-bidi",
		strings.Repeat("A", 10_000),
		"00000000-0000-0000-0000-00000000000Z", // one char off canonical
		"deadbeef-dead-beef-dead-beefdeadbeef", // hex-shaped but random
		uuid.Nil.String(),                      // canonical nil uuid
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, connectionIDRaw string) {
		// Clamp pathological lengths so the fuzzer does not eat memory
		// building multi-megabyte payloads; the production uuid.Parse
		// path rejects anything beyond 45 chars instantly anyway.
		if len(connectionIDRaw) > 64*1024 {
			connectionIDRaw = connectionIDRaw[:64*1024]
		}

		// Step 1: prove uuid.Parse itself never panics. A parse failure
		// here means the upstream adapter would have refused before
		// reaching the resolver, so we exit cleanly. A parse success
		// proceeds to the adapter call.
		connID, parseErr := uuid.Parse(connectionIDRaw)
		if parseErr != nil {
			// Parsing failed without panicking — that is the invariant
			// for this branch. Nothing further to test.
			return
		}

		// Step 2: exercise the adapter with the (now-typed) connection
		// id. We alternate between the "row present" and "no row" mock
		// shapes so both branches see fuzz coverage. The decisive byte
		// comes from the uuid itself (first byte parity).
		takeHappyPath := connID[0]%2 == 0

		db, mock, dbErr := sqlmock.New()
		if dbErr != nil {
			t.Fatalf("sqlmock.New: %v", dbErr)
		}
		defer func() { _ = db.Close() }()

		provider := testutil.NewMockProviderFromDB(t, db)

		adapter, newErr := NewBridgeSourceResolverAdapter(provider)
		if newErr != nil {
			t.Fatalf("construct adapter: %v", newErr)
		}

		mock.ExpectBegin()

		query := mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT id, context_id
				FROM reconciliation_sources
				WHERE type = 'FETCHER' AND config->>'connection_id' = $1::text
				ORDER BY created_at ASC
				LIMIT 2`,
		)).WithArgs(connID.String())

		if takeHappyPath {
			query.WillReturnRows(
				sqlmock.NewRows([]string{"id", "context_id"}).
					AddRow(uuid.New().String(), uuid.New().String()),
			)
			mock.ExpectCommit()
		} else {
			query.WillReturnRows(sqlmock.NewRows([]string{"id", "context_id"}))
			mock.ExpectRollback()
		}

		target, resolveErr := adapter.ResolveSourceForConnection(context.Background(), connID)

		// Nil-uuid must always be rejected explicitly with the
		// "connection id is required" sentinel, regardless of mock
		// setup. The uuid.Parse above would only produce uuid.Nil when
		// the seed/fuzz string is the canonical all-zero uuid. Because
		// in that case the adapter returns BEFORE the mock's
		// ExpectBegin fires, sqlmock's unmet-expectation check would
		// fail unless we stop the test here.
		if connID == uuid.Nil {
			if resolveErr == nil || !strings.Contains(resolveErr.Error(), "connection id is required") {
				t.Fatalf("nil uuid must be rejected; got err=%v target=%+v", resolveErr, target)
			}

			return
		}

		// All expectations must have been met — this is our proof that
		// the resolver routed the string through the parameterised
		// placeholder ($1) rather than concatenating it into the SQL.
		if unmetErr := mock.ExpectationsWereMet(); unmetErr != nil {
			t.Fatalf("unmet sqlmock expectations (sql-injection defence breach?): %v", unmetErr)
		}

		if takeHappyPath {
			if resolveErr != nil {
				t.Fatalf("happy path must return nil error; got %v", resolveErr)
			}

			if target.SourceID == uuid.Nil || target.ContextID == uuid.Nil {
				t.Fatalf("happy path must populate target ids; got %+v", target)
			}

			if target.Format != "json" {
				t.Fatalf("happy path must return json format; got %q", target.Format)
			}

			return
		}

		// No-row path must surface the canonical sentinel.
		if !errors.Is(resolveErr, sharedPorts.ErrBridgeSourceUnresolvable) {
			t.Fatalf("no-row path must return ErrBridgeSourceUnresolvable; got %v", resolveErr)
		}

		// Target must be zero-valued on error so callers cannot
		// accidentally consume partial data.
		if target.SourceID != uuid.Nil || target.ContextID != uuid.Nil || target.Format != "" {
			t.Fatalf("error path must return zero-valued target; got %+v", target)
		}
	})
}
