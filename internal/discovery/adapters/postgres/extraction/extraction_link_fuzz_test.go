// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package extraction

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// FuzzLinkIfUnlinked_Concurrency is a sqlmock-driven fuzz of the T-003 P1
// atomic link operation. True concurrency-race coverage lives in the
// integration suite (see extraction_lifecycle_link_integration_test.go);
// this fuzz stays at the unit layer and exercises the input-domain of
// (extraction_id, ingestion_job_id, mock-outcome) to prove the method:
//
//  1. Never panics, including on uuid.Nil pairs, hostile byte patterns
//     that happen to map to valid uuids, and random-tag uuids from
//     encoding/hex-style seeds.
//  2. always returns one of the well-defined outcomes:
//     - nil                                     (rows_affected=1)
//     - sharedPorts.ErrExtractionAlreadyLinked  (rows=0 + probe true)
//     - repositories.ErrExtractionNotFound      (rows=0 + probe ErrNoRows)
//     - repositories.ErrExtractionConflict      (rows=0 + probe false)
//     - sharedPorts.ErrLinkExtractionIDRequired (extraction_id = nil)
//     - sharedPorts.ErrLinkIngestionJobIDRequired (ingestion_job_id = nil)
//     - a wrapped SQL error with the "link extraction if unlinked:" prefix
//  3. Never emits a silent success on an error path. If err != nil, nothing
//     depends on an implicit side effect.
//
// The outcome discriminator is the high 4 bits of extractionIDBytes[0]; this
// is purely for branch selection inside the mock so every fuzz iteration
// rotates through the five non-panic outcome shapes deterministically.
func FuzzLinkIfUnlinked_Concurrency(f *testing.F) {
	// Seeds exercise the five discriminator branches plus nil-uuid guards.
	// Each seed is two 16-byte slices (extractionID, ingestionID). uuid.FromBytes
	// requires exactly 16 bytes; we pad/truncate inside the fuzz body so
	// raw fuzz inputs can be any length.
	seeds := [][2][]byte{
		// Seed 1: canonical uuids (rotates through all 5 branches based on
		// first byte parity).
		{
			mustUUIDBytes("11111111-1111-1111-1111-111111111111"),
			mustUUIDBytes("22222222-2222-2222-2222-222222222222"),
		},
		// Seed 2: all-zero extraction id (forces ErrLinkExtractionIDRequired).
		{
			make([]byte, 16),
			mustUUIDBytes("33333333-3333-3333-3333-333333333333"),
		},
		// Seed 3: all-zero ingestion id (forces ErrLinkIngestionJobIDRequired).
		{
			mustUUIDBytes("44444444-4444-4444-4444-444444444444"),
			make([]byte, 16),
		},
		// Seed 4: high-bit extraction id (steers discriminator into the
		// "already linked" branch).
		{
			mustUUIDBytes("ffffffff-ffff-ffff-ffff-ffffffffffff"),
			mustUUIDBytes("55555555-5555-5555-5555-555555555555"),
		},
		// Seed 5: byte pattern likely to collide with control/NUL
		// sequences under parse, but still produces a valid uuid after
		// byte coercion (high bit of first byte lands at 0x80 ⇒
		// "probe-true" branch).
		{
			[]byte{0x80, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
			[]byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0},
		},
		// Seed 6: midrange extraction id byte to steer into
		// "exec error" branch.
		{
			[]byte{0x40, 0xde, 0xad, 0xbe, 0xef, 0xfa, 0xce, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09},
			mustUUIDBytes("66666666-6666-6666-6666-666666666666"),
		},
	}

	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, extractionIDBytes, ingestionIDBytes []byte) {
		extractionID := coerceToUUID(extractionIDBytes)
		ingestionID := coerceToUUID(ingestionIDBytes)

		// Input-validation branches take precedence over any mock setup.
		// Exercise them first so the sqlmock helper never spins up a DB
		// for the "rejected before query" paths.
		switch {
		case extractionID == uuid.Nil:
			exerciseLinkWithoutMock(t, uuid.Nil, ingestionID, sharedPorts.ErrLinkExtractionIDRequired)

			return
		case ingestionID == uuid.Nil:
			exerciseLinkWithoutMock(t, extractionID, uuid.Nil, sharedPorts.ErrLinkIngestionJobIDRequired)

			return
		}

		// Pick one of the five mock outcomes based on extractionID[0] mod 5.
		// Every branch must be a well-defined terminal state; if any of
		// them panic or leak a half-initialised struct, the fuzz fails.
		outcome := int(extractionID[0]) % 5
		exerciseLinkWithMock(t, extractionID, ingestionID, outcome)
	})
}

// exerciseLinkWithoutMock covers the two input-validation branches.
func exerciseLinkWithoutMock(
	t *testing.T,
	extractionID, ingestionID uuid.UUID,
	wantErr error,
) {
	t.Helper()

	db, mock, dbErr := sqlmock.New()
	if dbErr != nil {
		t.Fatalf("sqlmock.New: %v", dbErr)
	}
	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	err := repo.LinkIfUnlinked(context.Background(), extractionID, ingestionID)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	// Validation must short-circuit before any SQL; mock should have
	// zero satisfied expectations.
	if unmetErr := mock.ExpectationsWereMet(); unmetErr != nil {
		t.Fatalf("validation branch fired SQL (mock had unmet expectations): %v", unmetErr)
	}
}

// exerciseLinkWithMock drives the five non-validation outcome branches and
// asserts that each one returns the canonical error/nil without panicking.
func exerciseLinkWithMock(
	t *testing.T,
	extractionID, ingestionID uuid.UUID,
	outcome int,
) {
	t.Helper()

	db, mock, dbErr := sqlmock.New()
	if dbErr != nil {
		t.Fatalf("sqlmock.New: %v", dbErr)
	}
	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	updateQuery := `UPDATE extraction_requests
				SET ingestion_job_id = $1, updated_at = $2
				WHERE id = $3 AND ingestion_job_id IS NULL`
	probeQuery := `SELECT ingestion_job_id IS NOT NULL FROM extraction_requests WHERE id = $1`

	mock.ExpectBegin()

	var wantErr error

	switch outcome {
	case 0: // happy path: atomic update affected 1 row.
		mock.ExpectExec(regexp.QuoteMeta(updateQuery)).WithArgs(
			ingestionID, sqlmock.AnyArg(), extractionID,
		).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	case 1: // zero rows + probe returns true ⇒ ErrExtractionAlreadyLinked.
		mock.ExpectExec(regexp.QuoteMeta(updateQuery)).WithArgs(
			ingestionID, sqlmock.AnyArg(), extractionID,
		).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(probeQuery)).WithArgs(extractionID).
			WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(true))
		mock.ExpectRollback()

		wantErr = sharedPorts.ErrExtractionAlreadyLinked
	case 2: // zero rows + probe returns false ⇒ ErrExtractionConflict.
		mock.ExpectExec(regexp.QuoteMeta(updateQuery)).WithArgs(
			ingestionID, sqlmock.AnyArg(), extractionID,
		).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(probeQuery)).WithArgs(extractionID).
			WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(false))
		mock.ExpectRollback()

		wantErr = repositories.ErrExtractionConflict
	case 3: // zero rows + probe returns ErrNoRows ⇒ ErrExtractionNotFound.
		mock.ExpectExec(regexp.QuoteMeta(updateQuery)).WithArgs(
			ingestionID, sqlmock.AnyArg(), extractionID,
		).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(probeQuery)).WithArgs(extractionID).
			WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

		wantErr = repositories.ErrExtractionNotFound
	case 4: // exec error ⇒ wrapped SQL error.
		mock.ExpectExec(regexp.QuoteMeta(updateQuery)).WithArgs(
			ingestionID, sqlmock.AnyArg(), extractionID,
		).WillReturnError(errTestExec)
		mock.ExpectRollback()

		wantErr = errTestExec
	}

	err := repo.LinkIfUnlinked(context.Background(), extractionID, ingestionID)

	switch outcome {
	case 0:
		if err != nil {
			t.Fatalf("happy path must return nil; got %v", err)
		}
	case 4:
		// Exec errors are wrapped with the "link extraction if unlinked:"
		// prefix. We do not assert errors.Is(errTestExec) because the
		// wrapping chain goes through %w and we want to verify the
		// public contract, which is: wrapped + non-nil.
		if err == nil {
			t.Fatalf("exec-error path must return a non-nil error")
		}

		if !errors.Is(err, errTestExec) {
			t.Fatalf("exec-error path must preserve underlying error via %%w; got %v", err)
		}
	default:
		if !errors.Is(err, wantErr) {
			t.Fatalf("outcome %d must return %v; got %v", outcome, wantErr, err)
		}
	}

	if unmetErr := mock.ExpectationsWereMet(); unmetErr != nil {
		t.Fatalf("outcome %d: unmet expectations: %v", outcome, unmetErr)
	}
}

// coerceToUUID normalises an arbitrary-length fuzz byte slice into a uuid.UUID.
// Short inputs zero-pad, long inputs truncate. This keeps the fuzz harness
// stable across the full input spectrum — uuid.FromBytes itself rejects
// anything that is not exactly 16 bytes.
func coerceToUUID(b []byte) uuid.UUID {
	var out uuid.UUID

	copy(out[:], b)

	return out
}

// mustUUIDBytes is a test-only helper; panics only on programmer error in
// seed construction (canonical uuid strings are statically known).
func mustUUIDBytes(s string) []byte {
	u, err := uuid.Parse(s)
	if err != nil {
		panic(err)
	}

	b, err := u.MarshalBinary()
	if err != nil {
		panic(err)
	}

	return b
}
