// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Property tests for the atomic LinkIfUnlinked repository method (T-003 P1
// hardening, Gate 5).
//
// These tests verify contract invariants against random inputs rather than
// specific scenarios:
//
//  1. Single-link idempotence — for any (extractionID, jobID) pair, linking
//     succeeds exactly once. A second call with the SAME jobID on an
//     already-linked row returns ErrExtractionAlreadyLinked (never silent 0
//     rows affected).
//  2. Different-job rejection — once extraction E is linked to job J1,
//     LinkIfUnlinked(E, J2) returns ErrExtractionAlreadyLinked and never
//     overwrites J1 with J2.
//  3. Simulated concurrent race — via sqlmock ordering, two LinkIfUnlinked
//     calls are set up with one winning (rows_affected=1) and the other
//     losing (rows_affected=0 + probe returns "linked"). The loser must
//     surface ErrExtractionAlreadyLinked. Real concurrency is integration-
//     test territory; here we prove the RETURN CONTRACT is race-safe.
//
// The tests cannot exercise true multi-goroutine contention with sqlmock —
// sqlmock expectations are ordered. What they prove is that the CONTRACT
// the production code exposes — atomic UPDATE + follow-up probe —
// correctly maps every SQL outcome to the correct sentinel. Integration
// tests (Gate 6) handle the real-concurrency surface.
package extraction

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"regexp"
	"testing"
	"testing/quick"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// propLinkCall is the generator payload: a pair of UUIDs (extraction + job).
// Kept as a struct so quick's shrinker yields diff-friendly counterexamples.
type propLinkCall struct {
	ExtractionID uuid.UUID
	IngestionJob uuid.UUID
}

// Generate implements quick.Generator. We hand-roll so Nil UUIDs appear with
// controlled frequency (1-in-25) instead of the quick default which, with
// uuid.UUID being a [16]byte alias, shrinks to all-zeros nearly every time.
func (propLinkCall) Generate(r *rand.Rand, _ int) reflect.Value {
	mk := func() uuid.UUID {
		if r.Intn(25) == 0 {
			return uuid.Nil
		}

		return uuid.New()
	}

	return reflect.ValueOf(propLinkCall{
		ExtractionID: mk(),
		IngestionJob: mk(),
	})
}

// linkUpdateSQL is the exact SQL the production code issues. Kept as a
// package-level constant so every expectation uses the same regex-quoted
// form and drift between test and implementation is a compile-time concern.
const linkUpdateSQL = `UPDATE extraction_requests
				SET ingestion_job_id = $1, updated_at = $2
				WHERE id = $3 AND ingestion_job_id IS NULL`

// linkProbeSQL is the follow-up probe SQL issued when UPDATE matches zero
// rows. The production code uses this to discriminate "not found" from
// "already linked".
const linkProbeSQL = `SELECT ingestion_job_id IS NOT NULL FROM extraction_requests WHERE id = $1`

// TestProperty_LinkIfUnlinked_ValidIDsAlwaysSucceedOnFirstCall verifies
// Invariant 1 (first half): for any pair of non-nil UUIDs, the first
// LinkIfUnlinked call against an unlinked row succeeds (err == nil).
//
// We model "unlinked row" as rows_affected=1 in sqlmock — that is the SQL
// semantics the production UPDATE statement is designed to produce when
// `ingestion_job_id IS NULL` at the time of execution.
func TestProperty_LinkIfUnlinked_ValidIDsAlwaysSucceedOnFirstCall(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkCall) bool {
		// Input-validation branch: the entity rejects zero UUIDs at the
		// port boundary. Don't model SQL for those cases — the property
		// for those inputs is "returns the specific required-id sentinel"
		// which is checked in its own property below.
		if in.ExtractionID == uuid.Nil || in.IngestionJob == uuid.Nil {
			return true
		}

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(linkUpdateSQL)).
			WithArgs(in.IngestionJob, sqlmock.AnyArg(), in.ExtractionID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err := repo.LinkIfUnlinked(context.Background(), in.ExtractionID, in.IngestionJob)

		// PROPERTY: valid inputs + unlinked row → nil error.
		return err == nil
	}

	// MaxCount=120 keeps the test fast; each iteration spins up an sqlmock
	// db which has non-trivial per-call overhead.
	cfg := &quick.Config{MaxCount: 120}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_LinkIfUnlinked_NilIDsAlwaysReturnValidationSentinel verifies
// Invariant 1 (second half): zero-valued UUID inputs ALWAYS short-circuit
// with the specific required-id sentinel, never reaching SQL.
func TestProperty_LinkIfUnlinked_NilIDsAlwaysReturnValidationSentinel(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkCall) bool {
		// Only exercise the property for inputs with at least one nil UUID.
		// Other inputs are covered by the happy-path property above.
		if in.ExtractionID != uuid.Nil && in.IngestionJob != uuid.Nil {
			return true
		}

		repo, mock, _ := setupMockRepository(t)

		err := repo.LinkIfUnlinked(context.Background(), in.ExtractionID, in.IngestionJob)

		// PROPERTY 1: nil extraction id short-circuits before any DB work.
		// PROPERTY 2: nil ingestion job id likewise.
		// sqlmock's ExpectationsWereMet() is asserted via the finish
		// pattern elsewhere; here we make it explicit because we skip
		// finish() for expectation-free paths.
		if in.ExtractionID == uuid.Nil && !errors.Is(err, sharedPorts.ErrLinkExtractionIDRequired) {
			return false
		}

		if in.ExtractionID != uuid.Nil && in.IngestionJob == uuid.Nil &&
			!errors.Is(err, sharedPorts.ErrLinkIngestionJobIDRequired) {
			return false
		}

		// Confirm no SQL was issued — the validation path must short-
		// circuit before Begin().
		return mock.ExpectationsWereMet() == nil
	}

	cfg := &quick.Config{MaxCount: 120}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_LinkIfUnlinked_AlreadyLinkedReturnsSentinel verifies
// Invariant 2: for any (extractionID, jobID) where the row is already
// linked, LinkIfUnlinked returns ErrExtractionAlreadyLinked — even if the
// caller happens to pass the same jobID that's already stored.
//
// At the repository layer, "already linked" manifests as rows_affected=0
// on the UPDATE + the follow-up probe returning TRUE.
func TestProperty_LinkIfUnlinked_AlreadyLinkedReturnsSentinel(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkCall) bool {
		if in.ExtractionID == uuid.Nil || in.IngestionJob == uuid.Nil {
			return true
		}

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(linkUpdateSQL)).
			WithArgs(in.IngestionJob, sqlmock.AnyArg(), in.ExtractionID).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(linkProbeSQL)).
			WithArgs(in.ExtractionID).
			WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(true))
		mock.ExpectRollback()

		err := repo.LinkIfUnlinked(context.Background(), in.ExtractionID, in.IngestionJob)

		// PROPERTY: already-linked row → ErrExtractionAlreadyLinked.
		return errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked)
	}

	cfg := &quick.Config{MaxCount: 120}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_LinkIfUnlinked_RowMissingReturnsNotFound verifies a
// complementary property to Invariant 2: when the UPDATE matches zero rows
// AND the probe finds no row at all, LinkIfUnlinked surfaces
// ErrExtractionNotFound rather than ErrExtractionAlreadyLinked.
//
// This is the "wrong ID" case — the sentinel must disambiguate it from
// the "already-linked" case so callers can act differently (retry vs.
// give up).
func TestProperty_LinkIfUnlinked_RowMissingReturnsNotFound(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkCall) bool {
		if in.ExtractionID == uuid.Nil || in.IngestionJob == uuid.Nil {
			return true
		}

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		// The production probe returns sql.ErrNoRows when the row doesn't
		// exist. sqlmock's empty-row-set shape + errors.Is(sql.ErrNoRows)
		// matching is how we model that outcome.
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(linkUpdateSQL)).
			WithArgs(in.IngestionJob, sqlmock.AnyArg(), in.ExtractionID).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(linkProbeSQL)).
			WithArgs(in.ExtractionID).
			WillReturnRows(sqlmock.NewRows([]string{"?column?"})) // empty => sql.ErrNoRows
		mock.ExpectRollback()

		err := repo.LinkIfUnlinked(context.Background(), in.ExtractionID, in.IngestionJob)

		// PROPERTY: row missing → ErrExtractionNotFound.
		return errors.Is(err, repositories.ErrExtractionNotFound)
	}

	cfg := &quick.Config{MaxCount: 120}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_LinkIfUnlinked_ConcurrentRaceContract_ExactlyOneWinner
// verifies Invariant 3 (race-safety contract): when two callers race to link
// the same extraction, the LinkIfUnlinked return values must distribute as
// exactly one nil (the winner) and one ErrExtractionAlreadyLinked (the
// loser). No silent success, no dual-success, no dual-failure.
//
// sqlmock does NOT let us spawn real concurrent transactions (its
// expectations are strictly ordered). What we prove here is the
// RETURN-VALUE CONTRACT: given the DB-level outcomes that a real race
// would produce (one UPDATE affects 1 row, the next affects 0), the repo
// code must translate those outcomes into (nil, ErrExtractionAlreadyLinked).
// The actual row-locking / MVCC / SERIALIZABLE semantics are the DB's job
// and are verified in integration tests (Gate 6).
func TestProperty_LinkIfUnlinked_ConcurrentRaceContract_ExactlyOneWinner(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkCall) bool {
		if in.ExtractionID == uuid.Nil || in.IngestionJob == uuid.Nil {
			return true
		}

		// Winner call: UPDATE affects 1 row.
		repoA, mockA, finishA := setupMockRepository(t)
		defer finishA()

		mockA.ExpectBegin()
		mockA.ExpectExec(regexp.QuoteMeta(linkUpdateSQL)).
			WithArgs(in.IngestionJob, sqlmock.AnyArg(), in.ExtractionID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mockA.ExpectCommit()

		errA := repoA.LinkIfUnlinked(context.Background(), in.ExtractionID, in.IngestionJob)

		// Loser call: UPDATE affects 0 rows (row is now linked); probe
		// reports true. The loser's ingestion job id can be the SAME as
		// the winner's (double-submit scenario) or DIFFERENT (two jobs
		// racing). In both cases the loser must get
		// ErrExtractionAlreadyLinked.
		repoB, mockB, finishB := setupMockRepository(t)
		defer finishB()

		// Simulate the loser submitting a potentially-different job id.
		// We use the winner's jobID half the time and a fresh one the
		// other half so both sub-cases are exercised across iterations.
		loserJob := in.IngestionJob
		if rand.Intn(2) == 0 {
			loserJob = uuid.New()
		}

		mockB.ExpectBegin()
		mockB.ExpectExec(regexp.QuoteMeta(linkUpdateSQL)).
			WithArgs(loserJob, sqlmock.AnyArg(), in.ExtractionID).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mockB.ExpectQuery(regexp.QuoteMeta(linkProbeSQL)).
			WithArgs(in.ExtractionID).
			WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(true))
		mockB.ExpectRollback()

		errB := repoB.LinkIfUnlinked(context.Background(), in.ExtractionID, loserJob)

		// PROPERTY: exactly one winner + exactly one loser with
		// ErrExtractionAlreadyLinked.
		return errA == nil && errors.Is(errB, sharedPorts.ErrExtractionAlreadyLinked)
	}

	cfg := &quick.Config{MaxCount: 80}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_LinkIfUnlinked_NeverSilentlySucceedsWithZeroRows is a
// negative property: the repo must NEVER return nil when the UPDATE
// reported zero rows affected AND the probe says the row is already linked.
// This guards against a future refactor that drops the follow-up probe
// and starts silently succeeding on 0-rows.
func TestProperty_LinkIfUnlinked_NeverSilentlySucceedsWithZeroRows(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkCall) bool {
		if in.ExtractionID == uuid.Nil || in.IngestionJob == uuid.Nil {
			return true
		}

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(linkUpdateSQL)).
			WithArgs(in.IngestionJob, sqlmock.AnyArg(), in.ExtractionID).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(linkProbeSQL)).
			WithArgs(in.ExtractionID).
			WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(true))
		mock.ExpectRollback()

		err := repo.LinkIfUnlinked(context.Background(), in.ExtractionID, in.IngestionJob)

		// PROPERTY: on rows_affected=0 + probe=true, err MUST be non-nil.
		// If a future refactor silently returns nil here, this test fails
		// loudly — which is the whole point of property-based regression
		// guards.
		return err != nil
	}

	cfg := &quick.Config{MaxCount: 120}
	require.NoError(t, quick.Check(prop, cfg))
}
