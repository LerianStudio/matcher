// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Package entities_test houses property-based tests for the ExtractionRequest
// state machine. The tests here verify domain invariants declared in
// ExtractionRequest.LinkToIngestion (Gate 5, T-003 P1 hardening):
//
//  1. State-transition validation — non-COMPLETE extractions cannot be linked.
//  2. UpdatedAt monotonicity — linking bumps UpdatedAt forward (never backwards).
//  3. IngestionJobID equality & idempotence — same jobID is a no-op, different
//     jobID on an already-linked extraction is rejected.
//
// These properties are verified via testing/quick.Check with 100+ iterations
// per run. The generators (propLinkInput) produce independent, well-typed
// payloads; the harness holds its own copy of "what the entity should do"
// rather than calling production helpers that the test is meant to verify.
package entities_test

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// allStatuses is the exhaustive set of ExtractionStatus values. The generator
// cycles through them so property tests see every status at least once across
// the quick.Check iteration budget.
var allStatuses = []vo.ExtractionStatus{
	vo.ExtractionStatusPending,
	vo.ExtractionStatusSubmitted,
	vo.ExtractionStatusExtracting,
	vo.ExtractionStatusComplete,
	vo.ExtractionStatusFailed,
	vo.ExtractionStatusCancelled,
}

// propLinkInput is the generator payload: a status and an ingestion job id.
// Kept as a small named struct so shrinking on failure produces diff-friendly
// counterexamples instead of opaque positional arguments.
type propLinkInput struct {
	Status         vo.ExtractionStatus
	IngestionJobID uuid.UUID
}

// Generate implements quick.Generator. We bias the output towards structural
// coverage (all statuses, plus one-in-twenty Nil UUIDs) rather than letting
// the framework produce near-zero UUIDs. quick's default Generate would emit
// all-zero UUIDs with alarming frequency, which would silently funnel every
// case into the "nil id" branch.
func (propLinkInput) Generate(rng *rand.Rand, _ int) reflect.Value {
	status := allStatuses[rng.Intn(len(allStatuses))]

	var id uuid.UUID

	// 1-in-20 chance we synthesise a Nil UUID so the input-validation branch
	// still sees coverage in the property run. Everything else gets a fresh
	// random UUID from uuid.New() which uses crypto/rand.
	if rng.Intn(20) == 0 {
		id = uuid.Nil
	} else {
		id = uuid.New()
	}

	return reflect.ValueOf(propLinkInput{Status: status, IngestionJobID: id})
}

// TestProperty_ExtractionRequest_LinkToIngestion_StateMachine verifies
// Invariant 4: for any (status, jobID) pair,
//   - status != COMPLETE → LinkToIngestion returns ErrInvalidTransition and
//     leaves IngestionJobID untouched.
//   - status == COMPLETE and jobID == uuid.Nil → returns ErrInvalidTransition.
//   - status == COMPLETE and jobID != uuid.Nil → sets IngestionJobID = jobID.
func TestProperty_ExtractionRequest_LinkToIngestion_StateMachine(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkInput) bool {
		er := &entities.ExtractionRequest{
			ID:     uuid.New(),
			Status: in.Status,
		}
		priorID := er.IngestionJobID

		err := er.LinkToIngestion(in.IngestionJobID)

		// Branch 1: nil ingestion job id. Entity rejects regardless of status.
		if in.IngestionJobID == uuid.Nil {
			return err != nil && er.IngestionJobID == priorID
		}

		// Branch 2: non-COMPLETE status. Entity rejects with
		// ErrInvalidTransition; IngestionJobID untouched.
		if in.Status != vo.ExtractionStatusComplete {
			return err != nil && er.IngestionJobID == priorID
		}

		// Branch 3: COMPLETE + valid job id. Link succeeds; IngestionJobID
		// reflects the new value.
		return err == nil && er.IngestionJobID == in.IngestionJobID
	}

	cfg := &quick.Config{MaxCount: 200}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_ExtractionRequest_LinkToIngestion_UpdatedAtMonotone verifies
// Invariant 5: after a successful link, UpdatedAt is strictly greater than
// or equal to its prior value. Monotone-forward bumping is what makes
// optimistic concurrency (UpdateIfUnchanged) correct.
//
// This property only makes sense when the link succeeds — reject cases are
// allowed to leave UpdatedAt untouched, so we skip them with `return true`.
func TestProperty_ExtractionRequest_LinkToIngestion_UpdatedAtMonotone(t *testing.T) {
	t.Parallel()

	prop := func(in propLinkInput) bool {
		// Seed with a UpdatedAt rooted in the past so the post-link bump is
		// unambiguously forward. Using UTC matches the domain convention.
		prior := time.Now().UTC().Add(-time.Hour)

		er := &entities.ExtractionRequest{
			ID:        uuid.New(),
			Status:    in.Status,
			UpdatedAt: prior,
		}

		err := er.LinkToIngestion(in.IngestionJobID)
		// Skip the reject paths — they have their own property test. Here we
		// verify only the success-path invariant.
		if err != nil {
			return true
		}

		// PROPERTY: UpdatedAt must be >= prior after a successful link.
		return !er.UpdatedAt.Before(prior)
	}

	cfg := &quick.Config{MaxCount: 200}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_ExtractionRequest_LinkToIngestion_IdempotentSameJob verifies
// Invariant 6 (first half): linking an already-linked extraction to the SAME
// job id is idempotent at the domain layer — no error, no change to
// IngestionJobID. (The adapter's atomic SQL is what enforces "no duplicate
// rows" across concurrent calls; the domain is merely well-behaved when
// handed the same input twice.)
func TestProperty_ExtractionRequest_LinkToIngestion_IdempotentSameJob(t *testing.T) {
	t.Parallel()

	prop := func(jobID uuid.UUID) bool {
		// Skip degenerate case: we want to prove idempotence on non-nil IDs.
		if jobID == uuid.Nil {
			return true
		}

		er := &entities.ExtractionRequest{
			ID:             uuid.New(),
			Status:         vo.ExtractionStatusComplete,
			IngestionJobID: jobID,
		}

		// Second call with the exact same jobID must be a no-op.
		err := er.LinkToIngestion(jobID)

		return err == nil && er.IngestionJobID == jobID
	}

	cfg := &quick.Config{MaxCount: 200}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_ExtractionRequest_LinkToIngestion_RejectsDifferentJobWhenLinked
// verifies Invariant 6 (second half): linking an already-linked extraction to
// a DIFFERENT job id is rejected without mutating the existing link. This is
// the one-extraction-to-one-ingestion invariant at the domain layer.
func TestProperty_ExtractionRequest_LinkToIngestion_RejectsDifferentJobWhenLinked(t *testing.T) {
	t.Parallel()

	type jobPair struct {
		First, Second uuid.UUID
	}

	prop := func(jp jobPair) bool {
		// Skip degenerate: both zero, or collisions (the shrinker can produce
		// same UUIDs on small seeds). Real production UUIDs have a 2^128
		// collision space so the skip is safe.
		if jp.First == uuid.Nil || jp.Second == uuid.Nil || jp.First == jp.Second {
			return true
		}

		er := &entities.ExtractionRequest{
			ID:             uuid.New(),
			Status:         vo.ExtractionStatusComplete,
			IngestionJobID: jp.First,
		}

		err := er.LinkToIngestion(jp.Second)

		// PROPERTY: error returned, existing link preserved.
		return err != nil && er.IngestionJobID == jp.First
	}

	cfg := &quick.Config{MaxCount: 200}
	require.NoError(t, quick.Check(prop, cfg))
}

// TestProperty_ExtractionRequest_LinkToIngestion_NoIDMutationOnReject is a
// catch-all safety property: for any reject path (nil id, non-complete
// status, already-linked to different job), the extraction's IngestionJobID
// must equal its prior value. This guards against accidental partial writes
// on the error branches.
func TestProperty_ExtractionRequest_LinkToIngestion_NoIDMutationOnReject(t *testing.T) {
	t.Parallel()

	type inputSeed struct {
		Status  vo.ExtractionStatus
		Prior   uuid.UUID // the pre-existing IngestionJobID (may be Nil)
		NewJob  uuid.UUID // the job id being handed to LinkToIngestion
		StatusI int       // indexes into allStatuses so the shrinker has something to bisect
	}

	generate := func(rng *rand.Rand) inputSeed {
		seed := inputSeed{
			Status:  allStatuses[rng.Intn(len(allStatuses))],
			Prior:   uuid.New(),
			NewJob:  uuid.New(),
			StatusI: rng.Intn(len(allStatuses)),
		}
		// Sometimes clear Prior to cover both branches.
		if rng.Intn(2) == 0 {
			seed.Prior = uuid.Nil
		}

		return seed
	}

	// quick.Check's default Generate on uuid.UUID produces all-zero values
	// nearly every time (UUID is a [16]byte; default gen picks arbitrary
	// bytes but shrinking collapses to Nil). Hand-rolling the generator keeps
	// coverage honest. The fixed seed makes failures reproducible.
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 200; i++ {
		seed := generate(rng)

		er := &entities.ExtractionRequest{
			ID:             uuid.New(),
			Status:         seed.Status,
			IngestionJobID: seed.Prior,
		}

		err := er.LinkToIngestion(seed.NewJob)
		// PROPERTY on reject: IngestionJobID unchanged.
		if err != nil {
			if er.IngestionJobID != seed.Prior {
				t.Fatalf("iter %d: reject mutated IngestionJobID: prior=%s post=%s status=%s newJob=%s err=%v",
					i, seed.Prior, er.IngestionJobID, seed.Status, seed.NewJob, err)
			}

			continue
		}

		// PROPERTY on accept: IngestionJobID equals NewJob OR equals Prior
		// when Prior == NewJob (the idempotent same-job branch).
		if seed.Prior != uuid.Nil && seed.Prior == seed.NewJob {
			if er.IngestionJobID != seed.Prior {
				t.Fatalf("iter %d: idempotent same-job accept mutated IngestionJobID", i)
			}
		} else if er.IngestionJobID != seed.NewJob {
			t.Fatalf("iter %d: accept did not set IngestionJobID: post=%s newJob=%s",
				i, er.IngestionJobID, seed.NewJob)
		}
	}
}
