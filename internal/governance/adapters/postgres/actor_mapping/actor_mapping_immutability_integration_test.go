// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Integration tests for actor_mapping immutability + TOCTOU resistance
// (Taura Security pentest finding 28/04/2026 / fix-actor-mapping-pseudonymization-bypass).
//
// The unit-level tests at actor_mapping_immutable_sqlmock_test.go pin the
// post-fix SQL shape and per-call comparison logic. These integration
// scenarios prove the same contract holds end-to-end against a real
// PostgreSQL where sqlmock cannot observe transaction serialisation, row
// locking, or the INSERT ... ON CONFLICT DO NOTHING + SELECT compare path
// under contention.
//
// Coverage matrix (acceptance criteria from the dispatch):
//
//	AC1 → TestIntegration_ActorMappingImmutability_NewActor_PersistsAndReturnsEntity
//	AC2 → TestIntegration_ActorMappingImmutability_IdempotentSameValues_NoMutation
//	AC3/AC4 → TestIntegration_ActorMappingImmutability_DifferentPayload_ReturnsImmutable
//	AC5 → TestIntegration_ActorMappingImmutability_OverRedacted_ReturnsImmutable
//	AC5 (race) → TestIntegration_ActorMappingImmutability_OverRedacted_ConcurrentPlaintextAttacks_AllFail
//	AC7 → TestIntegration_ActorMappingImmutability_DeleteAfterPseudonymize_Succeeds
//	AC8 → TestIntegration_ActorMappingImmutability_ConcurrentDifferentPayloads_NoMutation
//	AC8 → TestIntegration_ActorMappingImmutability_ConcurrentIdenticalPayloads_AllSucceedIdempotently
//	AC8 → TestIntegration_ActorMappingImmutability_ConcurrentFreshActor_OneInsertsRestSeeWinner
//
// Package `actormapping` (not `_test`) so the tests use the Repository's
// exported API directly. Build-tag `integration` keeps the unit build clean
// per docs/PROJECT_RULES.md.
package actormapping

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/tests/integration"
)

// -----------------------------------------------------------------------------
// AC1 — Fresh actor_id: INSERT ... ON CONFLICT DO NOTHING RETURNING yields
// the new row. End-to-end persistence check confirms the row is readable
// after the transaction commits, with PII intact.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_NewActor_PersistsAndReturnsEntity(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Alice Fresh"
		email := "alice.fresh@example.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-int-ac1", &displayName, &email)
		require.NoError(t, err)

		result, err := repo.Upsert(ctx, mapping)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "actor-int-ac1", result.ActorID)
		require.NotNil(t, result.DisplayName)
		assert.Equal(t, "Alice Fresh", *result.DisplayName)
		require.NotNil(t, result.Email)
		assert.Equal(t, "alice.fresh@example.com", *result.Email)
		require.False(t, result.CreatedAt.IsZero())
		require.False(t, result.UpdatedAt.IsZero())

		// Independent read confirms the row committed with PII intact.
		fetched, err := repo.GetByActorID(ctx, "actor-int-ac1")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.NotNil(t, fetched.DisplayName)
		assert.Equal(t, "Alice Fresh", *fetched.DisplayName)
		require.NotNil(t, fetched.Email)
		assert.Equal(t, "alice.fresh@example.com", *fetched.Email)
	})
}

// -----------------------------------------------------------------------------
// AC2 — Repeat PUT with identical payload returns the existing row without
// mutating updated_at. The new INSERT ... DO NOTHING + SELECT compare path
// is idempotent: zero rows inserted, comparison passes, existing entity
// returned.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_IdempotentSameValues_NoMutation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Bob Idempotent"
		email := "bob.idem@example.com"

		first, err := entities.NewActorMapping(ctx, "actor-int-ac2", &displayName, &email)
		require.NoError(t, err)

		original, err := repo.Upsert(ctx, first)
		require.NoError(t, err)
		require.NotNil(t, original)
		originalCreatedAt := original.CreatedAt
		originalUpdatedAt := original.UpdatedAt

		// Build a fresh entity with the same identity fields but a different
		// (later) UpdatedAt timestamp. The new path must NOT propagate the
		// caller's UpdatedAt because the row is not modified.
		second, err := entities.NewActorMapping(ctx, "actor-int-ac2", &displayName, &email)
		require.NoError(t, err)

		result, err := repo.Upsert(ctx, second)
		require.NoError(t, err, "identical payload upsert must succeed idempotently")
		require.NotNil(t, result)
		assert.Equal(t, "actor-int-ac2", result.ActorID)
		require.NotNil(t, result.DisplayName)
		assert.Equal(t, "Bob Idempotent", *result.DisplayName)
		require.NotNil(t, result.Email)
		assert.Equal(t, "bob.idem@example.com", *result.Email)

		// created_at and updated_at must remain pinned to the FIRST insert.
		// This proves the conflict path performed a SELECT rather than an UPDATE.
		// Use time.Equal so sub-second drift cannot slip past Unix-seconds rounding.
		assert.True(t, originalCreatedAt.Equal(result.CreatedAt),
			"created_at must survive the idempotent path (got %v, want %v)", result.CreatedAt, originalCreatedAt)
		assert.True(t, originalUpdatedAt.Equal(result.UpdatedAt),
			"updated_at must NOT advance on the idempotent path; row was not modified (got %v, want %v)", result.UpdatedAt, originalUpdatedAt)
	})
}

// -----------------------------------------------------------------------------
// AC3/AC4 — Repeat PUT with a different display_name or email returns
// ErrActorMappingImmutable. The persisted row must remain untouched.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_DifferentPayload_ReturnsImmutable(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		originalName := "Carol Original"
		originalEmail := "carol@example.com"

		seed, err := entities.NewActorMapping(ctx, "actor-int-ac3", &originalName, &originalEmail)
		require.NoError(t, err)

		_, err = repo.Upsert(ctx, seed)
		require.NoError(t, err, "seed upsert should succeed")

		// Attempt to mutate identity fields via PUT with a different email.
		differentEmail := "carol.different@example.com"
		mutationAttempt, err := entities.NewActorMapping(ctx, "actor-int-ac3", &originalName, &differentEmail)
		require.NoError(t, err)

		result, err := repo.Upsert(ctx, mutationAttempt)
		require.Error(t, err, "mutating an identity field must be rejected")
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrActorMappingImmutable,
			"the rejection must surface ErrActorMappingImmutable so handlers map it to 409")

		// Independent read proves the row was not touched.
		fetched, err := repo.GetByActorID(ctx, "actor-int-ac3")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.NotNil(t, fetched.DisplayName)
		assert.Equal(t, "Carol Original", *fetched.DisplayName,
			"persisted display_name must survive the rejected mutation")
		require.NotNil(t, fetched.Email)
		assert.Equal(t, "carol@example.com", *fetched.Email,
			"persisted email must survive the rejected mutation")
	})
}

// -----------------------------------------------------------------------------
// AC5 — Pentest PoC at the repository layer. Insert a mapping, pseudonymize
// it (PII → [REDACTED]), then attempt to PUT plaintext PII over the
// [REDACTED] row. The new comparison path must reject the attack and leave
// the row pseudonymized.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_OverRedacted_ReturnsImmutable(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Dave Sensitive"
		email := "dave.sensitive@example.com"

		seed, err := entities.NewActorMapping(ctx, "actor-int-ac5", &displayName, &email)
		require.NoError(t, err)

		_, err = repo.Upsert(ctx, seed)
		require.NoError(t, err, "seed upsert should succeed")

		// Pseudonymize: persisted row becomes ([REDACTED], [REDACTED]).
		// nil tx makes the repo open its own tenant-scoped transaction —
		// adequate at the integration layer since we're not coupling with
		// streaming emission here.
		err = repo.PseudonymizeWithTx(ctx, nil, "actor-int-ac5")
		require.NoError(t, err)

		redactedBefore, err := repo.GetByActorID(ctx, "actor-int-ac5")
		require.NoError(t, err)
		require.True(t, redactedBefore.IsRedacted(), "row must be redacted before the attack")

		// Pentest PoC: attacker sends plaintext PII. The post-fix path
		// compares the stored [REDACTED]/[REDACTED] to the attacker payload,
		// sees they differ, and rejects.
		attackerName := "Attacker Plaintext"
		attackerEmail := "attacker@evil.example"
		attack, err := entities.NewActorMapping(ctx, "actor-int-ac5", &attackerName, &attackerEmail)
		require.NoError(t, err)

		result, err := repo.Upsert(ctx, attack)
		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrActorMappingImmutable,
			"plaintext-over-[REDACTED] attack must be rejected with ErrActorMappingImmutable")

		// Persisted row remains pseudonymized.
		after, err := repo.GetByActorID(ctx, "actor-int-ac5")
		require.NoError(t, err)
		require.NotNil(t, after)
		require.True(t, after.IsRedacted(),
			"persisted row must remain [REDACTED]/[REDACTED] after the rejected attack")
	})
}

// -----------------------------------------------------------------------------
// AC7 — DELETE (right-to-erasure) must continue to work after the row has
// been pseudonymized. Insert → pseudonymize → delete → confirm absence.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_DeleteAfterPseudonymize_Succeeds(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Eve Erasable"
		email := "eve@eraseme.example"

		seed, err := entities.NewActorMapping(ctx, "actor-int-ac7", &displayName, &email)
		require.NoError(t, err)
		_, err = repo.Upsert(ctx, seed)
		require.NoError(t, err)

		err = repo.PseudonymizeWithTx(ctx, nil, "actor-int-ac7")
		require.NoError(t, err)

		err = repo.Delete(ctx, "actor-int-ac7")
		require.NoError(t, err, "Delete after pseudonymize must still succeed (GDPR right-to-erasure)")

		result, err := repo.GetByActorID(ctx, "actor-int-ac7")
		require.ErrorIs(t, err, ErrActorMappingNotFound)
		require.Nil(t, result)
	})
}

// -----------------------------------------------------------------------------
// AC8 — Concurrency / TOCTOU.
//
// Scenario A: Row exists with payload P0. N goroutines concurrently call
// Upsert with N DIFFERENT payloads (all P1..Pn distinct from P0). Under
// the post-fix design every goroutine takes the conflict path
// (INSERT ... DO NOTHING returns zero rows), executes the SELECT inside
// its own transaction, sees a payload that differs from its own, and
// returns ErrActorMappingImmutable.
//
// Contract:
//
//	successCount   = 0
//	immutableCount = N
//	persisted row  = unchanged (P0)
//
// This is the load-bearing scenario for the pseudonymization-bypass
// regression: under the old upsert-with-COALESCE path the last writer
// won and overwrote P0; under the new path every concurrent writer is
// rejected.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_ConcurrentDifferentPayloads_NoMutation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		const concurrency = 12

		// Seed the row with payload P0.
		originalName := "Frank Original"
		originalEmail := "frank.original@example.com"
		seed, err := entities.NewActorMapping(ctx, "actor-int-ac8-diff", &originalName, &originalEmail)
		require.NoError(t, err)
		_, err = repo.Upsert(ctx, seed)
		require.NoError(t, err)

		// Launch N goroutines, each with a unique attacker payload that
		// differs from P0 and from every other goroutine.
		var (
			successes      atomic.Int64
			immutableHits  atomic.Int64
			otherErrors    atomic.Int64
			otherErrorMu   sync.Mutex
			otherErrorList []error
			wg             sync.WaitGroup
		)
		wg.Add(concurrency)

		for i := 0; i < concurrency; i++ {
			i := i

			go func() {
				defer wg.Done()

				// Each goroutine produces a distinct payload to maximize
				// contention diversity. Identity fields differ on every call.
				name := "attacker-name-" + itoa(i)
				email := "attacker-" + itoa(i) + "@evil.example"

				attempt, attemptErr := entities.NewActorMapping(ctx, "actor-int-ac8-diff", &name, &email)
				if attemptErr != nil {
					otherErrors.Add(1)

					otherErrorMu.Lock()
					otherErrorList = append(otherErrorList, attemptErr)
					otherErrorMu.Unlock()

					return
				}

				_, err := repo.Upsert(ctx, attempt)
				if err == nil {
					successes.Add(1)

					return
				}

				if errors.Is(err, ErrActorMappingImmutable) {
					immutableHits.Add(1)

					return
				}

				otherErrors.Add(1)

				otherErrorMu.Lock()
				otherErrorList = append(otherErrorList, err)
				otherErrorMu.Unlock()
			}()
		}

		wg.Wait()

		assert.Equal(t, int64(0), successes.Load(),
			"no concurrent writer with a differing payload may succeed against an existing row")
		assert.Equal(t, int64(concurrency), immutableHits.Load(),
			"every concurrent writer with a differing payload must receive ErrActorMappingImmutable")
		require.Equal(t, int64(0), otherErrors.Load(),
			"unexpected non-immutable errors observed: %+v", otherErrorList)

		// Persisted row must remain pinned to P0 — no attacker payload won.
		fetched, err := repo.GetByActorID(ctx, "actor-int-ac8-diff")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.NotNil(t, fetched.DisplayName)
		assert.Equal(t, "Frank Original", *fetched.DisplayName,
			"persisted display_name must remain the seeded value under concurrent mutation attempts")
		require.NotNil(t, fetched.Email)
		assert.Equal(t, "frank.original@example.com", *fetched.Email,
			"persisted email must remain the seeded value under concurrent mutation attempts")
	})
}

// -----------------------------------------------------------------------------
// AC8 — Concurrency, idempotent variant.
//
// Scenario B: Row exists with payload P0. N goroutines concurrently call
// Upsert with IDENTICAL payload P0. Every goroutine takes the conflict
// path, the SELECT-compare succeeds, and Upsert returns success. No
// goroutine sees an error; the row is unchanged.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_ConcurrentIdenticalPayloads_AllSucceedIdempotently(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		const concurrency = 12

		displayName := "Grace Stable"
		email := "grace.stable@example.com"

		seed, err := entities.NewActorMapping(ctx, "actor-int-ac8-idem", &displayName, &email)
		require.NoError(t, err)

		original, err := repo.Upsert(ctx, seed)
		require.NoError(t, err)
		require.NotNil(t, original)
		originalUpdatedAt := original.UpdatedAt

		var (
			successes    atomic.Int64
			errorsCount  atomic.Int64
			errorsMu     sync.Mutex
			observedErrs []error
			wg           sync.WaitGroup
		)
		wg.Add(concurrency)

		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()

				// Identical payload across all goroutines.
				name := displayName
				mail := email

				attempt, attemptErr := entities.NewActorMapping(ctx, "actor-int-ac8-idem", &name, &mail)
				if attemptErr != nil {
					errorsCount.Add(1)
					errorsMu.Lock()
					observedErrs = append(observedErrs, attemptErr)
					errorsMu.Unlock()

					return
				}

				_, err := repo.Upsert(ctx, attempt)
				if err == nil {
					successes.Add(1)

					return
				}

				errorsCount.Add(1)

				errorsMu.Lock()
				observedErrs = append(observedErrs, err)
				errorsMu.Unlock()
			}()
		}

		wg.Wait()

		require.Equal(t, int64(0), errorsCount.Load(),
			"identical-payload concurrent upserts must all succeed; observed errors: %+v", observedErrs)
		assert.Equal(t, int64(concurrency), successes.Load(),
			"every concurrent writer with the seeded payload must receive idempotent success")

		// The row was not modified — updated_at remains pinned.
		fetched, err := repo.GetByActorID(ctx, "actor-int-ac8-idem")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		assert.True(t, originalUpdatedAt.Equal(fetched.UpdatedAt),
			"updated_at must remain pinned: no mutation occurred under identical-payload concurrency (got %v, want %v)", fetched.UpdatedAt, originalUpdatedAt)
	})
}

// -----------------------------------------------------------------------------
// AC5 (race) — concurrent plaintext attacks against a [REDACTED] row.
//
// Scenario C: Row is pseudonymized to [REDACTED]/[REDACTED]. N goroutines
// concurrently submit different plaintext PII payloads. Each goroutine
// must be rejected with ErrActorMappingImmutable; the persisted row must
// stay [REDACTED]/[REDACTED].
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_OverRedacted_ConcurrentPlaintextAttacks_AllFail(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		const concurrency = 12

		// Seed and pseudonymize the row.
		seedName := "Henry Sensitive"
		seedEmail := "henry.sensitive@example.com"

		seed, err := entities.NewActorMapping(ctx, "actor-int-ac5-race", &seedName, &seedEmail)
		require.NoError(t, err)
		_, err = repo.Upsert(ctx, seed)
		require.NoError(t, err)

		err = repo.PseudonymizeWithTx(ctx, nil, "actor-int-ac5-race")
		require.NoError(t, err)

		// Launch concurrent attackers with distinct plaintext payloads.
		var (
			successes      atomic.Int64
			immutableHits  atomic.Int64
			otherErrors    atomic.Int64
			otherErrorMu   sync.Mutex
			otherErrorList []error
			wg             sync.WaitGroup
		)
		wg.Add(concurrency)

		for i := 0; i < concurrency; i++ {
			i := i

			go func() {
				defer wg.Done()

				name := "race-attacker-" + itoa(i)
				email := "race-attacker-" + itoa(i) + "@evil.example"

				attempt, attemptErr := entities.NewActorMapping(ctx, "actor-int-ac5-race", &name, &email)
				if attemptErr != nil {
					otherErrors.Add(1)
					otherErrorMu.Lock()
					otherErrorList = append(otherErrorList, attemptErr)
					otherErrorMu.Unlock()

					return
				}

				_, err := repo.Upsert(ctx, attempt)
				if err == nil {
					successes.Add(1)

					return
				}

				if errors.Is(err, ErrActorMappingImmutable) {
					immutableHits.Add(1)

					return
				}

				otherErrors.Add(1)

				otherErrorMu.Lock()
				otherErrorList = append(otherErrorList, err)
				otherErrorMu.Unlock()
			}()
		}

		wg.Wait()

		assert.Equal(t, int64(0), successes.Load(),
			"no plaintext attacker may overwrite a [REDACTED] row, even under concurrency")
		assert.Equal(t, int64(concurrency), immutableHits.Load(),
			"every concurrent plaintext attacker must receive ErrActorMappingImmutable")
		require.Equal(t, int64(0), otherErrors.Load(),
			"unexpected non-immutable errors observed: %+v", otherErrorList)

		// Persisted row remains [REDACTED]/[REDACTED].
		fetched, err := repo.GetByActorID(ctx, "actor-int-ac5-race")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.True(t, fetched.IsRedacted(),
			"persisted row must remain pseudonymized after concurrent plaintext attacks")
	})
}

// -----------------------------------------------------------------------------
// AC8 — Concurrency on a fresh actor_id.
//
// Scenario D: No row exists. N goroutines concurrently call Upsert with
// N DIFFERENT payloads. PostgreSQL serialises the writes on the unique
// constraint:
//
//   - Exactly one INSERT wins (RETURNING yields a row).
//   - The other N-1 take the conflict path, SELECT the winner's row,
//     compare it to their own payload, see a mismatch, and return
//     ErrActorMappingImmutable.
//
// Contract:
//
//	successes      = 1   (the winner)
//	immutableHits  = N-1 (losers see a row that doesn't match their payload)
//	persisted row  = matches one of the N submitted payloads
//
// This proves the new path closes the TOCTOU window for the not-yet-existing
// case: an attacker cannot race with a legitimate creator to overwrite the
// row mid-flight.
// -----------------------------------------------------------------------------

func TestIntegration_ActorMappingImmutability_ConcurrentFreshActor_OneInsertsRestSeeWinner(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := NewRepository(h.Provider())
		ctx := h.Ctx()

		const concurrency = 12

		type submitted struct {
			displayName string
			email       string
		}

		submittedPayloads := make([]submitted, concurrency)
		for i := 0; i < concurrency; i++ {
			submittedPayloads[i] = submitted{
				displayName: "fresh-racer-" + itoa(i),
				email:       "fresh-racer-" + itoa(i) + "@example.com",
			}
		}

		var (
			successes      atomic.Int64
			immutableHits  atomic.Int64
			otherErrors    atomic.Int64
			otherErrorMu   sync.Mutex
			otherErrorList []error
			wg             sync.WaitGroup
		)
		wg.Add(concurrency)

		// Use a single context across all goroutines so they share the
		// same tenant scope. The tenant search_path is applied per-tx by
		// pgcommon helpers — no shared state hazard here.
		for i := 0; i < concurrency; i++ {
			i := i

			go func() {
				defer wg.Done()

				name := submittedPayloads[i].displayName
				mail := submittedPayloads[i].email

				attempt, attemptErr := entities.NewActorMapping(ctx, "actor-int-ac8-fresh", &name, &mail)
				if attemptErr != nil {
					otherErrors.Add(1)
					otherErrorMu.Lock()
					otherErrorList = append(otherErrorList, attemptErr)
					otherErrorMu.Unlock()

					return
				}

				_, err := repo.Upsert(ctx, attempt)
				if err == nil {
					successes.Add(1)

					return
				}

				if errors.Is(err, ErrActorMappingImmutable) {
					immutableHits.Add(1)

					return
				}

				otherErrors.Add(1)

				otherErrorMu.Lock()
				otherErrorList = append(otherErrorList, err)
				otherErrorMu.Unlock()
			}()
		}

		wg.Wait()

		require.Equal(t, int64(0), otherErrors.Load(),
			"unexpected non-immutable errors observed: %+v", otherErrorList)
		assert.Equal(t, int64(1), successes.Load(),
			"exactly one writer must win the INSERT on a fresh actor_id")
		assert.Equal(t, int64(concurrency-1), immutableHits.Load(),
			"every losing writer must receive ErrActorMappingImmutable (they submitted a different payload than the winner)")

		// Persisted row must match one of the N submitted payloads.
		fetched, err := repo.GetByActorID(ctx, "actor-int-ac8-fresh")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.NotNil(t, fetched.DisplayName)
		require.NotNil(t, fetched.Email)

		matched := false

		for _, p := range submittedPayloads {
			if *fetched.DisplayName == p.displayName && *fetched.Email == p.email {
				matched = true

				break
			}
		}

		require.True(t, matched,
			"persisted row (display_name=%q, email=%q) must match one of the submitted payloads",
			*fetched.DisplayName, *fetched.Email)
	})
}

// itoa is a local, allocation-free integer-to-string helper for building
// distinct attacker payloads without pulling strconv into the test file.
// The integers are bounded by the loop count (<= 100), so a tiny lookup
// table is sufficient.
func itoa(i int) string {
	const digits = "0123456789"

	if i < 0 {
		return "neg"
	}

	if i < 10 {
		return string(digits[i])
	}

	if i < 100 {
		return string(digits[i/10]) + string(digits[i%10])
	}

	// Fallback for safety; tests use concurrency <= 16.
	return "big"
}
