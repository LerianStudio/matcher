// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build chaos

// Chaos tests for actor_mapping immutability under Postgres fault injection.
//
// Background — Taura Security pentest finding (28/04/2026):
// The post-fix Upsert path uses INSERT ... ON CONFLICT (actor_id) DO NOTHING
// followed by an in-transaction SELECT-compare. The whole sequence runs
// inside a single tenant-scoped transaction so a concurrent UPDATE cannot
// race between the INSERT and the SELECT.
//
// These chaos tests verify that property holds when the database connection
// is poisoned mid-flight — connection drops, latency spikes, and complete
// outages MUST NOT produce a state where a row was partially written or
// where a [REDACTED] row was silently overwritten with attacker-supplied
// plaintext PII.
//
// Scenario coverage:
//
//	1. TestChaos_ActorMapping_Upsert_DBConnectionDropMidTransaction_NoDataLoss
//	   reset_peer toxic during a mutation attempt on an existing row →
//	   caller sees an error, row UNCHANGED.
//
//	2. TestChaos_ActorMapping_Upsert_DBConnectionDropAfterInsert_NoCorruption
//	   Fresh actor_id + reset_peer during the INSERT-or-RETURNING window →
//	   either the row exists with exactly the submitted payload, or no row
//	   exists. NEVER a partial state.
//
//	3. TestChaos_ActorMapping_Upsert_PseudonymizedRowUnderLatency_StillRejectsAttacker
//	   Pseudonymized row + 1s latency injection + concurrent plaintext
//	   attacks → every attack returns ErrActorMappingImmutable (or a
//	   timeout); persisted row stays [REDACTED]/[REDACTED].
//
//	4. TestChaos_ActorMapping_CreateOrGet_GracefulOnDBUnreachable
//	   PG proxy disabled → Upsert returns a wrapped error, no panic,
//	   no nil-with-no-error.
//
//	5. TestChaos_ActorMapping_Upsert_NetworkPartition_RecoveryConsistent
//	   Disable PG proxy → first Upsert fails → re-enable proxy → idempotent
//	   re-PUT with same payload succeeds; row matches original.
package chaos

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	actormapping "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/actor_mapping"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// -----------------------------------------------------------------------------
// Helpers — keep test bodies focused on the chaos contract.
// -----------------------------------------------------------------------------

// newActorMappingChaosRepo returns a repository wired to the proxied Postgres
// connection. All Upsert calls flow through Toxiproxy, so toxics injected on
// h.PGProxy take effect on each repository call.
func newActorMappingChaosRepo(h *ChaosHarness) *actormapping.Repository {
	return actormapping.NewRepository(h.Provider())
}

// readActorMappingDirect reads (display_name, email) directly from Postgres,
// bypassing Toxiproxy. Used to verify on-disk state after a chaos event
// without depending on the proxied connection that may still be poisoned.
//
// Returns (nil, nil, nil) when the row does not exist.
func readActorMappingDirect(t *testing.T, db *sql.DB, actorID string) (displayName, email *string, exists bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		dn sql.NullString
		em sql.NullString
	)

	err := db.QueryRowContext(ctx,
		`SELECT display_name, email FROM actor_mapping WHERE actor_id = $1`, actorID,
	).Scan(&dn, &em)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false
	}

	require.NoError(t, err, "direct read of actor_mapping row %q", actorID)

	if dn.Valid {
		v := dn.String
		displayName = &v
	}

	if em.Valid {
		v := em.String
		email = &v
	}

	return displayName, email, true
}

// truncateActorMappingDirect wipes the actor_mapping table on the direct
// connection. Each test is self-contained — we reset only the rows we care
// about rather than calling h.ResetDatabase (which truncates everything and
// re-seeds, slowing the suite).
func truncateActorMappingDirect(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `TRUNCATE TABLE actor_mapping`)
	require.NoError(t, err, "truncate actor_mapping for chaos test isolation")
}

// seedActorMappingDirect inserts a row directly (bypassing the repo) so the
// chaos scenario starts from a known state without any toxics interfering.
func seedActorMappingDirect(t *testing.T, db *sql.DB, actorID, displayName, email string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
		INSERT INTO actor_mapping (actor_id, display_name, email, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
	`, actorID, displayName, email)
	require.NoError(t, err, "seed actor_mapping row %q", actorID)
}

// pseudonymizeActorMappingDirect updates the row to [REDACTED]/[REDACTED]
// using the direct connection. Mirrors the production
// PseudonymizeWithTx side-effect for chaos scenarios that need a redacted
// starting state without going through the proxied path.
func pseudonymizeActorMappingDirect(t *testing.T, db *sql.DB, actorID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
		UPDATE actor_mapping
		SET display_name = '[REDACTED]', email = '[REDACTED]', updated_at = NOW()
		WHERE actor_id = $1
	`, actorID)
	require.NoError(t, err, "pseudonymize actor_mapping row %q (direct)", actorID)
}

// -----------------------------------------------------------------------------
// Scenario 1: Connection drop mid-transaction → no data loss on the existing row.
// -----------------------------------------------------------------------------

// TestChaos_ActorMapping_Upsert_DBConnectionDropMidTransaction_NoDataLoss
// verifies that an abrupt TCP reset during an Upsert against an existing row
// cannot silently overwrite the persisted identity fields. The caller MUST
// receive an error; the on-disk row MUST be identical to its pre-call state.
//
// This is the load-bearing chaos scenario for the pseudonymization-bypass
// fix: even if the connection dies in the middle of the
// INSERT-DO-NOTHING + SELECT-compare sequence, the transaction MUST roll
// back (Postgres semantics), and the row MUST NOT be partially updated.
func TestChaos_ActorMapping_Upsert_DBConnectionDropMidTransaction_NoDataLoss(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")

	directDB := h.DirectDB(t)
	truncateActorMappingDirect(t, directDB)

	const actorID = "chaos-actor-drop-mid-tx"

	// Seed an existing mapping with a known PII payload — this is the row
	// the attacker / failed call will try (and fail) to mutate.
	seedActorMappingDirect(t, directDB, actorID, "Original Owner", "original@example.com")

	// Inject reset_peer immediately (0 bytes through before reset) so any
	// query through the proxy will fail.
	h.InjectPGResetPeer(t, 0)

	repo := newActorMappingChaosRepo(h)

	attackerName := "Mutation Attempt"
	attackerEmail := "mutation.attempt@evil.example"

	mutationAttempt, err := entities.NewActorMapping(h.Ctx(), actorID, &attackerName, &attackerEmail)
	require.NoError(t, err, "construct mutation-attempt entity")

	// Bounded timeout — the test should NOT hang. If reset_peer doesn't
	// surface within a few seconds, that's a finding on its own.
	upsertCtx, cancel := context.WithTimeout(h.Ctx(), 5*time.Second)
	defer cancel()

	result, upsertErr := repo.Upsert(upsertCtx, mutationAttempt)
	assert.Error(t, upsertErr, "Upsert through poisoned proxy must return an error")
	assert.Nil(t, result, "Upsert returning an error must not also return a value")

	// CRITICAL: persisted row remains exactly as seeded.
	dn, em, exists := readActorMappingDirect(t, directDB, actorID)
	require.True(t, exists, "row must still exist after the failed Upsert")
	require.NotNil(t, dn, "display_name must remain non-NULL")
	require.NotNil(t, em, "email must remain non-NULL")
	assert.Equal(t, "Original Owner", *dn,
		"display_name MUST survive a mid-transaction connection drop unchanged")
	assert.Equal(t, "original@example.com", *em,
		"email MUST survive a mid-transaction connection drop unchanged")
}

// -----------------------------------------------------------------------------
// Scenario 2: Connection drop after INSERT → no partial-state corruption on fresh actor_id.
// -----------------------------------------------------------------------------

// TestChaos_ActorMapping_Upsert_DBConnectionDropAfterInsert_NoCorruption
// targets the fresh-actor_id path: no row exists for actor_id when Upsert
// is called, then the connection dies somewhere between the INSERT and the
// commit. PostgreSQL transactional semantics guarantee either the INSERT
// committed (row visible with exactly the submitted payload) or it rolled
// back (no row at all). NEVER a partial state.
//
// We can't deterministically interrupt at a precise byte boundary from
// userland, so we use a combination of latency + a tight context timeout
// to drive cancellation while the round-trip is in flight.
func TestChaos_ActorMapping_Upsert_DBConnectionDropAfterInsert_NoCorruption(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")

	directDB := h.DirectDB(t)
	truncateActorMappingDirect(t, directDB)

	const actorID = "chaos-actor-drop-after-insert"

	// Verify the actor_id does NOT exist before we start.
	_, _, exists := readActorMappingDirect(t, directDB, actorID)
	require.False(t, exists, "precondition: actor_id must not exist before chaos run")

	// Inject 2 seconds of downstream latency on every PG response.
	// Combined with a 500ms context timeout, this drives client-side
	// cancellation while the INSERT round-trip is in flight.
	h.InjectPGLatency(t, 2000, 0)

	repo := newActorMappingChaosRepo(h)

	freshName := "Fresh Owner"
	freshEmail := "fresh@example.com"

	mapping, err := entities.NewActorMapping(h.Ctx(), actorID, &freshName, &freshEmail)
	require.NoError(t, err, "construct fresh actor_mapping entity")

	upsertCtx, cancel := context.WithTimeout(h.Ctx(), 500*time.Millisecond)
	defer cancel()

	_, upsertErr := repo.Upsert(upsertCtx, mapping)

	// Either succeeds (latency happened to fit under 500ms across retries —
	// unlikely with a 2s latency floor) or fails with deadline/cancellation.
	// We accept either outcome; the invariant is the on-disk state.
	if upsertErr != nil {
		t.Logf("Upsert failed under latency+timeout as expected: %v", upsertErr)
	}

	// Read the row via the DIRECT connection (no toxic). Two valid states:
	//   A) row exists with exactly (freshName, freshEmail) — commit landed.
	//   B) row does not exist — transaction was rolled back / never committed.
	// ANY OTHER STATE is corruption.
	dn, em, exists := readActorMappingDirect(t, directDB, actorID)
	if !exists {
		// State B — clean rollback. Acceptable.
		t.Logf("State B (clean rollback): row does not exist after chaos run")

		return
	}

	// State A — commit landed. Verify integrity.
	require.NotNil(t, dn, "if row exists, display_name MUST be non-NULL (matches submitted payload)")
	require.NotNil(t, em, "if row exists, email MUST be non-NULL (matches submitted payload)")
	assert.Equal(t, freshName, *dn,
		"if row exists, display_name MUST match submitted payload exactly — partial state is corruption")
	assert.Equal(t, freshEmail, *em,
		"if row exists, email MUST match submitted payload exactly — partial state is corruption")
}

// -----------------------------------------------------------------------------
// Scenario 3: Pseudonymized row + latency + concurrent attackers → still immutable.
// -----------------------------------------------------------------------------

// TestChaos_ActorMapping_Upsert_PseudonymizedRowUnderLatency_StillRejectsAttacker
// is the chaos analogue of the AC5 integration test: a row has been
// pseudonymized to [REDACTED]/[REDACTED] and multiple concurrent attackers
// submit plaintext PII. Adding 1 second of latency expands the TOCTOU
// window dramatically — under the OLD COALESCE-based upsert path this
// would have surfaced any race. Under the NEW INSERT-DO-NOTHING +
// in-transaction-SELECT path, every attacker must be rejected with
// ErrActorMappingImmutable; timeouts are also acceptable. What is NOT
// acceptable is silent overwrite of the [REDACTED] row.
func TestChaos_ActorMapping_Upsert_PseudonymizedRowUnderLatency_StillRejectsAttacker(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")

	directDB := h.DirectDB(t)
	truncateActorMappingDirect(t, directDB)

	const (
		actorID     = "chaos-actor-redacted-under-latency"
		concurrency = 6
	)

	// Seed the row, then pseudonymize it directly.
	seedActorMappingDirect(t, directDB, actorID, "Pre-Redaction Owner", "preredaction@example.com")
	pseudonymizeActorMappingDirect(t, directDB, actorID)

	// Sanity check: row is now [REDACTED]/[REDACTED].
	dnBefore, emBefore, exists := readActorMappingDirect(t, directDB, actorID)
	require.True(t, exists, "row must exist before chaos run")
	require.NotNil(t, dnBefore)
	require.NotNil(t, emBefore)
	require.Equal(t, "[REDACTED]", *dnBefore, "precondition: display_name pseudonymized")
	require.Equal(t, "[REDACTED]", *emBefore, "precondition: email pseudonymized")

	// Inject 1 second of latency — widens the gap between the INSERT
	// (DO NOTHING) and the SELECT-compare, surfacing any non-atomicity bug.
	h.InjectPGLatency(t, 1000, 0)

	repo := newActorMappingChaosRepo(h)

	var (
		successes      atomic.Int64
		immutableHits  atomic.Int64
		timeouts       atomic.Int64
		otherErrors    atomic.Int64
		otherErrorsMu  sync.Mutex
		otherErrorList []error
		wg             sync.WaitGroup
	)

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			defer wg.Done()

			attackerName := "chaos-attacker-" + chaosItoa(i)
			attackerEmail := "chaos-attacker-" + chaosItoa(i) + "@evil.example"

			attempt, attemptErr := entities.NewActorMapping(h.Ctx(), actorID, &attackerName, &attackerEmail)
			if attemptErr != nil {
				otherErrors.Add(1)
				otherErrorsMu.Lock()
				otherErrorList = append(otherErrorList, attemptErr)
				otherErrorsMu.Unlock()

				return
			}

			// Bounded per-call timeout: 4s allows latency + compare path to
			// complete; anything beyond is treated as a timeout (acceptable).
			callCtx, cancel := context.WithTimeout(h.Ctx(), 4*time.Second)
			defer cancel()

			_, err := repo.Upsert(callCtx, attempt)
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, governanceErrors.ErrActorMappingImmutable):
				immutableHits.Add(1)
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				timeouts.Add(1)
			default:
				otherErrors.Add(1)
				otherErrorsMu.Lock()
				otherErrorList = append(otherErrorList, err)
				otherErrorsMu.Unlock()
			}
		}()
	}

	wg.Wait()

	// CRITICAL invariants — independent of which buckets the attempts landed in:
	//   1. ZERO successes (no plaintext attacker may overwrite [REDACTED]).
	//   2. Row remains [REDACTED]/[REDACTED] on disk.
	assert.Equal(t, int64(0), successes.Load(),
		"no plaintext attacker may overwrite a [REDACTED] row under chaos (latency injection)")

	dnAfter, emAfter, exists := readActorMappingDirect(t, directDB, actorID)
	require.True(t, exists, "pseudonymized row must still exist after chaos run")
	require.NotNil(t, dnAfter)
	require.NotNil(t, emAfter)
	assert.Equal(t, "[REDACTED]", *dnAfter,
		"display_name MUST remain [REDACTED] after concurrent plaintext attacks under latency")
	assert.Equal(t, "[REDACTED]", *emAfter,
		"email MUST remain [REDACTED] after concurrent plaintext attacks under latency")

	t.Logf("attempts: immutableHits=%d timeouts=%d otherErrors=%d (otherErrorList=%+v)",
		immutableHits.Load(), timeouts.Load(), otherErrors.Load(), otherErrorList)
}

// -----------------------------------------------------------------------------
// Scenario 4: PG proxy disabled (complete outage) → graceful error, no panic.
// -----------------------------------------------------------------------------

// TestChaos_ActorMapping_CreateOrGet_GracefulOnDBUnreachable verifies that
// when Postgres is completely unreachable, Upsert returns a wrapped error
// rather than panicking or returning a zero-value pair (nil, nil). This is
// the Gate 2 (SRE) observability contract: server faults are surfaced via
// HandleSpanError + wrapped error chains, not silent nil-returns.
func TestChaos_ActorMapping_CreateOrGet_GracefulOnDBUnreachable(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")

	directDB := h.DirectDB(t)
	truncateActorMappingDirect(t, directDB)

	const actorID = "chaos-actor-db-unreachable"

	// Pre-check baseline: row does NOT exist.
	_, _, exists := readActorMappingDirect(t, directDB, actorID)
	require.False(t, exists)

	// Disable the PG proxy entirely (connection refused at the proxy port).
	h.DisablePGProxy(t)

	repo := newActorMappingChaosRepo(h)

	displayName := "Unreachable Owner"
	email := "unreachable@example.com"

	mapping, err := entities.NewActorMapping(h.Ctx(), actorID, &displayName, &email)
	require.NoError(t, err)

	callCtx, cancel := context.WithTimeout(h.Ctx(), 5*time.Second)
	defer cancel()

	// Run inside a goroutine so we can fail loudly on a hang.
	done := make(chan struct {
		result *entities.ActorMapping
		err    error
	}, 1)

	go func() {
		// Guard against panics — the test fails immediately if any panic
		// escapes. The repository is required to convert all failure modes
		// into errors.
		defer func() {
			if r := recover(); r != nil {
				done <- struct {
					result *entities.ActorMapping
					err    error
				}{nil, errors.New("PANIC in repo.Upsert under DB-unreachable chaos")}
			}
		}()

		result, err := repo.Upsert(callCtx, mapping)
		done <- struct {
			result *entities.ActorMapping
			err    error
		}{result, err}
	}()

	select {
	case outcome := <-done:
		assert.Error(t, outcome.err, "Upsert with PG unreachable MUST return an error")
		assert.Nil(t, outcome.result,
			"Upsert returning an error MUST NOT also return a non-nil entity (no nil-and-no-error path)")
	case <-time.After(10 * time.Second):
		t.Fatal("repo.Upsert hung under DB-unreachable chaos — graceful-degradation contract violated")
	}

	// Row must NOT exist on disk.
	_, _, exists = readActorMappingDirect(t, directDB, actorID)
	assert.False(t, exists,
		"no row may exist after Upsert against an unreachable Postgres")
}

// -----------------------------------------------------------------------------
// Scenario 5: Network partition + heal → idempotent re-PUT is consistent.
// -----------------------------------------------------------------------------

// TestChaos_ActorMapping_Upsert_NetworkPartition_RecoveryConsistent simulates
// a clean network partition (proxy disabled), then heals it. The second
// idempotent Upsert with the same payload MUST succeed and the persisted
// row must match the original payload exactly. This proves the
// INSERT-DO-NOTHING + compare path is resilient to transient outages
// (no leftover [poisoned] state from the failed first attempt).
func TestChaos_ActorMapping_Upsert_NetworkPartition_RecoveryConsistent(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")

	directDB := h.DirectDB(t)
	truncateActorMappingDirect(t, directDB)

	const actorID = "chaos-actor-partition-recovery"

	displayName := "Recovery Owner"
	email := "recovery@example.com"

	repo := newActorMappingChaosRepo(h)

	// Step 1: partition Postgres.
	h.DisablePGProxy(t)

	mapping, err := entities.NewActorMapping(h.Ctx(), actorID, &displayName, &email)
	require.NoError(t, err)

	failedCtx, failedCancel := context.WithTimeout(h.Ctx(), 5*time.Second)
	defer failedCancel()

	_, failedErr := repo.Upsert(failedCtx, mapping)
	require.Error(t, failedErr, "Upsert during partition MUST fail")

	// Verify no leftover row on disk after the partitioned attempt.
	_, _, exists := readActorMappingDirect(t, directDB, actorID)
	require.False(t, exists,
		"no row may exist after a failed partitioned Upsert (would indicate phantom commit)")

	// Step 2: heal the partition.
	h.EnablePGProxy(t)

	// Give the connection pool a moment to recover.
	require.Eventually(t, func() bool {
		probeCtx, probeCancel := context.WithTimeout(h.Ctx(), 3*time.Second)
		defer probeCancel()
		_, probeErr := repo.GetByActorID(probeCtx, "nonexistent-probe")

		// ErrActorMappingNotFound is the healthy response for a missing
		// row; any other error means the pool hasn't recovered yet.
		return errors.Is(probeErr, actormapping.ErrActorMappingNotFound)
	}, 15*time.Second, 500*time.Millisecond,
		"PG connection pool failed to recover after partition heal")

	// Step 3: idempotent re-PUT with the same payload.
	healedMapping, err := entities.NewActorMapping(h.Ctx(), actorID, &displayName, &email)
	require.NoError(t, err)

	healedCtx, healedCancel := context.WithTimeout(h.Ctx(), 10*time.Second)
	defer healedCancel()

	result, err := repo.Upsert(healedCtx, healedMapping)
	require.NoError(t, err, "post-heal Upsert MUST succeed")
	require.NotNil(t, result)
	require.NotNil(t, result.DisplayName)
	require.NotNil(t, result.Email)
	assert.Equal(t, displayName, *result.DisplayName)
	assert.Equal(t, email, *result.Email)

	// Final on-disk verification via direct connection.
	dn, em, exists := readActorMappingDirect(t, directDB, actorID)
	require.True(t, exists, "row must exist after partition heal + Upsert")
	require.NotNil(t, dn)
	require.NotNil(t, em)
	assert.Equal(t, displayName, *dn,
		"post-recovery display_name must match submitted payload exactly")
	assert.Equal(t, email, *em,
		"post-recovery email must match submitted payload exactly")
}

// chaosItoa is a tiny zero-allocation integer-to-string helper, mirroring
// the one used in the integration tests. Bounded to concurrency <= 100.
func chaosItoa(i int) string {
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

	return "big"
}
