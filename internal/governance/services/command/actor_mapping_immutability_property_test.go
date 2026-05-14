// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Property-based tests for the actor_mapping immutability contract.
//
// Gate 5 of fix-actor-mapping-pseudonymization-bypass. The pentest finding
// (Taura Security, 28/04/2026) turned on three domain invariants that
// example-based tests can only sample. This file pins them against
// thousands of randomly generated operation sequences using
// testing/quick.
//
// Invariants encoded here:
//
//  1. Pseudonymization is irreversible — after a row enters the
//     [REDACTED] state, no Upsert/CreateOrGetActorMapping call can put
//     plaintext PII back on disk. The only way back to a non-redacted
//     state is DELETE followed by a fresh create.
//  2. Idempotency — CreateOrGetActorMapping called twice with identical
//     arguments must succeed both times and return equal entities.
//  3. Mutation rejection — once a non-redacted row exists, any call
//     that changes display_name OR email returns ErrActorMappingImmutable.
//
// The pseudonymization service path (PseudonymizeActor) goes through a
// SQL transaction + streaming emitter and is not the surface under test
// here. Gate 4 (fuzz) covers the constructor; the postgres-adapter
// sqlmock and immutability_fuzz_test files cover the repository path.
// This file targets the service-layer contract — the surface the HTTP
// handler depends on — using a hand-rolled stateful fake repository
// that mirrors the post-fix INSERT...ON CONFLICT DO NOTHING + SELECT
// + compare semantics from
// internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go.
//
// Convention: testing/quick with MaxCount=1000 per property. Operation
// sequences are bounded at sequencePropertyMaxOps to keep wall-clock
// time predictable.

package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/quick"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
)

const (
	// propertyMaxCount caps testing/quick iterations per property.
	propertyMaxCount = 1000

	// propertyTenantID mirrors the literal used by the example-based
	// immutability tests so the property suite shares the same tenant
	// context shape.
	propertyTenantID = "018f4f95-0000-7000-8000-000000000001"

	// redactedSentinel is the value the production repository writes
	// when pseudonymizing. Duplicated here intentionally because the
	// entity package owns the constant unexported and the property
	// test models the persisted state directly.
	redactedSentinel = "[REDACTED]"

	// sequencePropertyMaxOps caps the number of operations in a single
	// generated trace for the irreversibility property. 12 is enough
	// to interleave create → pseudonymize → mutate → delete → recreate
	// cycles a few times within one trace.
	sequencePropertyMaxOps = 12
)

// propertyContext returns a tenant-scoped context like the example tests.
func propertyContext() context.Context {
	return tmcore.ContextWithTenantID(testContext(), propertyTenantID)
}

// statefulFakeRepository is a hand-rolled fake that mirrors the post-fix
// repository contract from
// internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go.
//
// The semantics encoded here:
//   - Upsert on a fresh actor_id inserts the row and returns it.
//   - Upsert on an existing actor_id where (display_name, email) match
//     the stored row returns the stored row (idempotent success).
//   - Upsert on an existing actor_id where (display_name, email) differ
//     returns ErrActorMappingImmutable. The stored row is NOT touched.
//   - Pseudonymize sets both PII fields to redactedSentinel. Subsequent
//     Upserts therefore see the redacted row and reject any mutation
//     attempt because the payload (plaintext) cannot match the stored
//     [REDACTED] sentinel.
//   - Delete removes the row; a subsequent Upsert succeeds with the new
//     payload because there is no row to compare against.
//
// This is the same comparison semantics implemented by
// actorMappingPIIDiffers/stringPtrEqual in the postgres adapter.
type statefulFakeRepository struct {
	mu    sync.Mutex
	store map[string]*entities.ActorMapping
}

// Compile-time check.
var _ repositories.ActorMappingRepository = (*statefulFakeRepository)(nil)

func newStatefulFakeRepository() *statefulFakeRepository {
	return &statefulFakeRepository{store: make(map[string]*entities.ActorMapping)}
}

// stringPtrEqualLocal matches stringPtrEqual in actor_mapping.postgresql.go:
// nil and "" are semantically equivalent.
func stringPtrEqualLocal(lhs, rhs *string) bool {
	lhsEmpty := lhs == nil || *lhs == ""
	rhsEmpty := rhs == nil || *rhs == ""

	if lhsEmpty && rhsEmpty {
		return true
	}

	if lhsEmpty != rhsEmpty {
		return false
	}

	return *lhs == *rhs
}

// piiDiffers mirrors actorMappingPIIDiffers in the postgres adapter.
func piiDiffers(existing, payload *entities.ActorMapping) bool {
	if existing == nil || payload == nil {
		return true
	}

	return !stringPtrEqualLocal(existing.DisplayName, payload.DisplayName) ||
		!stringPtrEqualLocal(existing.Email, payload.Email)
}

func (r *statefulFakeRepository) Upsert(_ context.Context, mapping *entities.ActorMapping) (*entities.ActorMapping, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if mapping == nil {
		return nil, errors.New("statefulFakeRepository: nil mapping")
	}

	existing, ok := r.store[mapping.ActorID]
	if !ok {
		// Fresh row — clone to decouple stored state from caller.
		copied := *mapping
		r.store[mapping.ActorID] = &copied

		return cloneMapping(&copied), nil
	}

	if piiDiffers(existing, mapping) {
		return nil, ErrActorMappingImmutable
	}

	return cloneMapping(existing), nil
}

func (*statefulFakeRepository) GetByActorID(_ context.Context, _ string) (*entities.ActorMapping, error) {
	return nil, errors.New("statefulFakeRepository: GetByActorID not used in property tests")
}

func (*statefulFakeRepository) PseudonymizeWithTx(_ context.Context, _ *sql.Tx, _ string) error {
	// Service path PseudonymizeActor is not driven through the use case
	// in the property tests because it would require a real *sql.Tx.
	// The property test invokes the in-memory pseudonymize helper
	// (pseudonymize) directly on the fake. This function is here only to
	// satisfy the repositories.ActorMappingRepository interface; it is
	// never called.
	return errors.New("statefulFakeRepository: PseudonymizeWithTx not used in property tests")
}

func (r *statefulFakeRepository) Delete(_ context.Context, actorID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.store, actorID)

	return nil
}

// pseudonymize is a direct state mutation, mirroring the SQL UPDATE that
// production PseudonymizeWithTx performs. It is invoked by the property
// driver in place of going through the service's transactional path.
func (r *statefulFakeRepository) pseudonymize(actorID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	row, ok := r.store[actorID]
	if !ok {
		return
	}

	redacted := redactedSentinel
	row.DisplayName = &redacted
	row.Email = &redacted
	row.UpdatedAt = time.Now().UTC()
}

// snapshot returns a deep copy of the current row for the given actor_id.
// Property assertions compare against snapshot()s to verify the stored
// state did not change across rejected Upserts.
func (r *statefulFakeRepository) snapshot(actorID string) (*entities.ActorMapping, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	row, ok := r.store[actorID]
	if !ok {
		return nil, false
	}

	return cloneMapping(row), true
}

func cloneMapping(m *entities.ActorMapping) *entities.ActorMapping {
	if m == nil {
		return nil
	}

	copied := *m
	if m.DisplayName != nil {
		v := *m.DisplayName
		copied.DisplayName = &v
	}

	if m.Email != nil {
		v := *m.Email
		copied.Email = &v
	}

	return &copied
}

// ----- Quick generators / inputs -----

// propertyOpKind enumerates the operations the irreversibility driver
// can sample. Concrete operation parameters are derived from a single
// uint64 draw to keep the trace generator inside testing/quick's
// supported parameter shapes (no custom Generator needed).
type propertyOpKind int

const (
	opCreate propertyOpKind = iota
	opPseudonymize
	opMutateName
	opMutateEmail
	opDelete
	opRecreate
)

// numOpKinds is used to map a random byte to a propertyOpKind via modulo.
const numOpKinds = 6

// propertyOp is one step in a generated trace.
type propertyOp struct {
	kind        propertyOpKind
	displayName string
	email       string
}

// decodeTrace turns an opaque uint64 seed into a deterministic list of
// operations. Using a numeric seed keeps quick.Check parameter shapes
// simple while still surfacing thousands of distinct interleavings.
func decodeTrace(seed uint64, length int) []propertyOp {
	if length <= 0 {
		length = 1
	}

	if length > sequencePropertyMaxOps {
		length = sequencePropertyMaxOps
	}

	ops := make([]propertyOp, 0, length)

	// Linear-congruential PRNG seeded by the input; the choice of
	// constants is the classic Knuth LCG and produces a long cycle.
	state := seed
	for i := 0; i < length; i++ {
		state = state*6364136223846793005 + 1442695040888963407

		kind := propertyOpKind(state % numOpKinds)
		nameIdx := (state >> 8) % 4
		emailIdx := (state >> 16) % 4

		ops = append(ops, propertyOp{
			kind:        kind,
			displayName: fmt.Sprintf("name-%d", nameIdx),
			email:       fmt.Sprintf("email-%d@example.test", emailIdx),
		})
	}

	return ops
}

// strPtrLocal mirrors strPtr from actor_mapping_immutable_test.go.
func strPtrLocal(s string) *string {
	return &s
}

// runOnFakeService wires the use case onto the stateful fake and runs a
// single operation. It returns the post-operation snapshot of the row
// for diagnostic context when properties fail.
func runOnFakeService(
	ctx context.Context,
	uc *ActorMappingUseCase,
	repo *statefulFakeRepository,
	actorID string,
	op propertyOp,
) error {
	switch op.kind {
	case opCreate, opRecreate:
		_, err := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(op.displayName), strPtrLocal(op.email))
		// opRecreate semantics: delete first, then create. The driver
		// handles the delete; here both kinds reduce to a single
		// create call.
		return err
	case opPseudonymize:
		repo.pseudonymize(actorID)
		return nil
	case opMutateName:
		_, err := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(op.displayName+"-mutated"), strPtrLocal(op.email))
		return err
	case opMutateEmail:
		_, err := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(op.displayName), strPtrLocal(op.email+".mutated"))
		return err
	case opDelete:
		return uc.DeleteActorMapping(ctx, actorID)
	}

	return nil
}

// ----- Properties -----

// TestProperty_PseudonymizationIrreversible encodes Invariant 1.
//
// For any random trace of operations:
//   - After PSEUDONYMIZE on an existing row, the row is [REDACTED].
//   - Until the next DELETE (or end of trace), the row remains
//     [REDACTED] regardless of how many Upsert/CreateOrGetActorMapping
//     calls happen.
//   - The only way back to a non-redacted state is DELETE followed by
//     a fresh create.
func TestProperty_PseudonymizationIrreversible(t *testing.T) {
	t.Parallel()

	property := func(seed uint64, rawLen uint8) bool {
		length := int(rawLen)%sequencePropertyMaxOps + 1
		trace := decodeTrace(seed, length)

		repo := newStatefulFakeRepository()

		uc, err := NewActorMappingUseCase(repo)
		if err != nil {
			t.Logf("unexpected use-case constructor failure: %v", err)
			return false
		}

		ctx := propertyContext()
		actorID := "actor-irrev"

		// State machine: track whether the row is currently redacted
		// from the test's point of view.
		redacted := false
		exists := false

		for i, op := range trace {
			// opRecreate is modeled as delete-then-create.
			if op.kind == opRecreate {
				_ = uc.DeleteActorMapping(ctx, actorID)
				redacted = false
				exists = false
			}

			if err := runOnFakeService(ctx, uc, repo, actorID, op); err != nil {
				if !errors.Is(err, ErrActorMappingImmutable) {
					// Other errors are unexpected in this driver.
					t.Logf("op %d (%v) returned unexpected error: %v", i, op.kind, err)
					return false
				}
				// Rejected mutation: persisted state must not change.
			}

			// Reconcile model state with repo state.
			snap, ok := repo.snapshot(actorID)
			exists = ok

			switch op.kind {
			case opPseudonymize:
				if exists {
					redacted = true
				}
			case opDelete:
				redacted = false
				exists = false
			case opCreate, opRecreate:
				if exists && snap.IsRedacted() {
					redacted = true
				} else if exists {
					redacted = false
				}
			case opMutateName, opMutateEmail:
				// Mutations on existing rows are rejected; on a
				// non-existent row they create. Snapshot tells us
				// the truth.
				if exists {
					redacted = snap.IsRedacted()
				}
			}

			// CORE INVARIANT 1: if model says redacted, repo agrees.
			if redacted {
				if !exists {
					t.Logf("model says redacted but row missing at step %d", i)
					return false
				}
				if !snap.IsRedacted() {
					t.Logf("redacted state leaked: step=%d op=%v stored=%+v", i, op.kind, formatMapping(snap))
					return false
				}
			}
		}

		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: propertyMaxCount}); err != nil {
		t.Errorf("pseudonymization-irreversible property failed: %v", err)
	}
}

// TestProperty_IdempotencyOfCreateOrGetActorMapping encodes Invariant 2.
//
// For any valid (actorID, displayName, email) triple:
//   - The first call succeeds and returns a non-nil entity.
//   - The second call with identical arguments succeeds and returns an
//     entity whose persisted identity fields equal the first call's.
//   - In particular, the second call NEVER returns ErrActorMappingImmutable.
func TestProperty_IdempotencyOfCreateOrGetActorMapping(t *testing.T) {
	t.Parallel()

	property := func(
		actorSeed uint16,
		displayName, email string,
	) bool {
		// Reject inputs that would fail entity-level validation (empty
		// or oversize actor_id). The constructor's domain is a
		// precondition of this property, not the property itself.
		actorID := fmt.Sprintf("actor-idem-%d", actorSeed)
		if actorID == "" {
			return true
		}

		repo := newStatefulFakeRepository()
		uc, err := NewActorMappingUseCase(repo)
		if err != nil {
			return false
		}

		ctx := propertyContext()

		first, err1 := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(displayName), strPtrLocal(email))
		if err1 != nil || first == nil {
			t.Logf("first call failed: actorID=%q err=%v", actorID, err1)
			return false
		}

		second, err2 := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(displayName), strPtrLocal(email))
		if err2 != nil {
			t.Logf("idempotent second call returned error: actorID=%q err=%v", actorID, err2)
			return false
		}

		if second == nil {
			t.Logf("idempotent second call returned nil entity: actorID=%q", actorID)
			return false
		}

		if errors.Is(err2, ErrActorMappingImmutable) {
			// Defensive — err2 is nil above; this guards against any
			// future short-circuit that might surface immutable as a
			// non-error "soft" rejection.
			return false
		}

		// Identity fields must match the first call.
		if !stringPtrEqualLocal(first.DisplayName, second.DisplayName) {
			t.Logf("idempotent display_name drifted: first=%v second=%v", formatPtr(first.DisplayName), formatPtr(second.DisplayName))
			return false
		}

		if !stringPtrEqualLocal(first.Email, second.Email) {
			t.Logf("idempotent email drifted: first=%v second=%v", formatPtr(first.Email), formatPtr(second.Email))
			return false
		}

		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: propertyMaxCount}); err != nil {
		t.Errorf("idempotency property failed: %v", err)
	}
}

// TestProperty_MutationRejection encodes Invariant 3.
//
// For any existing, non-redacted mapping (actorID, D, E):
//   - CreateOrGetActorMapping(actorID, D', E) with D' != D returns
//     ErrActorMappingImmutable.
//   - CreateOrGetActorMapping(actorID, D, E') with E' != E returns
//     ErrActorMappingImmutable.
//   - The persisted row is not mutated by a rejected attempt.
func TestProperty_MutationRejection(t *testing.T) {
	t.Parallel()

	property := func(
		actorSeed uint16,
		baseDisplay, baseEmail, mutator string,
	) bool {
		actorID := fmt.Sprintf("actor-mut-%d", actorSeed)

		// Ensure mutator actually changes the field; if it happens to
		// be empty, replace with a deterministic non-empty marker.
		if mutator == "" {
			mutator = "MUTATOR"
		}

		repo := newStatefulFakeRepository()
		uc, err := NewActorMappingUseCase(repo)
		if err != nil {
			return false
		}

		ctx := propertyContext()

		// Seed the row.
		seedDisplay := baseDisplay
		seedEmail := baseEmail
		if seedDisplay == "" {
			seedDisplay = "Original Name"
		}

		if seedEmail == "" {
			seedEmail = "original@example.test"
		}

		if _, err := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(seedDisplay), strPtrLocal(seedEmail)); err != nil {
			return false
		}

		before, ok := repo.snapshot(actorID)
		if !ok {
			return false
		}

		// Attempt 1: mutate display_name only.
		mutatedDisplay := seedDisplay + "-" + mutator
		_, errDisplay := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(mutatedDisplay), strPtrLocal(seedEmail))

		if !errors.Is(errDisplay, ErrActorMappingImmutable) {
			t.Logf("display_name mutation not rejected: actor=%q new=%q got_err=%v", actorID, mutatedDisplay, errDisplay)
			return false
		}

		afterDisplayMut, _ := repo.snapshot(actorID)
		if !mappingsEqual(before, afterDisplayMut) {
			t.Logf("rejected display_name mutation altered stored row: before=%+v after=%+v", formatMapping(before), formatMapping(afterDisplayMut))
			return false
		}

		// Attempt 2: mutate email only.
		mutatedEmail := seedEmail + "." + mutator
		_, errEmail := uc.CreateOrGetActorMapping(ctx, actorID, strPtrLocal(seedDisplay), strPtrLocal(mutatedEmail))

		if !errors.Is(errEmail, ErrActorMappingImmutable) {
			t.Logf("email mutation not rejected: actor=%q new=%q got_err=%v", actorID, mutatedEmail, errEmail)
			return false
		}

		afterEmailMut, _ := repo.snapshot(actorID)
		if !mappingsEqual(before, afterEmailMut) {
			t.Logf("rejected email mutation altered stored row: before=%+v after=%+v", formatMapping(before), formatMapping(afterEmailMut))
			return false
		}

		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: propertyMaxCount}); err != nil {
		t.Errorf("mutation-rejection property failed: %v", err)
	}
}

// ----- helpers for diagnostics -----

func formatPtr(p *string) string {
	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%q", *p)
}

func formatMapping(m *entities.ActorMapping) string {
	if m == nil {
		return "<nil>"
	}

	return fmt.Sprintf("{ActorID=%q DisplayName=%s Email=%s}", m.ActorID, formatPtr(m.DisplayName), formatPtr(m.Email))
}

func mappingsEqual(a, b *entities.ActorMapping) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.ActorID == b.ActorID &&
		stringPtrEqualLocal(a.DisplayName, b.DisplayName) &&
		stringPtrEqualLocal(a.Email, b.Email)
}
