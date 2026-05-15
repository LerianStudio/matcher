// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Fuzz tests for the actor_mapping immutability primitives introduced as part
// of the Taura Security pentest finding (28/04/2026).
//
// The pseudonymization-bypass vulnerability was a *logic* bug, not a parser
// bug — an attacker can already control what JSON they POST, but the
// repository was COALESCE-overwriting `[REDACTED]` with plaintext. The fix
// hinges on two tiny helpers, `actorMappingPIIDiffers` and `stringPtrEqual`,
// deciding whether a re-Upsert is idempotent (success) or a mutation attempt
// (ErrActorMappingImmutable). Anything that confuses those two helpers
// re-opens the bypass.
//
// These fuzzers target *invariants* of those helpers, not example inputs:
//
//   - stringPtrEqual must be reflexive and symmetric, and it must agree with
//     `*lhs == *rhs` whenever both sides are non-nil and non-empty. The
//     nil/empty equivalence is a deliberate semantic carve-out for NULL vs ""
//     in the database.
//
//   - actorMappingPIIDiffers must be reflexive (a mapping never differs from
//     itself) and symmetric (the order of arguments cannot flip the answer).
//     Violation of either property would silently allow a mutation through.

package actormapping

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// fuzzFixedActorID is a deterministic actor ID used by buildActorMapping so
// fuzz failures are reproducible across runs. The PII-diff helpers do not
// branch on the actor ID value, so a constant ID is safe.
const fuzzFixedActorID = "actor-fuzz-fixed-00000000-0000-0000-0000-000000000001"

// FuzzStringPtrEqual probes the nil/empty/equality invariants of
// stringPtrEqual. A mismatch between the helper and the documented semantics
// would re-open the pseudonymization bypass, so we test the contract directly.
func FuzzStringPtrEqual(f *testing.F) {
	// Seed corpus covers: both nil, one nil + one empty, both empty,
	// ASCII equality, UTF-8 equality, REDACTED token, NUL byte, very long.
	f.Add("", false, "", false) // both nil
	f.Add("", true, "", true)   // both empty (non-nil)
	f.Add("", false, "", true)  // nil vs empty
	f.Add("alice@example.com", true, "alice@example.com", true)
	f.Add("Alice", true, "alice", true)                  // case difference
	f.Add("[REDACTED]", true, "alice@example.com", true) // adversarial: redacted vs plaintext
	f.Add("é", true, "é", true)                         // é vs e + combining accent
	f.Add("a\x00b", true, "a\x00b", true)                // embedded NUL
	f.Add("emoji \U0001F600", true, "emoji \U0001F600", true)

	f.Fuzz(func(t *testing.T, lhs string, lhsPresent bool, rhs string, rhsPresent bool) {
		var lhsPtr, rhsPtr *string

		if lhsPresent {
			lhsPtr = &lhs
		}

		if rhsPresent {
			rhsPtr = &rhs
		}

		got := stringPtrEqual(lhsPtr, rhsPtr)

		// Reflexivity: a value always equals itself.
		require.True(t, stringPtrEqual(lhsPtr, lhsPtr), "stringPtrEqual must be reflexive for lhs")
		require.True(t, stringPtrEqual(rhsPtr, rhsPtr), "stringPtrEqual must be reflexive for rhs")

		// Symmetry: swapping arguments cannot change the answer.
		require.Equal(t, got, stringPtrEqual(rhsPtr, lhsPtr), "stringPtrEqual must be symmetric")

		// Documented semantics:
		//   - nil and "" on the same side are interchangeable
		//   - one side empty + one side non-empty → not equal
		//   - both non-empty → exact byte-wise equality
		lhsEmpty := lhsPtr == nil || *lhsPtr == ""
		rhsEmpty := rhsPtr == nil || *rhsPtr == ""

		switch {
		case lhsEmpty && rhsEmpty:
			require.True(t, got, "two empty/nil sides must be equal")
		case lhsEmpty != rhsEmpty:
			require.False(t, got, "empty vs non-empty must NOT be equal (this is the bypass guard)")
		default:
			require.Equal(t, lhs == rhs, got, "two non-empty pointers must follow byte-wise equality")
		}
	})
}

// FuzzActorMappingPIIDiffers asserts the reflexivity + symmetry invariants of
// the helper that gates the immutability check. If either invariant fails for
// any input, the conflict path of `insertOrCompare` could accept a mutation
// against an existing row.
func FuzzActorMappingPIIDiffers(f *testing.F) {
	// Seeds vary nil-ness via the *Present booleans and content via the strings.
	// Each row encodes (display1, has_display1, email1, has_email1,
	//                   display2, has_display2, email2, has_email2).
	f.Add("Alice", true, "alice@example.com", true, "Alice", true, "alice@example.com", true)   // identical
	f.Add("[REDACTED]", true, "[REDACTED]", true, "Alice", true, "alice@example.com", true)     // adversarial: redacted vs plaintext
	f.Add("", false, "", false, "", false, "", false)                                           // both fully nil
	f.Add("", true, "", true, "", false, "", false)                                             // empty vs nil
	f.Add("é", true, "user@émail.com", true, "é", true, "user@émail.com", true)               // UTF-8 normalization
	f.Add("name\x00null", true, "a@b\x00.com", true, "name\x00null", true, "a@b\x00.com", true) // embedded NUL bytes

	f.Fuzz(func(t *testing.T,
		d1 string, d1Present bool, e1 string, e1Present bool,
		d2 string, d2Present bool, e2 string, e2Present bool,
	) {
		// Skip wildly oversized inputs — fuzzer occasionally generates them
		// and they just consume CPU without exercising the logic.
		if len(d1) > 4096 || len(e1) > 4096 || len(d2) > 4096 || len(e2) > 4096 {
			return
		}

		a := buildActorMapping(d1, d1Present, e1, e1Present)
		b := buildActorMapping(d2, d2Present, e2, e2Present)

		// buildActorMapping returns nil for invalid UTF-8 to avoid encoding
		// noise. Skip those iterations — reflexivity below requires non-nil.
		if a == nil || b == nil {
			return
		}

		// Reflexivity: a non-nil mapping never differs from itself.
		require.False(t, actorMappingPIIDiffers(a, a), "actorMappingPIIDiffers must be reflexive (a vs a)")
		require.False(t, actorMappingPIIDiffers(b, b), "actorMappingPIIDiffers must be reflexive (b vs b)")

		// Symmetry: ordering must not change the verdict.
		require.Equal(t,
			actorMappingPIIDiffers(a, b),
			actorMappingPIIDiffers(b, a),
			"actorMappingPIIDiffers must be symmetric",
		)

		// Nil-arg behavior is documented as "differs": the helper treats any
		// nil entity as a mismatch so the caller never silently passes.
		require.True(t, actorMappingPIIDiffers(nil, a), "nil existing must be flagged as differs")
		require.True(t, actorMappingPIIDiffers(a, nil), "nil payload must be flagged as differs")
	})
}

// buildActorMapping is a small helper that converts the flat fuzz inputs into
// an *entities.ActorMapping with nil-able pointer fields. The actor ID is a
// fixed constant so fuzz failures are reproducible — the PII comparison does
// not branch on the actor ID, so determinism here is safe.
func buildActorMapping(display string, displayPresent bool, email string, emailPresent bool) *entities.ActorMapping {
	// Defensive: drop malformed UTF-8 to keep the assertion failures focused
	// on logic bugs rather than incidental encoding artefacts. The DB layer
	// uses TEXT/VARCHAR which would already reject invalid UTF-8 on write.
	// Only validate fields that the caller marks as present — invalid bytes
	// in an absent field are inert (we never read them), so rejecting them
	// would shrink the valid input space without protecting any assertion.
	if displayPresent && !utf8.ValidString(display) {
		return nil
	}

	if emailPresent && !utf8.ValidString(email) {
		return nil
	}

	mapping := &entities.ActorMapping{ActorID: fuzzFixedActorID}
	if displayPresent {
		mapping.DisplayName = &display
	}

	if emailPresent {
		mapping.Email = &email
	}

	return mapping
}
