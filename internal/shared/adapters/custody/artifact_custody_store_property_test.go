// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package custody

import (
	"errors"
	"math/rand"
	"strings"
	"testing"
	"testing/quick"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// This file holds Gate 5 property-based tests for the custody object-key
// construction. The custody store is the boundary where tenant isolation
// is materialised as a path prefix; bugs here leak cross-tenant data, so
// property-level guarantees matter more than example coverage.
//
// Two invariants are under test:
//
//   1. Path idempotence — the path for (tenantID, extractionID) is a
//      pure function of those inputs. No clock coupling, no package-
//      level mutable state, no hidden counters.
//   2. Tenant isolation — different tenant IDs yield disjoint path
//      prefixes. No collision shall map two tenants to the same custody
//      location, even with adversarial whitespace or casing.

// propertyMaxCount is the default iteration budget for quick.Check in
// this file. 100 matches the Ring standard.
const propertyMaxCount = 100

// seededConfig builds a quick.Config with a fixed seed so any property
// failure is reproducible from CI logs alone.
func seededConfig(seed int64) *quick.Config {
	return &quick.Config{
		MaxCount: propertyMaxCount,
		Rand:     rand.New(rand.NewSource(seed)),
	}
}

// sanitizeTenantID trims whitespace and rejects tenant IDs that would be
// rejected by BuildObjectKey (empty, '/', or any ASCII control byte).
// Property generators produce arbitrary strings — including NUL and
// other control bytes — which BuildObjectKey correctly refuses per its
// contract. This filter keeps the harness focused on the "well-formed
// input" invariants instead of re-testing those already-unit-tested
// rejection paths.
func sanitizeTenantID(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}

	if strings.Contains(trimmed, "/") {
		return "", false
	}

	// Mirror BuildObjectKey's control-byte guard. A fuzz-discovered
	// regression showed that unfiltered generator output could contain
	// NUL bytes and other C0 control characters, which are rejected at
	// build time for path-injection reasons.
	for i := 0; i < len(trimmed); i++ {
		b := trimmed[i]
		if b < 0x20 || b == 0x7F {
			return "", false
		}
	}

	return trimmed, true
}

// deterministicUUID expands a random seed into a well-formed UUID by
// reading 16 bytes from the generator and feeding them to uuid.FromBytes.
// quick.Check cannot generate uuid.UUID natively; this adapter lets us
// drive the generator with a small seed and get a valid id out.
func deterministicUUID(rng *rand.Rand) uuid.UUID {
	var raw [16]byte

	_, _ = rng.Read(raw[:])

	id, err := uuid.FromBytes(raw[:])
	if err != nil {
		// uuid.FromBytes only fails on wrong slice length; we control
		// it at 16 so this branch is impossible. Fall back to a fresh
		// random id rather than panicking inside a test helper.
		return uuid.New()
	}

	return id
}

// TestProperty_CustodyPath_Deterministic (Invariant 6): for any
// well-formed (tenantID, extractionID), BuildObjectKey returns the same
// key on every invocation.
//
// This is the "no hidden state" property. If BuildObjectKey ever grows a
// clock dependency, a package-level counter, or a randomised prefix, the
// property fails — which is exactly when we want to catch it, because
// the bridge worker calls this path multiple times per extraction
// (Store on success, Delete on retention sweep) and expects byte-stable
// keys.
func TestProperty_CustodyPath_Deterministic(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(601)

	property := func(rawTenant string, uuidSeed uint64) bool {
		tenant, ok := sanitizeTenantID(rawTenant)
		if !ok {
			return true // skip rejected inputs; covered by unit tests.
		}

		uuidRNG := rand.New(rand.NewSource(int64(uuidSeed) + 1))
		extractionID := deterministicUUID(uuidRNG)

		first, err := BuildObjectKey(tenant, extractionID)
		if err != nil {
			return false
		}

		second, err := BuildObjectKey(tenant, extractionID)
		if err != nil {
			return false
		}

		third, err := BuildObjectKey(tenant, extractionID)
		if err != nil {
			return false
		}

		return first == second && second == third
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_CustodyPath_StructureMatchesContract pins the layout we
// publish via the BuildObjectKey doc comment:
//
//	{tenantID}/fetcher-artifacts/{extractionID}.json
//
// Property form: for any valid input, the returned key has three and
// only three '/' separators, the middle segment is exactly KeyPrefix,
// and the suffix ends in ".json". Drifting the layout would silently
// break downstream readers (ingestion worker, retention sweep) that
// pattern-match the key.
func TestProperty_CustodyPath_StructureMatchesContract(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(602)

	property := func(rawTenant string, uuidSeed uint64) bool {
		tenant, ok := sanitizeTenantID(rawTenant)
		if !ok {
			return true
		}

		uuidRNG := rand.New(rand.NewSource(int64(uuidSeed) + 1))
		extractionID := deterministicUUID(uuidRNG)

		key, err := BuildObjectKey(tenant, extractionID)
		if err != nil {
			return false
		}

		parts := strings.Split(key, "/")
		if len(parts) != 3 {
			return false
		}

		if parts[0] != tenant {
			return false
		}

		if parts[1] != KeyPrefix {
			return false
		}

		if parts[2] != extractionID.String()+".json" {
			return false
		}

		return strings.HasSuffix(key, ".json")
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_CustodyPath_TenantIsolation (Invariant 7): different
// tenant IDs MUST yield different path prefixes.
//
// This is the core multi-tenant safety property. The custody store's
// sole mechanism for isolating tenants is the leading path segment; if
// two distinct tenant IDs could collapse to the same prefix, a malicious
// or buggy caller could steer their custody writes into another
// tenant's namespace.
//
// We also assert the path is strictly prefixed by tenantID + "/", so no
// "sibling" tenant could accidentally share a prefix (e.g. "tenant-1"
// and "tenant-10").
func TestProperty_CustodyPath_TenantIsolation(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(703)

	property := func(rawA, rawB string, uuidSeed uint64) bool {
		tenantA, okA := sanitizeTenantID(rawA)
		tenantB, okB := sanitizeTenantID(rawB)

		if !okA || !okB {
			return true
		}

		if tenantA == tenantB {
			return true // property is "distinct in ⇒ distinct out".
		}

		uuidRNG := rand.New(rand.NewSource(int64(uuidSeed) + 1))
		extractionID := deterministicUUID(uuidRNG)

		keyA, err := BuildObjectKey(tenantA, extractionID)
		if err != nil {
			return false
		}

		keyB, err := BuildObjectKey(tenantB, extractionID)
		if err != nil {
			return false
		}

		if keyA == keyB {
			return false
		}

		// Each key's top-level segment must be the tenant id literally.
		// This is stronger than "keys differ": it rules out a crafty
		// implementation that encodes the tenant in a later position,
		// which would defeat prefix-based IAM policies downstream.
		if !strings.HasPrefix(keyA, tenantA+"/") {
			return false
		}

		if !strings.HasPrefix(keyB, tenantB+"/") {
			return false
		}

		// Cross-contamination check: keyA must NOT start with tenantB +
		// "/", and vice versa. (Catches the "tenant-1" vs "tenant-10"
		// substring trap where HasPrefix alone would misfire.)
		if strings.HasPrefix(keyA, tenantB+"/") {
			return false
		}

		if strings.HasPrefix(keyB, tenantA+"/") {
			return false
		}

		return true
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_CustodyPath_ExtractionIsolation is a symmetry property:
// for a fixed tenant, different extraction IDs yield different keys.
// This catches a class of bug where the extraction id might be dropped,
// overwritten, or truncated inside BuildObjectKey — which would cause
// two concurrent extractions to overwrite each other's custody copy.
func TestProperty_CustodyPath_ExtractionIsolation(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(804)

	property := func(rawTenant string, seedA, seedB uint64) bool {
		tenant, ok := sanitizeTenantID(rawTenant)
		if !ok {
			return true
		}

		if seedA == seedB {
			return true
		}

		rngA := rand.New(rand.NewSource(int64(seedA) + 1))
		idA := deterministicUUID(rngA)

		rngB := rand.New(rand.NewSource(int64(seedB) + 1))
		idB := deterministicUUID(rngB)

		if idA == idB {
			return true // unlikely collision on 128-bit ids; skip.
		}

		keyA, err := BuildObjectKey(tenant, idA)
		if err != nil {
			return false
		}

		keyB, err := BuildObjectKey(tenant, idB)
		if err != nil {
			return false
		}

		return keyA != keyB
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_CustodyPath_RejectsInvalidTenant confirms the property
// that tenantIDs containing '/' or reducing to empty after TrimSpace
// always produce a sentinel error. Complements the happy-path
// properties above by covering the rejection side as a property rather
// than a handful of fixed examples.
func TestProperty_CustodyPath_RejectsInvalidTenant(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(905)

	property := func(prefix, suffix string, uuidSeed uint64) bool {
		// Inject a '/' so any non-empty combination produces an invalid
		// tenant. Empty (prefix, suffix, "/") still reduces to "/" which
		// is also invalid.
		rawTenant := prefix + "/" + suffix

		uuidRNG := rand.New(rand.NewSource(int64(uuidSeed) + 1))
		extractionID := deterministicUUID(uuidRNG)

		_, err := BuildObjectKey(rawTenant, extractionID)

		// The sentinel chain must include ErrArtifactTenantIDRequired
		// regardless of the surface-level formatting. This is tested by
		// unit tests at the example level; here we confirm the property
		// holds for a generated space of inputs.
		return err != nil && sentinelIsTenantIDRequired(err)
	}

	require.NoError(t, quick.Check(property, cfg))
}

// sentinelIsTenantIDRequired is a readable alias around errors.Is. Kept
// as a named helper so the property's intent — "reject this exact
// sentinel chain" — reads out loud at the call site.
func sentinelIsTenantIDRequired(err error) bool {
	return errors.Is(err, sharedPorts.ErrArtifactTenantIDRequired)
}
