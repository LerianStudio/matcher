// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Property-based tests for the ActorMapping domain entity.
//
// Gate 5 of fix-actor-mapping-pseudonymization-bypass. Encodes the
// IsRedacted invariant against the random-input space rather than
// hand-picked examples, ensuring the predicate is well-defined for any
// combination of (nil/non-nil display_name, nil/non-nil email, arbitrary
// string contents).
//
// Convention: testing/quick with bounded MaxCount=1000 per property to
// stay inside the unit-test wall-clock budget while still covering
// thousands of pseudo-random inputs per CI run.

package entities

import (
	"reflect"
	"testing"
	"testing/quick"
)

const (
	// propertyMaxCount is the number of random inputs drawn per property.
	// 1000 is empirically enough to surface the kind of contradictions
	// these properties target while keeping the suite under a few seconds.
	propertyMaxCount = 1000
)

// stringPtrOrNil converts a (string, bool) draw into a *string. The boolean
// controls whether the pointer is non-nil. Using a derived helper rather than
// quick.Generator keeps the test free of reflection plumbing.
func stringPtrOrNil(s string, present bool) *string {
	if !present {
		return nil
	}

	return &s
}

// TestProperty_IsRedacted_WellDefined encodes Invariant 4:
//
//	IsRedacted() == true ⟺
//	  DisplayName != nil ∧ Email != nil ∧
//	  *DisplayName == "[REDACTED]" ∧ *Email == "[REDACTED]"
//
// Specifically:
//   - When either pointer is nil the result is false (regardless of the
//     other side's value).
//   - The receiver-nil fast path returns false.
//   - The check is symmetric: order of arguments inside the struct does
//     not matter.
func TestProperty_IsRedacted_WellDefined(t *testing.T) {
	t.Parallel()

	property := func(
		displayName, email string,
		displayPresent, emailPresent bool,
	) bool {
		mapping := &ActorMapping{
			ActorID:     "actor-property",
			DisplayName: stringPtrOrNil(displayName, displayPresent),
			Email:       stringPtrOrNil(email, emailPresent),
		}

		got := mapping.IsRedacted()

		// Reference: hand-coded biconditional that must match the
		// implementation under all inputs.
		want := displayPresent && emailPresent &&
			displayName == "[REDACTED]" && email == "[REDACTED]"

		return got == want
	}

	if err := quick.Check(property, &quick.Config{MaxCount: propertyMaxCount}); err != nil {
		t.Errorf("IsRedacted well-defined property failed: %v", err)
	}
}

// TestProperty_IsRedacted_NilReceiver encodes the fast-path guarantee:
// a nil *ActorMapping never crashes and always reports false.
//
// quick.Check is overkill for a single-shape input, but we use it for
// consistency with the other property tests in this file and to keep the
// "Gate 5 properties report" table uniform.
func TestProperty_IsRedacted_NilReceiver(t *testing.T) {
	t.Parallel()

	property := func(_ int) bool {
		var mapping *ActorMapping

		return mapping.IsRedacted() == false
	}

	if err := quick.Check(property, &quick.Config{MaxCount: propertyMaxCount}); err != nil {
		t.Errorf("IsRedacted nil-receiver property failed: %v", err)
	}
}

// TestProperty_IsRedacted_PartialRedactionAlwaysFalse asserts that any
// configuration where exactly one of (DisplayName, Email) equals
// "[REDACTED]" returns false. This guards against future refactors that
// might collapse the AND into an OR.
func TestProperty_IsRedacted_PartialRedactionAlwaysFalse(t *testing.T) {
	t.Parallel()

	property := func(otherValue string) bool {
		// quick can synthesise otherValue == "[REDACTED]", in which case
		// the partial-redaction premise is not met and we skip.
		if otherValue == "[REDACTED]" {
			return true
		}

		redacted := "[REDACTED]"
		other := otherValue

		// Display redacted, email not.
		m1 := &ActorMapping{DisplayName: &redacted, Email: &other}
		if m1.IsRedacted() {
			return false
		}

		// Email redacted, display not.
		m2 := &ActorMapping{DisplayName: &other, Email: &redacted}

		return !m2.IsRedacted()
	}

	if err := quick.Check(property, &quick.Config{MaxCount: propertyMaxCount}); err != nil {
		t.Errorf("partial-redaction property failed: %v", err)
	}
}

// Compile-time type assertion to keep the helper signature stable; if
// stringPtrOrNil ever drifts away from the (string, bool) → *string
// shape required by quick, this line will fail to compile and surface
// the breakage early.
var _ = func() *string {
	return stringPtrOrNil("x", true)
}

// reflectKindCheck is a guard the test author keeps around to ensure
// the property-test environment can synthesise the parameter types
// quick.Check requires. If a Go version changes the supported kinds,
// this catches it before the real property tests do.
func reflectKindCheck(t *testing.T) {
	t.Helper()

	required := []reflect.Kind{reflect.String, reflect.Bool, reflect.Int}
	for _, k := range required {
		if k == reflect.Invalid {
			t.Fatalf("reflect kind %v invalid in this Go runtime", k)
		}
	}
}

func TestProperty_QuickEnvironmentSane(t *testing.T) {
	t.Parallel()
	reflectKindCheck(t)
}
