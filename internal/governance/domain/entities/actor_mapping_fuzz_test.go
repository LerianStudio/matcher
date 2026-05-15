// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Fuzz tests for the NewActorMapping constructor — the input-validation entry
// point that gates ActorID length and trimming behaviour. Although the
// pseudonymization-bypass logic lives in the repository, the constructor is
// the first chance to reject malformed actor IDs, so its contract must hold
// under adversarial inputs:
//
//   - Either the constructor returns (*ActorMapping, nil) OR (nil, error).
//     Never both nil, never both non-nil. (XOR success/error invariant.)
//
//   - On success, ActorID equals strings.TrimSpace(input) and the trimmed
//     length is within the documented bound.
//
//   - Errors are sentinel errors documented in this package; the constructor
//     never panics regardless of input shape.

package entities

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzNewActorMapping(f *testing.F) {
	// Seed corpus: empty, whitespace-only, valid ASCII, UTF-8, boundary
	// lengths around MaxActorMappingActorIDLength, REDACTED token,
	// NUL bytes, and adversarial overlong inputs.
	f.Add("", "Alice", "alice@example.com", true, true)
	f.Add("   ", "Alice", "alice@example.com", true, true)
	f.Add("user-123", "Alice", "alice@example.com", true, true)
	f.Add("user-123", "", "", false, false) // valid ID, nil PII
	f.Add("[REDACTED]", "[REDACTED]", "[REDACTED]", true, true)
	f.Add("user\x00with\x00nulls", "name", "e@m.com", true, true)
	f.Add(strings.Repeat("x", MaxActorMappingActorIDLength), "n", "e", true, true)
	f.Add(strings.Repeat("x", MaxActorMappingActorIDLength+1), "n", "e", true, true)
	f.Add("é-actor", "É", "user@émail.com", true, true)

	f.Fuzz(func(t *testing.T, actorID, displayName, email string, displayPresent, emailPresent bool) {
		ctx := context.Background()

		var displayPtr, emailPtr *string

		if displayPresent {
			displayPtr = &displayName
		}

		if emailPresent {
			emailPtr = &email
		}

		mapping, err := NewActorMapping(ctx, actorID, displayPtr, emailPtr)

		// XOR invariant: exactly one of (mapping, err) is non-nil.
		if mapping == nil && err == nil {
			t.Fatalf("constructor returned (nil, nil) for actorID=%q — must be exclusive", actorID)
		}

		if mapping != nil && err != nil {
			t.Fatalf("constructor returned both mapping and err (=%v) for actorID=%q", err, actorID)
		}

		if err != nil {
			// Error path: must be one of the documented sentinels.
			require.True(t,
				errors.Is(err, ErrActorIDRequired) || errors.Is(err, ErrActorIDExceedsMaxLen),
				"unexpected error %v for actorID=%q (must be sentinel)", err, actorID,
			)

			// Justify the rejection: either trimmed-empty or over the
			// length bound. Anything else would be a contract violation.
			trimmed := strings.TrimSpace(actorID)
			switch {
			case errors.Is(err, ErrActorIDRequired):
				require.Empty(t, trimmed, "ErrActorIDRequired requires trimmed-empty input")
			case errors.Is(err, ErrActorIDExceedsMaxLen):
				require.Greater(t, len(trimmed), MaxActorMappingActorIDLength,
					"ErrActorIDExceedsMaxLen requires trimmed length > %d", MaxActorMappingActorIDLength)
			}

			return
		}

		// Success path: validate the produced entity.
		trimmed := strings.TrimSpace(actorID)
		require.Equal(t, trimmed, mapping.ActorID, "ActorID must be trimmed")
		require.NotEmpty(t, mapping.ActorID, "successful construction implies non-empty ActorID")
		require.LessOrEqual(t, len(mapping.ActorID), MaxActorMappingActorIDLength,
			"successful construction implies ActorID within bound")

		// CreatedAt and UpdatedAt are set together to the same UTC instant.
		require.Equal(t, mapping.CreatedAt, mapping.UpdatedAt,
			"CreatedAt and UpdatedAt must be equal on construction")
		require.Equal(t, "UTC", mapping.CreatedAt.Location().String(),
			"timestamps must be UTC per project convention")

		// PII pointer fields must round-trip the caller's intent.
		if displayPresent {
			require.NotNil(t, mapping.DisplayName, "DisplayName must be set when caller passed a pointer")
			require.Equal(t, displayName, *mapping.DisplayName)
		} else {
			require.Nil(t, mapping.DisplayName, "DisplayName must be nil when caller passed nil")
		}

		if emailPresent {
			require.NotNil(t, mapping.Email, "Email must be set when caller passed a pointer")
			require.Equal(t, email, *mapping.Email)
		} else {
			require.Nil(t, mapping.Email, "Email must be nil when caller passed nil")
		}
	})
}
