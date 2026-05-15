// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// TDD-RED for fix-actor-mapping-pseudonymization-bypass.
//
// These tests encode the post-fix behavior for the governance actor_mapping
// pseudonymization bypass discovered by Taura Security (28/04/2026):
//
//   - actor_mapping identity fields (display_name, email) become append-only
//     after the first successful create. Subsequent calls with a different
//     payload must return ErrActorMappingImmutable, mapped to HTTP 409.
//   - The current UpsertActorMapping is renamed to CreateOrGetActorMapping
//     at the service layer to better reflect the post-fix semantics; the
//     handler keeps the PUT verb for external API compatibility.
//   - Idempotent PUT (identical payload) returns the existing entity with no
//     error (no-op success).
//   - PseudonymizeActor and DeleteActorMapping are unchanged.
//
// All tests in this file are expected to FAIL until Gate 0.2 (TDD-GREEN)
// implements: (a) the sentinel ErrActorMappingImmutable, (b) the new
// CreateOrGetActorMapping method, and (c) the service-layer guard that
// detects mutation attempts against an existing actor_mapping row.
package command

import (
	"context"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
)

// strPtr is a tiny local helper to take the address of a string literal.
func strPtr(s string) *string {
	return &s
}

// immutableTestTenantID is reused across the file; mirrors the tenant ID
// used by existing tests in actor_mapping_commands_test.go.
const immutableTestTenantID = "018f4f95-0000-7000-8000-000000000001"

func immutableTestContext() context.Context {
	return tmcore.ContextWithTenantID(testContext(), immutableTestTenantID)
}

// AC1 — actor_id not yet present.
// Behavior: CreateOrGetActorMapping inserts a brand-new mapping and returns
// the persisted entity. Repository receives a single Upsert (which will be
// implemented post-fix as INSERT ... ON CONFLICT DO NOTHING RETURNING; on
// fresh actor_id RETURNING yields the new row).
func TestCreateOrGetActorMapping_NewActor_Creates(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)

	displayName := "John Doe"
	email := "john@example.com"

	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, m *entities.ActorMapping) (*entities.ActorMapping, error) {
			require.Equal(t, "actor-new-001", m.ActorID)
			require.NotNil(t, m.DisplayName)
			require.Equal(t, "John Doe", *m.DisplayName)
			require.NotNil(t, m.Email)
			require.Equal(t, "john@example.com", *m.Email)
			return &entities.ActorMapping{
				ActorID:     m.ActorID,
				DisplayName: m.DisplayName,
				Email:       m.Email,
				CreatedAt:   m.CreatedAt,
				UpdatedAt:   m.UpdatedAt,
			}, nil
		},
	)

	uc, err := NewActorMappingUseCase(repo)
	require.NoError(t, err)

	result, err := uc.CreateOrGetActorMapping(immutableTestContext(), "actor-new-001", &displayName, &email)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "actor-new-001", result.ActorID)
	require.NotNil(t, result.DisplayName)
	assert.Equal(t, "John Doe", *result.DisplayName)
	require.NotNil(t, result.Email)
	assert.Equal(t, "john@example.com", *result.Email)
}

// AC2 — actor_id exists and the caller submits the SAME values.
// Behavior: no-op success. The use case returns the existing entity without
// error. From the service perspective this is observable as a successful
// call that returns the current persisted state.
func TestCreateOrGetActorMapping_IdempotentSameValues_NoOp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)

	displayName := "Jane Roe"
	email := "jane@example.com"
	now := time.Now().UTC().Add(-time.Hour) // existing row was created an hour ago

	existing := &entities.ActorMapping{
		ActorID:     "actor-idem-002",
		DisplayName: strPtr("Jane Roe"),
		Email:       strPtr("jane@example.com"),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// The repository contract post-fix returns the EXISTING row when the
	// payload matches (the new SQL is INSERT ... ON CONFLICT DO NOTHING;
	// when RETURNING yields nothing the repository SELECTs the current
	// row and compares; identical → returns it; different → returns
	// ErrActorMappingImmutable).
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(existing, nil)

	uc, err := NewActorMappingUseCase(repo)
	require.NoError(t, err)

	result, err := uc.CreateOrGetActorMapping(immutableTestContext(), "actor-idem-002", &displayName, &email)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "actor-idem-002", result.ActorID)
	require.NotNil(t, result.DisplayName)
	assert.Equal(t, "Jane Roe", *result.DisplayName)
	require.NotNil(t, result.Email)
	assert.Equal(t, "jane@example.com", *result.Email)
	// Idempotent path must NOT bump updated_at — the persisted row is unchanged.
	assert.Equal(t, now, result.UpdatedAt)
}

// AC3 — actor_id exists, caller submits a DIFFERENT email.
// Behavior: 409-mapped error ErrActorMappingImmutable. The use case must
// surface the sentinel verbatim (wrapped) so the handler can map it to
// HTTP 409. The persisted row remains untouched.
func TestCreateOrGetActorMapping_DifferentEmail_ReturnsImmutableError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)

	// Repository post-fix detects the divergence (INSERT ... ON CONFLICT DO
	// NOTHING returned no rows, SELECT showed different email) and returns
	// ErrActorMappingImmutable.
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, ErrActorMappingImmutable)

	uc, err := NewActorMappingUseCase(repo)
	require.NoError(t, err)

	newDisplayName := "Jane Roe"
	newEmail := "different@example.com" // mutation attempt
	result, err := uc.CreateOrGetActorMapping(immutableTestContext(), "actor-mut-003", &newDisplayName, &newEmail)
	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, ErrActorMappingImmutable)
}

// AC4 — actor_id exists, caller submits a DIFFERENT display_name.
// Behavior: 409-mapped error ErrActorMappingImmutable. Display name
// mutation is a separate AC because both PII fields are independently
// immutable post-creation.
func TestCreateOrGetActorMapping_DifferentDisplayName_ReturnsImmutableError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)

	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, ErrActorMappingImmutable)

	uc, err := NewActorMappingUseCase(repo)
	require.NoError(t, err)

	newDisplayName := "Renamed Person" // mutation attempt
	email := "stable@example.com"
	result, err := uc.CreateOrGetActorMapping(immutableTestContext(), "actor-mut-004", &newDisplayName, &email)
	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, ErrActorMappingImmutable)
}

// AC5 — pentest PoC reproduction.
// Pseudonymized mapping ([REDACTED] for both display_name and email) MUST
// remain redacted. Submitting any plaintext PII via PUT must be rejected
// with ErrActorMappingImmutable. The repository, after the fix, returns
// the sentinel because the post-INSERT SELECT shows the current row
// differs from the payload (and additionally, the existing row is in
// the [REDACTED] state — the pseudonymization is irreversible per
// actor_id).
func TestCreateOrGetActorMapping_OnRedactedMapping_ReturnsImmutableError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)

	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, ErrActorMappingImmutable)

	uc, err := NewActorMappingUseCase(repo)
	require.NoError(t, err)

	// Attacker payload — tries to overwrite [REDACTED] with PII.
	attackerName := "Attacker Name"
	attackerEmail := "attacker@evil.example"
	result, err := uc.CreateOrGetActorMapping(immutableTestContext(), "actor-pseudo-005", &attackerName, &attackerEmail)
	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, ErrActorMappingImmutable)
}

// Regression check: when the repository returns the domain not-found error
// the service must propagate it (NOT wrap it as ImmutableError). This
// guards against future regressions where the wrong sentinel gets bound
// to a 404 status.
func TestCreateOrGetActorMapping_RepositoryReturnsNotFound_PropagatedCleanly(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, governanceErrors.ErrActorMappingNotFound)

	uc, err := NewActorMappingUseCase(repo)
	require.NoError(t, err)

	displayName := "Test"
	result, err := uc.CreateOrGetActorMapping(immutableTestContext(), "actor-nf-006", &displayName, nil)
	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, governanceErrors.ErrActorMappingNotFound)
	assert.NotErrorIs(t, err, ErrActorMappingImmutable)
}

// Sentinel availability test: ErrActorMappingImmutable must be exported
// from the command package and must have a stable, non-empty message.
// This will fail to compile until Gate 0.2 introduces the sentinel.
func TestErrActorMappingImmutable_Available(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrActorMappingImmutable)
	assert.NotEmpty(t, ErrActorMappingImmutable.Error())
}
