// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// TDD-RED for fix-actor-mapping-pseudonymization-bypass.
//
// These handler tests encode the post-fix HTTP contract:
//
//   - PUT /v1/governance/actor-mappings/{actorId} on a fresh actor_id:
//     succeeds (200 OK with the persisted entity body).
//   - PUT with payload identical to the existing row: succeeds (200 OK,
//     idempotent no-op).
//   - PUT with a different display_name or email on an existing row:
//     returns 409 Conflict with ErrActorMappingImmutable surfaced.
//   - PUT against a pseudonymized row ([REDACTED]) with any PII: returns
//     409 Conflict. Stored data remains [REDACTED].
//
// All tests in this file are expected to FAIL until Gate 0.2 (TDD-GREEN)
// (a) maps command.ErrActorMappingImmutable to fiber.StatusConflict in
// the handler and (b) the service layer surfaces the sentinel.
package http

import (
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/governance/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
)

// AC1 (HTTP layer) — PUT on a fresh actor_id creates the mapping and
// returns 200 OK. This validates the happy path is preserved.
func TestPutActorMapping_NewActor_Returns200(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	now := time.Now().UTC()
	displayName := "John Doe"
	email := "john@example.com"

	mapping := &entities.ActorMapping{
		ActorID:     "actor-new-201",
		DisplayName: &displayName,
		Email:       &email,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	repo := mocks.NewMockActorMappingRepository(ctrl)
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(mapping, nil)

	handler := newTestActorMappingHandler(t, repo)

	body := dto.UpsertActorMappingRequest{
		DisplayName: &displayName,
		Email:       &email,
	}

	resp := testUpsertActorMappingRequest(t, handler, "actor-new-201", body)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

// AC2 (HTTP layer) — PUT with payload identical to existing row returns
// 200 OK. Idempotency preserves HTTP PUT semantics.
func TestPutActorMapping_Idempotent_Returns200(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	displayName := "Jane Roe"
	email := "jane@example.com"
	createdAt := time.Now().UTC().Add(-time.Hour)

	existing := &entities.ActorMapping{
		ActorID:     "actor-idem-202",
		DisplayName: &displayName,
		Email:       &email,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}

	repo := mocks.NewMockActorMappingRepository(ctrl)
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(existing, nil)

	handler := newTestActorMappingHandler(t, repo)

	body := dto.UpsertActorMappingRequest{
		DisplayName: &displayName,
		Email:       &email,
	}

	resp := testUpsertActorMappingRequest(t, handler, "actor-idem-202", body)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

// AC3 (HTTP layer) — PUT with different email on an existing row returns
// 409 Conflict. The handler must map command.ErrActorMappingImmutable to
// fiber.StatusConflict; this test will fail until that mapping is added.
func TestPutActorMapping_DifferentEmail_Returns409(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)
	// Service surfaces the immutable sentinel — handler must map it to 409.
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, command.ErrActorMappingImmutable)

	handler := newTestActorMappingHandler(t, repo)

	newDisplayName := "Stable Name"
	newEmail := "changed@example.com"
	body := dto.UpsertActorMappingRequest{
		DisplayName: &newDisplayName,
		Email:       &newEmail,
	}

	resp := testUpsertActorMappingRequest(t, handler, "actor-mut-203", body)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
}

// AC4 (HTTP layer) — PUT with different display_name returns 409.
func TestPutActorMapping_DifferentDisplayName_Returns409(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, command.ErrActorMappingImmutable)

	handler := newTestActorMappingHandler(t, repo)

	newDisplayName := "Different Name"
	stableEmail := "stable@example.com"
	body := dto.UpsertActorMappingRequest{
		DisplayName: &newDisplayName,
		Email:       &stableEmail,
	}

	resp := testUpsertActorMappingRequest(t, handler, "actor-mut-204", body)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
}

// AC5 (HTTP layer) — the pentest PoC. PUT against a pseudonymized
// row with plaintext PII must return 409 and MUST NOT overwrite the
// [REDACTED] values. We rely on the service/repository layers to enforce
// the actual storage invariant; this test checks only the HTTP-layer
// contract that the conflict is surfaced as a 409.
func TestPutActorMapping_OverRedacted_Returns409(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)
	repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, command.ErrActorMappingImmutable)

	handler := newTestActorMappingHandler(t, repo)

	attackerDisplayName := "Attacker Name"
	attackerEmail := "attacker@evil.example"
	body := dto.UpsertActorMappingRequest{
		DisplayName: &attackerDisplayName,
		Email:       &attackerEmail,
	}

	resp := testUpsertActorMappingRequest(t, handler, "actor-pseudo-205", body)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
}

// Compile-time guard: ensures the http and command packages are wired
// together in this test file even if individual cases are temporarily
// commented out.
var _ = http.StatusOK
