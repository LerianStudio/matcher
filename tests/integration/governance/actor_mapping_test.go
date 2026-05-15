//go:build integration

package governance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	actormapping "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/actor_mapping"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Governance_ActorMapping_UpsertAndGet(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Alice Johnson"
		email := "alice@example.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-001", &displayName, &email)
		require.NoError(t, err)

		result, err := repo.Upsert(ctx, mapping)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "actor-001", result.ActorID)
		require.NotNil(t, result.DisplayName)
		require.Equal(t, "Alice Johnson", *result.DisplayName)
		require.NotNil(t, result.Email)
		require.Equal(t, "alice@example.com", *result.Email)
		require.False(t, result.CreatedAt.IsZero())
		require.False(t, result.UpdatedAt.IsZero())

		// Also verify via separate read to confirm persistence.
		fetched, err := repo.GetByActorID(ctx, "actor-001")
		require.NoError(t, err)
		require.Equal(t, result.ActorID, fetched.ActorID)
	})
}

// TestIntegration_Governance_ActorMapping_UpsertPartialPayloadIsRejected verifies
// that a second PUT with a partial payload (e.g., a different display_name and a
// nil email) against an existing row is rejected with ErrActorMappingImmutable
// and the persisted row remains untouched.
//
// Pre-fix (vulnerable) behaviour was to COALESCE the omitted field with the
// existing value, effectively letting partial PUTs mutate identity fields. The
// new contract is "actor_mapping rows are immutable": any payload that differs
// from the stored row — including via field omission — is rejected.
//
// The broader payload-mismatch scenarios live in
// internal/governance/adapters/postgres/actor_mapping/actor_mapping_immutability_integration_test.go.
// This test pins the specific nil-field (no-COALESCE) angle from the package-level
// integration harness.
func TestIntegration_Governance_ActorMapping_UpsertPartialPayloadIsRejected(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Carol Original"
		email := "carol@example.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-omit", &displayName, &email)
		require.NoError(t, err)

		_, err = repo.Upsert(ctx, mapping)
		require.NoError(t, err)

		// Partial payload: different display_name, nil email. Under the old
		// COALESCE-based path this would have silently mutated display_name and
		// preserved email. Under the new immutability contract the comparison
		// must observe a mismatch (display_name differs; nil email differs from
		// the stored value) and reject the write.
		updatedName := "Carol Updated"
		partialUpdate, err := entities.NewActorMapping(ctx, "actor-omit", &updatedName, nil)
		require.NoError(t, err)

		result, err := repo.Upsert(ctx, partialUpdate)
		require.Error(t, err, "partial PUT against an existing row must be rejected")
		require.Nil(t, result)
		require.ErrorIs(t, err, actormapping.ErrActorMappingImmutable,
			"the rejection must surface ErrActorMappingImmutable so handlers map it to 409")

		// Persisted row must remain untouched — no COALESCE escape hatch.
		fetched, err := repo.GetByActorID(ctx, "actor-omit")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.NotNil(t, fetched.DisplayName)
		assert.Equal(t, "Carol Original", *fetched.DisplayName,
			"persisted display_name must survive the rejected partial PUT")
		require.NotNil(t, fetched.Email)
		assert.Equal(t, "carol@example.com", *fetched.Email,
			"persisted email must survive the rejected partial PUT")
	})
}

func TestIntegration_Governance_ActorMapping_Pseudonymize(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Charlie Sensitive"
		email := "charlie@pii.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-003", &displayName, &email)
		require.NoError(t, err)

		_, err = repo.Upsert(ctx, mapping)
		require.NoError(t, err)

		// PseudonymizeWithTx replaces PII with [REDACTED]. Passing nil tx
		// makes the repository open its own tenant-scoped transaction, which
		// is fine for the integration test since we are not coupling with a
		// streaming emit here — that coupling is exercised at the service
		// layer in unit tests.
		err = repo.PseudonymizeWithTx(ctx, nil, "actor-003")
		require.NoError(t, err)

		fetched, err := repo.GetByActorID(ctx, "actor-003")
		require.NoError(t, err)

		require.Equal(t, "actor-003", fetched.ActorID, "actor ID must survive pseudonymization")
		require.NotNil(t, fetched.DisplayName)
		require.Equal(t, "[REDACTED]", *fetched.DisplayName)
		require.NotNil(t, fetched.Email)
		require.Equal(t, "[REDACTED]", *fetched.Email)
		require.True(t, fetched.IsRedacted(), "IsRedacted() should return true after pseudonymization")
	})
}

func TestIntegration_Governance_ActorMapping_Delete(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Dave Erasable"
		email := "dave@eraseme.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-004", &displayName, &email)
		require.NoError(t, err)

		_, err = repo.Upsert(ctx, mapping)
		require.NoError(t, err)

		// Verify it exists before deletion.
		_, err = repo.GetByActorID(ctx, "actor-004")
		require.NoError(t, err)

		// Right-to-erasure: delete the mapping entirely.
		err = repo.Delete(ctx, "actor-004")
		require.NoError(t, err)

		// GetByActorID should return ErrActorMappingNotFound.
		result, err := repo.GetByActorID(ctx, "actor-004")
		require.ErrorIs(t, err, actormapping.ErrActorMappingNotFound)
		require.Nil(t, result)
	})
}

func TestIntegration_Governance_ActorMapping_GetNonExistent(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		result, err := repo.GetByActorID(ctx, "actor-does-not-exist")
		require.ErrorIs(t, err, actormapping.ErrActorMappingNotFound)
		require.Nil(t, result)
	})
}

func TestIntegration_Governance_ActorMapping_MultipleActors(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		type actor struct {
			id          string
			displayName string
			email       string
		}

		actors := []actor{
			{id: "multi-actor-A", displayName: "Alpha User", email: "alpha@example.com"},
			{id: "multi-actor-B", displayName: "Beta User", email: "beta@example.com"},
			{id: "multi-actor-C", displayName: "Gamma User", email: "gamma@example.com"},
		}

		for _, a := range actors {
			a := a

			t.Run(fmt.Sprintf("%s-%s", a.id, a.displayName), func(t *testing.T) {
				name := a.displayName
				mail := a.email

				mapping, err := entities.NewActorMapping(ctx, a.id, &name, &mail)
				require.NoError(t, err)

				_, err = repo.Upsert(ctx, mapping)
				require.NoError(t, err)

				fetched, err := repo.GetByActorID(ctx, a.id)
				require.NoError(t, err)

				require.Equal(t, a.id, fetched.ActorID)
				require.NotNil(t, fetched.DisplayName)
				require.Equal(t, a.displayName, *fetched.DisplayName)
				require.NotNil(t, fetched.Email)
				require.Equal(t, a.email, *fetched.Email)
			})
		}
	})
}
