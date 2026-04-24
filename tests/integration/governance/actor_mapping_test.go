//go:build integration

package governance

import (
	"fmt"
	"testing"

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

func TestIntegration_Governance_ActorMapping_UpsertUpdatesExisting(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Bob Original"
		email := "bob@example.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-002", &displayName, &email)
		require.NoError(t, err)

		original, err := repo.Upsert(ctx, mapping)
		require.NoError(t, err)
		require.NotNil(t, original)

		readBack, err := repo.GetByActorID(ctx, "actor-002")
		require.NoError(t, err)
		require.Equal(t, "Bob Original", *readBack.DisplayName)

		// Upsert with updated display name — same actor ID triggers ON CONFLICT UPDATE.
		updatedName := "Bob Updated"
		updatedEmail := "bob.updated@example.com"

		updated, err := entities.NewActorMapping(ctx, "actor-002", &updatedName, &updatedEmail)
		require.NoError(t, err)

		updateResult, err := repo.Upsert(ctx, updated)
		require.NoError(t, err)
		require.NotNil(t, updateResult)
		require.Equal(t, "actor-002", updateResult.ActorID)
		require.NotNil(t, updateResult.DisplayName)
		require.Equal(t, "Bob Updated", *updateResult.DisplayName)
		require.NotNil(t, updateResult.Email)
		require.Equal(t, "bob.updated@example.com", *updateResult.Email)

		fetched, err := repo.GetByActorID(ctx, "actor-002")
		require.NoError(t, err)

		require.Equal(t, "actor-002", fetched.ActorID)
		require.NotNil(t, fetched.DisplayName)
		require.Equal(t, "Bob Updated", *fetched.DisplayName)
		require.NotNil(t, fetched.Email)
		require.Equal(t, "bob.updated@example.com", *fetched.Email)
		// Verify created_at preserved from original insert (RETURNING correctness).
		require.Equal(t, original.CreatedAt.Unix(), fetched.CreatedAt.Unix(),
			"created_at should be preserved from original insert")
	})
}

func TestIntegration_Governance_ActorMapping_UpsertPreservesExistingFieldWhenOmitted(t *testing.T) {
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

		updatedName := "Carol Updated"
		partialUpdate, err := entities.NewActorMapping(ctx, "actor-omit", &updatedName, nil)
		require.NoError(t, err)

		result, err := repo.Upsert(ctx, partialUpdate)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.DisplayName)
		require.Equal(t, updatedName, *result.DisplayName)
		require.NotNil(t, result.Email)
		require.Equal(t, email, *result.Email)
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

		// Pseudonymize replaces PII with [REDACTED].
		err = repo.Pseudonymize(ctx, "actor-003")
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
