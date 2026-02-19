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

func TestActorMapping_UpsertAndGet(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Alice Johnson"
		email := "alice@example.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-001", &displayName, &email)
		require.NoError(t, err)

		err = repo.Upsert(ctx, mapping)
		require.NoError(t, err)

		fetched, err := repo.GetByActorID(ctx, "actor-001")
		require.NoError(t, err)

		require.Equal(t, "actor-001", fetched.ActorID)
		require.NotNil(t, fetched.DisplayName)
		require.Equal(t, "Alice Johnson", *fetched.DisplayName)
		require.NotNil(t, fetched.Email)
		require.Equal(t, "alice@example.com", *fetched.Email)
		require.False(t, fetched.CreatedAt.IsZero())
		require.False(t, fetched.UpdatedAt.IsZero())
	})
}

func TestActorMapping_UpsertUpdatesExisting(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Bob Original"
		email := "bob@example.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-002", &displayName, &email)
		require.NoError(t, err)

		err = repo.Upsert(ctx, mapping)
		require.NoError(t, err)

		original, err := repo.GetByActorID(ctx, "actor-002")
		require.NoError(t, err)
		require.Equal(t, "Bob Original", *original.DisplayName)

		// Upsert with updated display name — same actor ID triggers ON CONFLICT UPDATE.
		updatedName := "Bob Updated"
		updatedEmail := "bob.updated@example.com"

		updated, err := entities.NewActorMapping(ctx, "actor-002", &updatedName, &updatedEmail)
		require.NoError(t, err)

		err = repo.Upsert(ctx, updated)
		require.NoError(t, err)

		fetched, err := repo.GetByActorID(ctx, "actor-002")
		require.NoError(t, err)

		require.Equal(t, "actor-002", fetched.ActorID)
		require.NotNil(t, fetched.DisplayName)
		require.Equal(t, "Bob Updated", *fetched.DisplayName)
		require.NotNil(t, fetched.Email)
		require.Equal(t, "bob.updated@example.com", *fetched.Email)
		// updated_at should advance (or at least not regress) after the upsert.
		require.False(t, fetched.UpdatedAt.Before(original.CreatedAt),
			"updated_at should not be before the original created_at")
	})
}

func TestActorMapping_Pseudonymize(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Charlie Sensitive"
		email := "charlie@pii.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-003", &displayName, &email)
		require.NoError(t, err)

		err = repo.Upsert(ctx, mapping)
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

func TestActorMapping_Delete(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		displayName := "Dave Erasable"
		email := "dave@eraseme.com"

		mapping, err := entities.NewActorMapping(ctx, "actor-004", &displayName, &email)
		require.NoError(t, err)

		err = repo.Upsert(ctx, mapping)
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

func TestActorMapping_GetNonExistent(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := actormapping.NewRepository(h.Provider())
		ctx := h.Ctx()

		result, err := repo.GetByActorID(ctx, "actor-does-not-exist")
		require.ErrorIs(t, err, actormapping.ErrActorMappingNotFound)
		require.Nil(t, result)
	})
}

func TestActorMapping_MultipleActors(t *testing.T) {
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

				err = repo.Upsert(ctx, mapping)
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
