//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
)

// TestActorMapping_UpsertAndGet creates an actor mapping via PUT and retrieves it via GET,
// verifying that the round-trip preserves all fields.
func TestActorMapping_UpsertAndGet(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		actorID := "e2e-actor-" + tc.RunID()
		displayName := "E2E Test User"
		email := "e2e-test-" + tc.RunID() + "@example.com"

		// Upsert the actor mapping.
		upsertResp, err := apiClient.Governance.UpsertActorMapping(ctx, actorID, client.UpsertActorMappingRequest{
			DisplayName: strPtr(displayName),
			Email:       strPtr(email),
		})
		require.NoError(t, err, "upsert actor mapping should succeed")
		require.NotNil(t, upsertResp)

		assert.Equal(t, actorID, upsertResp.ActorID)
		require.NotNil(t, upsertResp.DisplayName)
		assert.Equal(t, displayName, *upsertResp.DisplayName)
		require.NotNil(t, upsertResp.Email)
		assert.Equal(t, email, *upsertResp.Email)
		assert.NotEmpty(t, upsertResp.CreatedAt)
		assert.NotEmpty(t, upsertResp.UpdatedAt)

		tc.Logf("Upserted actor mapping: actorID=%s", actorID)

		// Retrieve the same actor mapping by GET.
		getResp, err := apiClient.Governance.GetActorMapping(ctx, actorID)
		require.NoError(t, err, "get actor mapping should succeed")
		require.NotNil(t, getResp)

		assert.Equal(t, actorID, getResp.ActorID)
		require.NotNil(t, getResp.DisplayName)
		assert.Equal(t, displayName, *getResp.DisplayName)
		require.NotNil(t, getResp.Email)
		assert.Equal(t, email, *getResp.Email)

		tc.Logf("Retrieved actor mapping matches upsert: actorID=%s", actorID)
	})
}

// TestActorMapping_Update verifies that a second PUT updates the existing mapping
// while leaving unchanged fields intact.
func TestActorMapping_Update(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		actorID := "e2e-update-" + tc.RunID()
		originalName := "Original Name"
		email := "original-" + tc.RunID() + "@example.com"

		// Create initial mapping.
		_, err := apiClient.Governance.UpsertActorMapping(ctx, actorID, client.UpsertActorMappingRequest{
			DisplayName: strPtr(originalName),
			Email:       strPtr(email),
		})
		require.NoError(t, err)
		tc.Logf("Created initial actor mapping: actorID=%s", actorID)

		// Update only the display name.
		updatedName := "Updated Name"
		updateResp, err := apiClient.Governance.UpsertActorMapping(ctx, actorID, client.UpsertActorMappingRequest{
			DisplayName: strPtr(updatedName),
		})
		require.NoError(t, err, "update actor mapping should succeed")
		require.NotNil(t, updateResp)
		require.NotNil(t, updateResp.DisplayName)
		assert.Equal(t, updatedName, *updateResp.DisplayName, "put response should include updated display name")
		require.NotNil(t, updateResp.Email)
		assert.Equal(t, email, *updateResp.Email, "put response should preserve existing email")

		// Verify the update took effect.
		getResp, err := apiClient.Governance.GetActorMapping(ctx, actorID)
		require.NoError(t, err)
		require.NotNil(t, getResp)

		require.NotNil(t, getResp.DisplayName)
		assert.Equal(t, updatedName, *getResp.DisplayName, "display name should be updated")
		require.NotNil(t, getResp.Email)
		assert.Equal(t, email, *getResp.Email, "email should remain unchanged")

		tc.Logf("Actor mapping updated successfully: actorID=%s, displayName=%s", actorID, *getResp.DisplayName)
	})
}

// TestActorMapping_Delete verifies that DELETE removes the actor mapping and
// subsequent GET returns 404.
func TestActorMapping_Delete(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		actorID := "e2e-delete-" + tc.RunID()

		// Create an actor mapping first.
		_, err := apiClient.Governance.UpsertActorMapping(ctx, actorID, client.UpsertActorMappingRequest{
			DisplayName: strPtr("To Be Deleted"),
			Email:       strPtr("delete-me@example.com"),
		})
		require.NoError(t, err)
		tc.Logf("Created actor mapping for deletion: actorID=%s", actorID)

		// Delete the actor mapping.
		err = apiClient.Governance.DeleteActorMapping(ctx, actorID)
		require.NoError(t, err, "delete actor mapping should succeed")
		tc.Logf("Deleted actor mapping: actorID=%s", actorID)

		// GET should now return 404.
		_, err = apiClient.Governance.GetActorMapping(ctx, actorID)
		require.Error(t, err, "get after delete should fail")

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr), "error should be an APIError")
		assert.True(t, apiErr.IsNotFound(), "expected 404 after deletion, got %d", apiErr.StatusCode)

		tc.Logf("Confirmed actor mapping not found after delete: actorID=%s", actorID)
	})
}

// TestActorMapping_Pseudonymize verifies that the pseudonymize endpoint replaces
// PII fields (display name, email) with [REDACTED].
func TestActorMapping_Pseudonymize(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		actorID := "e2e-pseudo-" + tc.RunID()
		displayName := "Sensitive Name"
		email := "sensitive-" + tc.RunID() + "@example.com"

		// Create an actor mapping with PII.
		_, err := apiClient.Governance.UpsertActorMapping(ctx, actorID, client.UpsertActorMappingRequest{
			DisplayName: strPtr(displayName),
			Email:       strPtr(email),
		})
		require.NoError(t, err)
		tc.Logf("Created actor mapping with PII: actorID=%s", actorID)

		// Pseudonymize the actor.
		err = apiClient.Governance.PseudonymizeActor(ctx, actorID)
		require.NoError(t, err, "pseudonymize actor should succeed")
		tc.Logf("Pseudonymized actor: actorID=%s", actorID)

		// GET the actor — PII fields should be [REDACTED].
		getResp, err := apiClient.Governance.GetActorMapping(ctx, actorID)
		require.NoError(t, err, "get after pseudonymize should succeed")
		require.NotNil(t, getResp)

		assert.Equal(t, actorID, getResp.ActorID, "actor ID should be preserved")
		require.NotNil(t, getResp.DisplayName)
		assert.Equal(t, "[REDACTED]", *getResp.DisplayName, "display name should be redacted")
		require.NotNil(t, getResp.Email)
		assert.Equal(t, "[REDACTED]", *getResp.Email, "email should be redacted")

		tc.Logf("Confirmed PII fields are redacted: displayName=%s, email=%s",
			*getResp.DisplayName, *getResp.Email)
	})
}

// TestActorMapping_GetNonExistent verifies that GET for a non-existent actor ID
// returns a 404 error.
func TestActorMapping_GetNonExistent(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		nonExistentID := "non-existent-" + uuid.New().String()

		_, err := apiClient.Governance.GetActorMapping(ctx, nonExistentID)
		require.Error(t, err, "get non-existent actor mapping should fail")

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr), "error should be an APIError")
		assert.True(t, apiErr.IsNotFound(), "expected 404, got %d", apiErr.StatusCode)

		tc.Logf("Confirmed 404 for non-existent actor: %s", nonExistentID)
	})
}

// TestActorMapping_DeleteNonExistent verifies that DELETE for a non-existent actor ID
// returns a 404 error.
func TestActorMapping_DeleteNonExistent(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		nonExistentID := "non-existent-" + uuid.New().String()

		err := apiClient.Governance.DeleteActorMapping(ctx, nonExistentID)
		require.Error(t, err, "delete non-existent actor mapping should fail")

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr), "error should be an APIError")
		assert.True(t, apiErr.IsNotFound(), "expected 404, got %d", apiErr.StatusCode)

		tc.Logf("Confirmed 404 for deleting non-existent actor: %s", nonExistentID)
	})
}

// TestActorMapping_PseudonymizeNonExistent verifies that pseudonymizing a non-existent
// actor ID returns a 404 error.
func TestActorMapping_PseudonymizeNonExistent(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		nonExistentID := "non-existent-" + uuid.New().String()

		err := apiClient.Governance.PseudonymizeActor(ctx, nonExistentID)
		require.Error(t, err, "pseudonymize non-existent actor should fail")

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr), "error should be an APIError")
		assert.True(t, apiErr.IsNotFound(), "expected 404, got %d", apiErr.StatusCode)

		tc.Logf("Confirmed 404 for pseudonymizing non-existent actor: %s", nonExistentID)
	})
}

// TestActorMapping_PseudonymizeIdempotent verifies that pseudonymizing an already-
// redacted actor is idempotent (succeeds without changing the outcome).
func TestActorMapping_PseudonymizeIdempotent(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		actorID := "e2e-idempotent-" + tc.RunID()

		// Create actor with PII.
		_, err := apiClient.Governance.UpsertActorMapping(ctx, actorID, client.UpsertActorMappingRequest{
			DisplayName: strPtr("Will Be Redacted Twice"),
			Email:       strPtr("redact-twice@example.com"),
		})
		require.NoError(t, err)

		// First pseudonymize.
		err = apiClient.Governance.PseudonymizeActor(ctx, actorID)
		require.NoError(t, err, "first pseudonymize should succeed")

		// Second pseudonymize — should still succeed.
		err = apiClient.Governance.PseudonymizeActor(ctx, actorID)
		require.NoError(t, err, "second pseudonymize should be idempotent")

		// Verify fields are still [REDACTED].
		getResp, err := apiClient.Governance.GetActorMapping(ctx, actorID)
		require.NoError(t, err)
		require.NotNil(t, getResp)
		require.NotNil(t, getResp.DisplayName)
		assert.Equal(t, "[REDACTED]", *getResp.DisplayName)
		require.NotNil(t, getResp.Email)
		assert.Equal(t, "[REDACTED]", *getResp.Email)

		tc.Logf("Confirmed pseudonymize is idempotent: actorID=%s", actorID)
	})
}
