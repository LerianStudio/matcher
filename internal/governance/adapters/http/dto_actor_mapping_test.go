//go:build unit

package http

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

func TestActorMappingToResponse(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty response", func(t *testing.T) {
		t.Parallel()

		resp := ActorMappingToResponse(nil)
		assert.Empty(t, resp.ActorID)
		assert.Nil(t, resp.DisplayName)
		assert.Nil(t, resp.Email)
		assert.Empty(t, resp.CreatedAt)
		assert.Empty(t, resp.UpdatedAt)
	})

	t.Run("full conversion with all fields", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
		displayName := "John Doe"
		email := "john@example.com"

		mapping := &entities.ActorMapping{
			ActorID:     "user:550e8400-e29b-41d4-a716-446655440000",
			DisplayName: &displayName,
			Email:       &email,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		resp := ActorMappingToResponse(mapping)
		assert.Equal(t, "user:550e8400-e29b-41d4-a716-446655440000", resp.ActorID)
		require.NotNil(t, resp.DisplayName)
		assert.Equal(t, "John Doe", *resp.DisplayName)
		require.NotNil(t, resp.Email)
		assert.Equal(t, "john@example.com", *resp.Email)
		assert.Equal(t, "2026-01-15T10:30:00Z", resp.CreatedAt)
		assert.Equal(t, "2026-01-15T10:30:00Z", resp.UpdatedAt)
	})

	t.Run("conversion with nil optional fields", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

		mapping := &entities.ActorMapping{
			ActorID:     "system:cron-job",
			DisplayName: nil,
			Email:       nil,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		resp := ActorMappingToResponse(mapping)
		assert.Equal(t, "system:cron-job", resp.ActorID)
		assert.Nil(t, resp.DisplayName)
		assert.Nil(t, resp.Email)
		assert.Equal(t, "2026-02-01T00:00:00Z", resp.CreatedAt)
		assert.Equal(t, "2026-02-01T00:00:00Z", resp.UpdatedAt)
	})

	t.Run("conversion with redacted fields", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		redacted := "[REDACTED]"

		mapping := &entities.ActorMapping{
			ActorID:     "user:gdpr-removed",
			DisplayName: &redacted,
			Email:       &redacted,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		resp := ActorMappingToResponse(mapping)
		assert.Equal(t, "user:gdpr-removed", resp.ActorID)
		require.NotNil(t, resp.DisplayName)
		assert.Equal(t, "[REDACTED]", *resp.DisplayName)
		require.NotNil(t, resp.Email)
		assert.Equal(t, "[REDACTED]", *resp.Email)
	})
}

func TestUpsertActorMappingRequestStructure(t *testing.T) {
	t.Parallel()

	// Verify the struct can be instantiated with nil fields.
	req := UpsertActorMappingRequest{}
	assert.Nil(t, req.DisplayName)
	assert.Nil(t, req.Email)

	// Verify with set fields.
	displayName := "Alice"
	email := "alice@example.com"
	req = UpsertActorMappingRequest{
		DisplayName: &displayName,
		Email:       &email,
	}
	require.NotNil(t, req.DisplayName)
	assert.Equal(t, "Alice", *req.DisplayName)
	require.NotNil(t, req.Email)
	assert.Equal(t, "alice@example.com", *req.Email)
}

func TestActorMappingResponseStructure(t *testing.T) {
	t.Parallel()

	resp := ActorMappingResponse{
		ActorID:   "test-actor",
		CreatedAt: "2026-01-01T00:00:00Z",
		UpdatedAt: "2026-01-01T00:00:00Z",
	}
	assert.Equal(t, "test-actor", resp.ActorID)
	assert.Nil(t, resp.DisplayName)
	assert.Nil(t, resp.Email)
}
