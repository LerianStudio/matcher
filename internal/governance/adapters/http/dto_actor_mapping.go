package http

import (
	"time"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// UpsertActorMappingRequest represents the request body for creating/updating an actor mapping.
// At least one of display_name or email must be provided.
// @Description Request body for upserting an actor mapping
type UpsertActorMappingRequest struct {
	// Human-readable display name for the actor
	DisplayName *string `json:"display_name" example:"John Doe"`
	// Email address of the actor
	Email *string `json:"email" validate:"omitempty,email" example:"john.doe@example.com"`
}

// ActorMappingResponse represents an actor mapping in API responses.
// @Description Actor mapping entry linking an opaque actor ID to PII
type ActorMappingResponse struct {
	// Opaque actor identifier
	ActorID string `json:"actor_id" example:"user:550e8400-e29b-41d4-a716-446655440000"`
	// Human-readable display name
	DisplayName *string `json:"display_name,omitempty" example:"John Doe"`
	// Email address
	Email *string `json:"email,omitempty" example:"john.doe@example.com"`
	// Timestamp when the mapping was created (RFC3339)
	CreatedAt string `json:"created_at" example:"2026-01-15T10:30:00Z"`
	// Timestamp when the mapping was last updated (RFC3339)
	UpdatedAt string `json:"updated_at" example:"2026-01-15T10:30:00Z"`
}

// ActorMappingToResponse converts a domain entity to an API response.
func ActorMappingToResponse(mapping *entities.ActorMapping) ActorMappingResponse {
	if mapping == nil {
		return ActorMappingResponse{}
	}

	return ActorMappingResponse{
		ActorID:     mapping.ActorID,
		DisplayName: mapping.DisplayName,
		Email:       mapping.Email,
		CreatedAt:   mapping.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   mapping.UpdatedAt.Format(time.RFC3339),
	}
}
