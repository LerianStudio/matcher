// Package adapters provides infrastructure implementations for the exception bounded context.
package adapters

import (
	"context"
	"strings"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// Ensure AuthActorExtractor implements ports.ActorExtractor.
var _ ports.ActorExtractor = (*AuthActorExtractor)(nil)

// AuthActorExtractor implements ActorExtractor using the auth package.
type AuthActorExtractor struct{}

// NewAuthActorExtractor creates a new AuthActorExtractor.
func NewAuthActorExtractor() *AuthActorExtractor {
	return &AuthActorExtractor{}
}

// GetActor returns the actor ID from the context using auth.GetUserID.
func (e *AuthActorExtractor) GetActor(ctx context.Context) string {
	return strings.TrimSpace(auth.GetUserID(ctx))
}
