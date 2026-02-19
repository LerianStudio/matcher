// Package ports defines outbound interfaces for the exception bounded context.
package ports

import "context"

//go:generate mockgen -destination=mocks/actor_extractor_mock.go -package=mocks . ActorExtractor

// ActorExtractor extracts the current actor (user) from the request context.
// This interface decouples use cases from the auth package, enabling:
// - Easier testing with mock implementations
// - Clear dependency injection
// - Explicit contract for actor identification.
type ActorExtractor interface {
	// GetActor returns the actor ID from the context.
	// Returns empty string if no actor is present.
	GetActor(ctx context.Context) string
}
