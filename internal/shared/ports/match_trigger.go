package ports

import (
	"context"

	"github.com/google/uuid"
)

//go:generate mockgen -source=match_trigger.go -destination=mocks/match_trigger_mock.go -package=mocks

// MatchTrigger defines the interface for triggering a match run.
// This is a shared port interface that allows cross-context communication
// (e.g., ingestion or configuration triggering matching) without direct
// dependencies between bounded contexts.
type MatchTrigger interface {
	// TriggerMatchForContext starts an asynchronous match run for the given context.
	// It fires and forgets; errors are logged but do not affect the caller.
	TriggerMatchForContext(ctx context.Context, tenantID, contextID uuid.UUID)
}

// ContextProvider defines the interface for checking context configuration.
// This allows contexts to check auto-match settings without directly
// importing configuration domain types.
type ContextProvider interface {
	// IsAutoMatchEnabled returns whether auto-match on upload is enabled for the context.
	IsAutoMatchEnabled(ctx context.Context, contextID uuid.UUID) (bool, error)
}
