package ports

import (
	"context"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

//go:generate mockgen -destination=mocks/event_publisher_mock.go -package=mocks . MatchEventPublisher

// MatchEventPublisher emits matching lifecycle events.
// This interface mirrors the shared domain contract (sharedDomain.MatchEventPublisher)
// for use within the matching context.
//
// IMPORTANT: This interface MUST remain synchronized with internal/shared/domain/events.go.
// If you add or modify methods here, update the shared domain interface as well.
// The shared interface exists to allow cross-context access without direct coupling.
type MatchEventPublisher interface {
	PublishMatchConfirmed(ctx context.Context, event *sharedDomain.MatchConfirmedEvent) error
	PublishMatchUnmatched(ctx context.Context, event *sharedDomain.MatchUnmatchedEvent) error
}
