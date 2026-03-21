// Copyright 2025 Lerian Studio.

package ports

import (
	"context"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// HistoryEntry represents a single change record in the audit trail.
type HistoryEntry struct {
	Revision  domain.Revision
	Key       string
	Scope     domain.Scope
	SubjectID string
	OldValue  any
	NewValue  any
	ActorID   string
	ChangedAt time.Time
}

// HistoryFilter controls which history entries to retrieve.
type HistoryFilter struct {
	Kind      domain.Kind
	Scope     domain.Scope
	SubjectID string
	Key       string
	Limit     int
	Offset    int
}

// HistoryStore provides read access to the configuration change audit trail.
type HistoryStore interface {
	// ListHistory retrieves history entries matching the given filter.
	ListHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
}
