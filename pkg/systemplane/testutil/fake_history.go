// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"sync"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.HistoryStore = (*FakeHistoryStore)(nil)

// FakeHistoryStore is an in-memory implementation of ports.HistoryStore.
// Entries are stored in insertion order and returned newest-first by
// ListHistory.
type FakeHistoryStore struct {
	mu      sync.Mutex
	entries []ports.HistoryEntry
}

// NewFakeHistoryStore creates an empty FakeHistoryStore.
func NewFakeHistoryStore() *FakeHistoryStore {
	return &FakeHistoryStore{}
}

// Append adds a history entry. This is a test-setup helper for pre-populating
// history or for recording writes from an external caller.
func (h *FakeHistoryStore) Append(entry ports.HistoryEntry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.entries = append(h.entries, entry)
}

// ListHistory retrieves history entries matching the given filter. Results are
// returned in reverse chronological order (newest first). Filtering is applied
// for Target (if non-nil) and Before (if non-nil), and the result is capped
// at filter.Limit when Limit > 0.
func (h *FakeHistoryStore) ListHistory(_ context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Iterate in reverse to produce newest-first ordering.
	result := make([]ports.HistoryEntry, 0, len(h.entries))

	for i := len(h.entries) - 1; i >= 0; i-- {
		e := h.entries[i]

		if filter.Kind != "" {
			if filter.Kind == domain.KindConfig && e.Scope != domain.ScopeGlobal {
				continue
			}
			if filter.Kind == domain.KindSetting && filter.Scope == domain.ScopeGlobal && e.Scope != domain.ScopeGlobal {
				continue
			}
		}

		if filter.Scope != "" && e.Scope != filter.Scope {
			continue
		}

		if filter.SubjectID != "" && e.SubjectID != filter.SubjectID {
			continue
		}

		if filter.Key != "" && e.Key != filter.Key {
			continue
		}

		result = append(result, e)
	}

	if filter.Offset > 0 {
		if filter.Offset >= len(result) {
			return []ports.HistoryEntry{}, nil
		}
		result = result[filter.Offset:]
	}

	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}

	return result, nil
}
