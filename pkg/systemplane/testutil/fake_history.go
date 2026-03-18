// Copyright 2025 Lerian Studio.

// Package testutil provides test doubles for systemplane components.
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
	entries []fakeHistoryRecord
}

type fakeHistoryRecord struct {
	kind  domain.Kind
	entry ports.HistoryEntry
}

// NewFakeHistoryStore creates an empty FakeHistoryStore.
func NewFakeHistoryStore() *FakeHistoryStore {
	return &FakeHistoryStore{}
}

// Append adds a history entry using scope-based kind inference.
//
// WARNING: This heuristic maps ScopeTenant → KindSetting and everything else
// → KindConfig. Global-scoped settings will be misclassified as KindConfig.
// Prefer AppendForKind when the caller knows the entry's kind.
func (store *FakeHistoryStore) Append(entry ports.HistoryEntry) {
	store.AppendForKind(inferHistoryKind(entry), entry)
}

// AppendForKind adds a history entry with an explicit kind. Prefer this helper
// when the caller knows whether the record is for configs or settings.
func (store *FakeHistoryStore) AppendForKind(kind domain.Kind, entry ports.HistoryEntry) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.entries = append(store.entries, fakeHistoryRecord{kind: kind, entry: entry})
}

// ListHistory retrieves history entries matching the given filter. Results are
// returned in reverse chronological order (newest first). Filtering is applied
// for Target (if non-nil) and Before (if non-nil), and the result is capped
// at filter.Limit when Limit > 0.
func (store *FakeHistoryStore) ListHistory(_ context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	// Iterate in reverse to produce newest-first ordering.
	result := make([]ports.HistoryEntry, 0, len(store.entries))

	for index := len(store.entries) - 1; index >= 0; index-- {
		record := store.entries[index]

		if !matchesHistoryFilter(filter, record) {
			continue
		}

		result = append(result, record.entry)
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

func matchesHistoryFilter(filter ports.HistoryFilter, record fakeHistoryRecord) bool {
	if !matchesKindFilter(filter, record) {
		return false
	}

	entry := record.entry

	if filter.Scope != "" && entry.Scope != filter.Scope {
		return false
	}

	if filter.SubjectID != "" && entry.SubjectID != filter.SubjectID {
		return false
	}

	if filter.Key != "" && entry.Key != filter.Key {
		return false
	}

	return true
}

func matchesKindFilter(filter ports.HistoryFilter, record fakeHistoryRecord) bool {
	if filter.Kind == "" {
		return true
	}

	return record.kind == filter.Kind
}

func inferHistoryKind(entry ports.HistoryEntry) domain.Kind {
	if entry.Scope == domain.ScopeTenant {
		return domain.KindSetting
	}

	return domain.KindConfig
}
