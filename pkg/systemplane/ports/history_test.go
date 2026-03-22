//go:build unit

// Copyright 2025 Lerian Studio.

package ports

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubHistoryStore is a minimal test double for HistoryStore.
type stubHistoryStore struct {
	entries []HistoryEntry
	err     error
}

func (s *stubHistoryStore) ListHistory(_ context.Context, _ HistoryFilter) ([]HistoryEntry, error) {
	return s.entries, s.err
}

// Compile-time interface check.
var _ HistoryStore = (*stubHistoryStore)(nil)

func TestHistoryEntry_ZeroValue(t *testing.T) {
	t.Parallel()

	var entry HistoryEntry

	assert.Equal(t, domain.RevisionZero, entry.Revision)
	assert.Empty(t, entry.Key)
	assert.Empty(t, string(entry.Scope))
	assert.Empty(t, entry.SubjectID)
	assert.Nil(t, entry.OldValue)
	assert.Nil(t, entry.NewValue)
	assert.Empty(t, entry.ActorID)
	assert.True(t, entry.ChangedAt.IsZero())
}

func TestHistoryEntry_FieldAssignment(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entry := HistoryEntry{
		Revision:  domain.Revision(5),
		Key:       "log_level",
		Scope:     domain.ScopeGlobal,
		SubjectID: "",
		OldValue:  "info",
		NewValue:  "debug",
		ActorID:   "user-1",
		ChangedAt: now,
	}

	assert.Equal(t, domain.Revision(5), entry.Revision)
	assert.Equal(t, "log_level", entry.Key)
	assert.Equal(t, domain.ScopeGlobal, entry.Scope)
	assert.Equal(t, "info", entry.OldValue)
	assert.Equal(t, "debug", entry.NewValue)
	assert.Equal(t, "user-1", entry.ActorID)
	assert.Equal(t, now, entry.ChangedAt)
}

func TestHistoryFilter_ZeroValue(t *testing.T) {
	t.Parallel()

	var filter HistoryFilter

	assert.Empty(t, string(filter.Kind))
	assert.Empty(t, string(filter.Scope))
	assert.Empty(t, filter.SubjectID)
	assert.Empty(t, filter.Key)
	assert.Equal(t, 0, filter.Limit)
	assert.Equal(t, 0, filter.Offset)
}

func TestHistoryStore_CompileCheck(t *testing.T) {
	t.Parallel()

	var store HistoryStore = &stubHistoryStore{}
	require.NotNil(t, store)
}

func TestHistoryStore_ListHistory_ReturnsEntries(t *testing.T) {
	t.Parallel()

	want := []HistoryEntry{
		{Key: "k1", Revision: domain.Revision(1)},
		{Key: "k2", Revision: domain.Revision(2)},
	}
	store := &stubHistoryStore{entries: want}

	got, err := store.ListHistory(context.Background(), HistoryFilter{})

	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestHistoryStore_ListHistory_ReturnsError(t *testing.T) {
	t.Parallel()

	store := &stubHistoryStore{err: assert.AnError}

	_, err := store.ListHistory(context.Background(), HistoryFilter{})

	require.ErrorIs(t, err, assert.AnError)
}
