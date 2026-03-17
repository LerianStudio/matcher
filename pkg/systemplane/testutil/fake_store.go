// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.Store = (*FakeStore)(nil)

// FakeStore is an in-memory implementation of ports.Store for testing.
// It enforces optimistic concurrency: Put returns domain.ErrRevisionMismatch
// when the expected revision does not match the current one.
type FakeStore struct {
	mu      sync.Mutex
	targets map[string]*targetState
}

type targetState struct {
	entries  map[string]domain.Entry
	revision domain.Revision
}

// NewFakeStore creates an empty FakeStore ready for use.
func NewFakeStore() *FakeStore {
	return &FakeStore{
		targets: make(map[string]*targetState),
	}
}

// Seed pre-populates entries for a target at the given revision.
// This is a test-setup helper; it overwrites any existing state for the target.
func (s *FakeStore) Seed(target domain.Target, entries []domain.Entry, revision domain.Revision) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := &targetState{
		entries:  make(map[string]domain.Entry, len(entries)),
		revision: revision,
	}

	for _, e := range entries {
		state.entries[e.Key] = e
	}

	s.targets[target.String()] = state
}

// Get retrieves all entries for a target at its current revision.
// If the target has never been written, it returns an empty slice and RevisionZero.
func (s *FakeStore) Get(_ context.Context, target domain.Target) (ports.ReadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.targets[target.String()]
	if !ok {
		return ports.ReadResult{
			Entries:  nil,
			Revision: domain.RevisionZero,
		}, nil
	}

	entries := make([]domain.Entry, 0, len(state.entries))
	for _, e := range state.entries {
		entries = append(entries, e)
	}

	return ports.ReadResult{
		Entries:  entries,
		Revision: state.revision,
	}, nil
}

// Put atomically writes a batch of operations for a target.
// It returns domain.ErrRevisionMismatch when the expected revision does not
// match the current revision. For Reset ops or nil-Value ops, the entry is
// deleted from the map. Otherwise the entry is upserted with fresh metadata.
func (s *FakeStore) Put(_ context.Context, target domain.Target, ops []ports.WriteOp,
	expected domain.Revision, actor domain.Actor, source string,
) (domain.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := target.String()

	state, ok := s.targets[key]
	if !ok {
		state = &targetState{
			entries:  make(map[string]domain.Entry),
			revision: domain.RevisionZero,
		}
		s.targets[key] = state
	}

	if expected != state.revision {
		return state.revision, domain.ErrRevisionMismatch
	}

	now := time.Now().UTC()

	for _, op := range ops {
		if op.Reset || op.Value == nil {
			delete(state.entries, op.Key)

			continue
		}

		state.entries[op.Key] = domain.Entry{
			Kind:      target.Kind,
			Scope:     target.Scope,
			Subject:   target.SubjectID,
			Key:       op.Key,
			Value:     op.Value,
			Revision:  state.revision.Next(),
			UpdatedAt: now,
			UpdatedBy: actor.ID,
			Source:    source,
		}
	}

	state.revision = state.revision.Next()

	return state.revision, nil
}
