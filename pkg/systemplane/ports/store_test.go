//go:build unit

// Copyright 2025 Lerian Studio.

package ports

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubStore is a minimal test double for Store.
type stubStore struct {
	getResult ReadResult
	getErr    error
	putRev    domain.Revision
	putErr    error
}

func (s *stubStore) Get(_ context.Context, _ domain.Target) (ReadResult, error) {
	return s.getResult, s.getErr
}

func (s *stubStore) Put(_ context.Context, _ domain.Target, _ []WriteOp,
	_ domain.Revision, _ domain.Actor, _ string,
) (domain.Revision, error) {
	return s.putRev, s.putErr
}

// Compile-time interface check.
var _ Store = (*stubStore)(nil)

func TestWriteOp_ZeroValue(t *testing.T) {
	t.Parallel()

	var op WriteOp

	assert.Empty(t, op.Key)
	assert.Nil(t, op.Value)
	assert.False(t, op.Reset)
}

func TestWriteOp_FieldAssignment(t *testing.T) {
	t.Parallel()

	op := WriteOp{
		Key:   "log_level",
		Value: "debug",
		Reset: false,
	}

	assert.Equal(t, "log_level", op.Key)
	assert.Equal(t, "debug", op.Value)
	assert.False(t, op.Reset)
}

func TestWriteOp_ResetFlag(t *testing.T) {
	t.Parallel()

	op := WriteOp{
		Key:   "timeout",
		Reset: true,
	}

	assert.True(t, op.Reset)
	assert.Nil(t, op.Value)
}

func TestReadResult_ZeroValue(t *testing.T) {
	t.Parallel()

	var result ReadResult

	assert.Nil(t, result.Entries)
	assert.Equal(t, domain.RevisionZero, result.Revision)
}

func TestReadResult_FieldAssignment(t *testing.T) {
	t.Parallel()

	entries := []domain.Entry{
		{Key: "k1", Value: "v1"},
	}
	result := ReadResult{
		Entries:  entries,
		Revision: domain.Revision(3),
	}

	assert.Len(t, result.Entries, 1)
	assert.Equal(t, domain.Revision(3), result.Revision)
}

func TestStore_CompileCheck(t *testing.T) {
	t.Parallel()

	var store Store = &stubStore{}
	require.NotNil(t, store)
}

func TestStore_Get_ReturnsResult(t *testing.T) {
	t.Parallel()

	want := ReadResult{
		Entries:  []domain.Entry{{Key: "k", Value: "v"}},
		Revision: domain.Revision(5),
	}
	store := &stubStore{getResult: want}

	got, err := store.Get(context.Background(), domain.Target{})

	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestStore_Get_ReturnsError(t *testing.T) {
	t.Parallel()

	store := &stubStore{getErr: assert.AnError}

	_, err := store.Get(context.Background(), domain.Target{})

	require.ErrorIs(t, err, assert.AnError)
}

func TestStore_Put_ReturnsRevision(t *testing.T) {
	t.Parallel()

	store := &stubStore{putRev: domain.Revision(10)}

	rev, err := store.Put(
		context.Background(),
		domain.Target{},
		[]WriteOp{{Key: "k", Value: "v"}},
		domain.RevisionZero,
		domain.Actor{ID: "user-1"},
		"test",
	)

	require.NoError(t, err)
	assert.Equal(t, domain.Revision(10), rev)
}

func TestStore_Put_ReturnsError(t *testing.T) {
	t.Parallel()

	store := &stubStore{putErr: domain.ErrRevisionMismatch}

	_, err := store.Put(
		context.Background(),
		domain.Target{},
		[]WriteOp{{Key: "k", Value: "v"}},
		domain.RevisionZero,
		domain.Actor{ID: "user-1"},
		"test",
	)

	require.ErrorIs(t, err, domain.ErrRevisionMismatch)
}
