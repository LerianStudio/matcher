//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRevisionZero(t *testing.T) {
	t.Parallel()

	assert.Equal(t, Revision(0), RevisionZero)
	assert.Equal(t, uint64(0), RevisionZero.Uint64())
}

func TestRevision_Next(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rev  Revision
		want Revision
	}{
		{name: "zero to one", rev: RevisionZero, want: Revision(1)},
		{name: "one to two", rev: Revision(1), want: Revision(2)},
		{name: "large value increments", rev: Revision(999), want: Revision(1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.rev.Next())
		})
	}
}

func TestRevision_Uint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rev  Revision
		want uint64
	}{
		{name: "zero", rev: RevisionZero, want: 0},
		{name: "positive", rev: Revision(42), want: 42},
		{name: "large", rev: Revision(1<<32 + 1), want: 1<<32 + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.rev.Uint64())
		})
	}
}
