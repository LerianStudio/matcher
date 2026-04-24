// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package testutil

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFixedTime_IsUTC(t *testing.T) {
	t.Parallel()

	ft := FixedTime()
	assert.Equal(t, time.UTC, ft.Location())
}

func TestFixedTime_IsStable(t *testing.T) {
	t.Parallel()

	assert.Equal(t, FixedTime(), FixedTime())
}

func TestFixedTime_HasExpectedComponents(t *testing.T) {
	t.Parallel()

	ft := FixedTime()
	assert.Equal(t, 2026, ft.Year())
	assert.Equal(t, time.January, ft.Month())
	assert.Equal(t, 15, ft.Day())
	assert.Equal(t, 10, ft.Hour())
	assert.Equal(t, 30, ft.Minute())
}

func TestDeterministicUUID_IsStable(t *testing.T) {
	t.Parallel()

	id1 := DeterministicUUID("test-seed")
	id2 := DeterministicUUID("test-seed")
	assert.Equal(t, id1, id2)
}

func TestDeterministicUUID_DifferentSeedsDifferentIDs(t *testing.T) {
	t.Parallel()

	id1 := DeterministicUUID("seed-a")
	id2 := DeterministicUUID("seed-b")
	assert.NotEqual(t, id1, id2)
}

func TestDeterministicUUID_NeverNil(t *testing.T) {
	t.Parallel()

	id := DeterministicUUID("anything")
	assert.NotEqual(t, uuid.Nil, id)
}

func TestDeterministicUUID_EmptySeed(t *testing.T) {
	t.Parallel()

	id := DeterministicUUID("")
	assert.NotEqual(t, uuid.Nil, id)
}

func TestDeterministicUUIDs_CorrectCount(t *testing.T) {
	t.Parallel()

	ids := DeterministicUUIDs("item", 5)
	require.Len(t, ids, 5)
}

func TestDeterministicUUIDs_AllUnique(t *testing.T) {
	t.Parallel()

	ids := DeterministicUUIDs("item", 10)
	seen := make(map[uuid.UUID]bool)
	for _, id := range ids {
		assert.False(t, seen[id], "duplicate UUID found")
		seen[id] = true
	}
}

func TestDeterministicUUIDs_IsStable(t *testing.T) {
	t.Parallel()

	ids1 := DeterministicUUIDs("batch", 3)
	ids2 := DeterministicUUIDs("batch", 3)
	assert.Equal(t, ids1, ids2)
}

func TestDeterministicUUIDs_Zero(t *testing.T) {
	t.Parallel()

	ids := DeterministicUUIDs("item", 0)
	assert.Empty(t, ids)
}

func TestDeterministicNamespace_IsNotNil(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, uuid.Nil, deterministicNamespace)
}
