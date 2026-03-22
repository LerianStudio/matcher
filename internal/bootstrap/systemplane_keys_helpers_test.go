//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// --- concatKeyDefs ---

func TestConcatKeyDefs_MultipleGroups(t *testing.T) {
	t.Parallel()

	group1 := []domain.KeyDef{
		{Key: "a.1"},
		{Key: "a.2"},
	}
	group2 := []domain.KeyDef{
		{Key: "b.1"},
	}
	group3 := []domain.KeyDef{
		{Key: "c.1"},
		{Key: "c.2"},
		{Key: "c.3"},
	}

	result := concatKeyDefs(group1, group2, group3)

	assert.Len(t, result, 6)
	assert.Equal(t, "a.1", result[0].Key)
	assert.Equal(t, "a.2", result[1].Key)
	assert.Equal(t, "b.1", result[2].Key)
	assert.Equal(t, "c.1", result[3].Key)
	assert.Equal(t, "c.2", result[4].Key)
	assert.Equal(t, "c.3", result[5].Key)
}

func TestConcatKeyDefs_EmptyGroups(t *testing.T) {
	t.Parallel()

	result := concatKeyDefs(nil, []domain.KeyDef{}, nil)

	assert.Empty(t, result)
}

func TestConcatKeyDefs_SingleGroup(t *testing.T) {
	t.Parallel()

	group := []domain.KeyDef{{Key: "only.one"}}

	result := concatKeyDefs(group)

	assert.Len(t, result, 1)
	assert.Equal(t, "only.one", result[0].Key)
}

func TestConcatKeyDefs_NoArgs(t *testing.T) {
	t.Parallel()

	result := concatKeyDefs()

	assert.Empty(t, result)
}

func TestConcatKeyDefs_PreservesOrder(t *testing.T) {
	t.Parallel()

	group1 := []domain.KeyDef{{Key: "first"}, {Key: "second"}}
	group2 := []domain.KeyDef{{Key: "third"}, {Key: "fourth"}}

	result := concatKeyDefs(group1, group2)

	expected := []string{"first", "second", "third", "fourth"}
	for i, exp := range expected {
		assert.Equal(t, exp, result[i].Key, "index %d", i)
	}
}

func TestConcatKeyDefs_MixedEmptyAndNonEmpty(t *testing.T) {
	t.Parallel()

	group1 := []domain.KeyDef{{Key: "a"}}
	empty := []domain.KeyDef{}
	group2 := []domain.KeyDef{{Key: "b"}}

	result := concatKeyDefs(group1, empty, nil, group2)

	assert.Len(t, result, 2)
	assert.Equal(t, "a", result[0].Key)
	assert.Equal(t, "b", result[1].Key)
}
