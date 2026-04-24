// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestIndexTransactions_EmptySlice(t *testing.T) {
	t.Parallel()

	result := indexTransactions(nil)
	assert.Empty(t, result)
}

func TestIndexTransactions_MultipleTxPreservesAll(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000260010")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000260011")
	txs := []*shared.Transaction{
		{ID: id1, Amount: decimal.NewFromInt(100)},
		{ID: id2, Amount: decimal.NewFromInt(200)},
	}

	result := indexTransactions(txs)
	assert.Len(t, result, 2)
	assert.NotNil(t, result[id1])
	assert.NotNil(t, result[id2])
}

func TestMergeTransactionMaps_SingleMap(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260020")
	tx := &shared.Transaction{ID: id}

	result := mergeTransactionMaps(map[uuid.UUID]*shared.Transaction{id: tx})
	assert.Len(t, result, 1)
	assert.Equal(t, tx, result[id])
}

func TestMergeTransactionMaps_OverlappingKeys_LastWins(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260040")
	tx1 := &shared.Transaction{ID: id, Amount: decimal.NewFromInt(100)}
	tx2 := &shared.Transaction{ID: id, Amount: decimal.NewFromInt(200)}

	m1 := map[uuid.UUID]*shared.Transaction{id: tx1}
	m2 := map[uuid.UUID]*shared.Transaction{id: tx2}

	result := mergeTransactionMaps(m1, m2)
	assert.Len(t, result, 1)
	assert.True(t, result[id].Amount.Equal(decimal.NewFromInt(200)))
}

func TestMergeTransactionMaps_ThreeMaps(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000260030")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000260031")
	id3 := uuid.MustParse("00000000-0000-0000-0000-000000260032")

	m1 := map[uuid.UUID]*shared.Transaction{id1: {ID: id1}}
	m2 := map[uuid.UUID]*shared.Transaction{id2: {ID: id2}}
	m3 := map[uuid.UUID]*shared.Transaction{id3: {ID: id3}}

	result := mergeTransactionMaps(m1, m2, m3)
	assert.Len(t, result, 3)
}

func TestCollectUnmatched_MixedMatchedAndUnmatched(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000260060")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000260061")
	id3 := uuid.MustParse("00000000-0000-0000-0000-000000260062")
	txs := []*shared.Transaction{{ID: id1}, {ID: id2}, {ID: id3}}
	matched := map[uuid.UUID]struct{}{id2: {}}

	result := collectUnmatched(txs, matched)
	require.Len(t, result, 2)
	assert.Contains(t, result, id1)
	assert.Contains(t, result, id3)
}

func TestCollectUnmatched_EmptyTxs(t *testing.T) {
	t.Parallel()

	result := collectUnmatched(nil, map[uuid.UUID]struct{}{})
	assert.Empty(t, result)
}

func TestMergeMatched_BothNilSafe(t *testing.T) {
	t.Parallel()

	mergeMatched(nil, nil) // Should not panic
}

func TestMergeMatched_MergesCorrectly(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000260070")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000260071")
	dest := map[uuid.UUID]struct{}{id1: {}}
	src := map[uuid.UUID]struct{}{id2: {}}

	mergeMatched(dest, src)
	assert.Len(t, dest, 2)
}

func TestMergeMatched_DuplicateKeysNoOp(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260080")
	dest := map[uuid.UUID]struct{}{id: {}}
	src := map[uuid.UUID]struct{}{id: {}}

	mergeMatched(dest, src)
	assert.Len(t, dest, 1)
}
