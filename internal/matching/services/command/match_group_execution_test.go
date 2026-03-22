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

func TestIndexTransactions_Empty(t *testing.T) {
	t.Parallel()

	result := indexTransactions(nil)
	assert.Empty(t, result)
}

func TestIndexTransactions_SkipsNil(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260001")
	txs := []*shared.Transaction{nil, {ID: id}, nil}

	result := indexTransactions(txs)
	require.Len(t, result, 1)
	assert.NotNil(t, result[id])
}

func TestIndexTransactions_MultipleTx(t *testing.T) {
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

func TestMergeTransactionMaps_Empty(t *testing.T) {
	t.Parallel()

	result := mergeTransactionMaps()
	assert.Empty(t, result)
}

func TestMergeTransactionMaps_SingleMap(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260020")
	tx := &shared.Transaction{ID: id}

	result := mergeTransactionMaps(map[uuid.UUID]*shared.Transaction{id: tx})
	assert.Len(t, result, 1)
	assert.Equal(t, tx, result[id])
}

func TestMergeTransactionMaps_MultipleMaps(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000260030")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000260031")
	tx1 := &shared.Transaction{ID: id1}
	tx2 := &shared.Transaction{ID: id2}

	m1 := map[uuid.UUID]*shared.Transaction{id1: tx1}
	m2 := map[uuid.UUID]*shared.Transaction{id2: tx2}

	result := mergeTransactionMaps(m1, m2)
	assert.Len(t, result, 2)
}

func TestMergeTransactionMaps_OverlappingKeys(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260040")
	tx1 := &shared.Transaction{ID: id, Amount: decimal.NewFromInt(100)}
	tx2 := &shared.Transaction{ID: id, Amount: decimal.NewFromInt(200)}

	m1 := map[uuid.UUID]*shared.Transaction{id: tx1}
	m2 := map[uuid.UUID]*shared.Transaction{id: tx2}

	result := mergeTransactionMaps(m1, m2)
	assert.Len(t, result, 1)
	// Last write wins
	assert.True(t, result[id].Amount.Equal(decimal.NewFromInt(200)))
}

func TestCollectUnmatched_AllMatched(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260050")
	txs := []*shared.Transaction{{ID: id}}
	matched := map[uuid.UUID]struct{}{id: {}}

	result := collectUnmatched(txs, matched)
	assert.Empty(t, result)
}

func TestCollectUnmatched_NoneMatched(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000260051")
	txs := []*shared.Transaction{{ID: id}}

	result := collectUnmatched(txs, map[uuid.UUID]struct{}{})
	require.Len(t, result, 1)
	assert.Equal(t, id, result[0])
}

func TestCollectUnmatched_SkipsNilTx(t *testing.T) {
	t.Parallel()

	txs := []*shared.Transaction{nil, nil}
	result := collectUnmatched(txs, map[uuid.UUID]struct{}{})
	assert.Empty(t, result)
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

func TestMergeMatched_NilDest(t *testing.T) {
	t.Parallel()

	src := map[uuid.UUID]struct{}{uuid.New(): {}}
	mergeMatched(nil, src) // Should not panic
}

func TestMergeMatched_NilSrc(t *testing.T) {
	t.Parallel()

	dest := map[uuid.UUID]struct{}{}
	mergeMatched(dest, nil) // Should not panic
	assert.Empty(t, dest)
}

func TestMergeMatched_BothNil(t *testing.T) {
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
	_, ok1 := dest[id1]
	assert.True(t, ok1)
	_, ok2 := dest[id2]
	assert.True(t, ok2)
}
