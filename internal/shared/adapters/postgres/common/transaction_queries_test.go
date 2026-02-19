//go:build unit

package common

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildMarkStatusQuery(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New()}

	query, err := BuildMarkStatusQuery(contextID, txIDs, "MATCHED")
	require.NoError(t, err)

	sqlStr, args, err := query.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sqlStr, "UPDATE transactions")
	assert.Contains(t, sqlStr, "SET")
	assert.Contains(t, args, "MATCHED")
}

func TestBuildMarkMatchedQuery(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	query, err := BuildMarkMatchedQuery(contextID, txIDs)
	require.NoError(t, err)

	sqlStr, args, err := query.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sqlStr, "UPDATE transactions")
	assert.Contains(t, sqlStr, "SET")
	assert.NotEmpty(t, args)
	assert.Contains(t, args, "MATCHED")
}

func TestBuildMarkPendingReviewQuery(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New()}

	query, err := BuildMarkPendingReviewQuery(contextID, txIDs)
	require.NoError(t, err)

	sqlStr, args, err := query.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sqlStr, "UPDATE transactions")
	assert.Contains(t, sqlStr, "SET")
	assert.NotEmpty(t, args)
	assert.Contains(t, args, "PENDING_REVIEW")
}

func TestBuildMarkUnmatchedQuery(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New()}

	query, err := BuildMarkUnmatchedQuery(contextID, txIDs)
	require.NoError(t, err)

	sqlStr, args, err := query.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sqlStr, "UPDATE transactions")
	assert.Contains(t, sqlStr, "SET")
	assert.NotEmpty(t, args)
	assert.Contains(t, args, "UNMATCHED")
}

func TestBuildMarkStatusQuery_EmptyIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	emptyIDs := []uuid.UUID{}

	_, err := BuildMarkMatchedQuery(contextID, emptyIDs)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransactionIDsEmpty)
}

func TestBuildMarkStatusQuery_NilIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	_, err := BuildMarkStatusQuery(contextID, nil, "MATCHED")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransactionIDsEmpty)
}

func TestBuildMarkPendingReviewQuery_EmptyIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	_, err := BuildMarkPendingReviewQuery(contextID, []uuid.UUID{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransactionIDsEmpty)
}

func TestBuildMarkUnmatchedQuery_EmptyIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	_, err := BuildMarkUnmatchedQuery(contextID, []uuid.UUID{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransactionIDsEmpty)
}

func TestBuildMarkStatusQuery_SingleID(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	singleID := []uuid.UUID{uuid.New()}

	query, err := BuildMarkMatchedQuery(contextID, singleID)
	require.NoError(t, err)

	sqlStr, args, err := query.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sqlStr, "UPDATE transactions")
	assert.Contains(t, sqlStr, "SET")
	assert.NotEmpty(t, args)
}
