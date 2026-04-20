//go:build unit

package match_rule

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestSafeUint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int
		expected uint64
	}{
		{"zero", 0, 0},
		{"positive", 10, 10},
		{"large positive", 1000000, 1000000},
		{"negative returns zero", -1, 0},
		{"large negative returns zero", -999999, 0},
		{"one", 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := safeUint64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPaginateAndCalculateCursor_EmptyRules(t *testing.T) {
	t.Parallel()

	rules := entities.MatchRules{}
	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	result, pagination, err := paginateAndCalculateCursor("", decodedCursor, rules, 10)

	require.NoError(t, err)
	assert.Empty(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestPaginateAndCalculateCursor_FirstPageWithResults(t *testing.T) {
	t.Parallel()

	rules := make(entities.MatchRules, 0, 3)
	for i := 0; i < 3; i++ {
		rules = append(rules, &entities.MatchRule{
			ID:       uuid.New(),
			Priority: i + 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{},
		})
	}

	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	result, pagination, err := paginateAndCalculateCursor("", decodedCursor, rules, 10)

	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestPaginateAndCalculateCursor_HasMorePages(t *testing.T) {
	t.Parallel()

	// When len(rules) > limit, there are more pages
	rules := make(entities.MatchRules, 0, 4)
	for i := 0; i < 4; i++ {
		rules = append(rules, &entities.MatchRule{
			ID:       uuid.New(),
			Priority: i + 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{},
		})
	}

	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	result, pagination, err := paginateAndCalculateCursor("", decodedCursor, rules, 3)

	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.NotEmpty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestPaginateAndCalculateCursor_BackwardsCursor(t *testing.T) {
	t.Parallel()

	rules := make(entities.MatchRules, 0, 3)
	for i := 0; i < 3; i++ {
		rules = append(rules, &entities.MatchRule{
			ID:       uuid.New(),
			Priority: i + 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{},
		})
	}

	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionPrev}

	result, pagination, err := paginateAndCalculateCursor("some-cursor", decodedCursor, rules, 2)

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.True(t, pagination.Next != "" || pagination.Prev != "")
}

func TestPaginateAndCalculateCursor_ExactLimitCount(t *testing.T) {
	t.Parallel()

	rules := make(entities.MatchRules, 0, 5)
	for i := 0; i < 5; i++ {
		rules = append(rules, &entities.MatchRule{
			ID:       uuid.New(),
			Priority: i + 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{},
		})
	}

	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	result, _, err := paginateAndCalculateCursor("", decodedCursor, rules, 5)

	require.NoError(t, err)
	assert.Len(t, result, 5)
}

func TestRepository_FindByPriority_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByPriority(ctx, uuid.New(), 1)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindByPriority(ctx, uuid.New(), 1)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestBuildCursorConditions_DirectionNext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := newSqlMockDB()
	require.NoError(t, err)

	defer db.Close()

	cursorID := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()

	rows := newPriorityRows(5)
	mock.ExpectQuery("SELECT priority FROM match_rules").
		WithArgs(cursorID.String(), contextID.String()).
		WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext, ID: cursorID.String()}

	cond, orderDirection, err := buildCursorConditions(ctx, tx, decodedCursor, cursorID, contextID)

	require.NoError(t, err)
	require.NotNil(t, cond)
	assert.Equal(t, "ASC", orderDirection)
}

func TestBuildCursorConditions_DirectionPrev(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := newSqlMockDB()
	require.NoError(t, err)

	defer db.Close()

	cursorID := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()

	rows := newPriorityRows(3)
	mock.ExpectQuery("SELECT priority FROM match_rules").
		WithArgs(cursorID.String(), contextID.String()).
		WillReturnRows(rows)

	tx, err := db.Begin()
	require.NoError(t, err)

	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionPrev, ID: cursorID.String()}

	cond, orderDirection, err := buildCursorConditions(ctx, tx, decodedCursor, cursorID, contextID)

	require.NoError(t, err)
	require.NotNil(t, cond)
	assert.Equal(t, "DESC", orderDirection)
}

func TestBuildCursorConditions_CursorNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := newSqlMockDB()
	require.NoError(t, err)

	defer db.Close()

	cursorID := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT priority FROM match_rules").
		WithArgs(cursorID.String(), contextID.String()).
		WillReturnError(ErrCursorNotFound)

	tx, err := db.Begin()
	require.NoError(t, err)

	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext, ID: cursorID.String()}

	_, _, err = buildCursorConditions(ctx, tx, decodedCursor, cursorID, contextID)

	require.Error(t, err)
}
