//go:build unit

package match_group

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestQuery      = errors.New("query error")
	errTestPrepare    = errors.New("prepare error")
	errTestExec       = errors.New("exec error")
	errTestConnection = errors.New("connection error")
)

func setupMockRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func createTestMatchGroup(t *testing.T) *matchingEntities.MatchGroup {
	t.Helper()

	ctx := context.Background()
	confidence, err := matchingVO.ParseConfidenceScore(80)
	require.NoError(t, err)

	item1, err := matchingEntities.NewMatchItem(
		ctx,
		uuid.New(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
	)
	require.NoError(t, err)

	item2, err := matchingEntities.NewMatchItem(
		ctx,
		uuid.New(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
	)
	require.NoError(t, err)

	group, err := matchingEntities.NewMatchGroup(
		ctx,
		uuid.New(),
		uuid.New(),
		uuid.New(),
		confidence,
		[]*matchingEntities.MatchItem{item1, item2},
	)
	require.NoError(t, err)

	return group
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("creates repository with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestCreateBatch_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.CreateBatch(ctx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreateBatch_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	result, err := repo.CreateBatch(ctx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreateBatch_EmptySlice(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.CreateBatch(ctx, nil)

	assert.Nil(t, result)
	require.NoError(t, err)
}

func TestCreateBatchWithTx_InvalidTxType(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	invalidTx := &mockInvalidTx{}

	result, err := repo.CreateBatchWithTx(ctx, invalidTx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestCreateBatchWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.CreateBatchWithTx(ctx, nil, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestListByRunID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()
	filter := matchingRepos.CursorFilter{Limit: 10}

	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	assert.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestListByRunID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()
	filter := matchingRepos.CursorFilter{Limit: 10}

	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	assert.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestFindByID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdate_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.Update(ctx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdate_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	result, err := repo.Update(ctx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdate_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.Update(ctx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchGroupEntityNeeded)
}

func TestUpdateWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.UpdateWithTx(ctx, nil, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	result, err := repo.UpdateWithTx(ctx, nil, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdateWithTx_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.UpdateWithTx(ctx, nil, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchGroupEntityNeeded)
}

func TestUpdateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.UpdateWithTx(ctx, nil, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchGroupEntityNeeded)
}

func TestUpdateWithTx_InvalidTxType(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	invalidTx := &mockInvalidTx{}

	result, err := repo.UpdateWithTx(ctx, invalidTx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchGroupEntityNeeded)
}

func TestNormalizeSortColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"id lowercase", "id", "id"},
		{"ID uppercase", "ID", "id"},
		{"created_at", "created_at", "created_at"},
		{"CREATED_AT uppercase", "CREATED_AT", "created_at"},
		{"status", "status", "status"},
		{"STATUS uppercase", "STATUS", "status"},
		{"unknown defaults to id", "unknown", "id"},
		{"empty defaults to id", "", "id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := normalizeSortColumn(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRepositoryImplementsInterface(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	var _ matchingRepos.MatchGroupRepository = repo
}

type mockInvalidTx struct{}

func (m *mockInvalidTx) Commit() error   { return nil }
func (m *mockInvalidTx) Rollback() error { return nil }

var (
	errScanValueCountMismatch = errors.New("scan: value count mismatch")
	errMockDatabaseScan       = errors.New("database scan error")
)

type mockScanner struct {
	values    []any
	scanErr   error
	callCount int
}

//nolint:gocyclo,cyclop // mock scanner needs to handle multiple types
func (scanner *mockScanner) Scan(dest ...any) error {
	scanner.callCount++

	if scanner.scanErr != nil {
		return scanner.scanErr
	}

	if len(dest) != len(scanner.values) {
		return errScanValueCountMismatch
	}

	for i, val := range scanner.values {
		switch typedDest := dest[i].(type) {
		case *string:
			if v, ok := val.(string); ok {
				*typedDest = v
			}
		case *int:
			if v, ok := val.(int); ok {
				*typedDest = v
			}
		case **string:
			if v, ok := val.(*string); ok {
				*typedDest = v
			} else if s, ok := val.(string); ok {
				*typedDest = &s
			} else if val == nil {
				*typedDest = nil
			}
		case **time.Time:
			if v, ok := val.(*time.Time); ok {
				*typedDest = v
			} else if val == nil {
				*typedDest = nil
			}
		case *time.Time:
			if v, ok := val.(time.Time); ok {
				*typedDest = v
			}
		}
	}

	return nil
}

func TestScan_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	confirmedAt := now.Add(time.Hour)
	rejectedReason := "test rejection"

	scanner := &mockScanner{
		values: []any{
			uuid.New().String(), // id
			uuid.New().String(), // context_id
			uuid.New().String(), // run_id
			uuid.New().String(), // rule_id
			75,                  // confidence
			"PROPOSED",          // status
			&rejectedReason,     // rejected_reason
			&confirmedAt,        // confirmed_at
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, 75, entity.Confidence.Value())
	assert.Equal(t, "PROPOSED", entity.Status.String())
	assert.NotNil(t, entity.RejectedReason)
	assert.Equal(t, rejectedReason, *entity.RejectedReason)
	assert.NotNil(t, entity.ConfirmedAt)
}

func TestScan_ScanError(t *testing.T) {
	t.Parallel()

	scanner := &mockScanner{
		scanErr: errMockDatabaseScan,
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.ErrorIs(t, err, errMockDatabaseScan)
}

func TestScan_InvalidID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScanner{
		values: []any{
			"invalid-uuid",      // id - invalid
			uuid.New().String(), // context_id
			uuid.New().String(), // run_id
			uuid.New().String(), // rule_id
			75,                  // confidence
			"PROPOSED",          // status
			(*string)(nil),      // rejected_reason
			(*time.Time)(nil),   // confirmed_at
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse id")
}

func TestScan_InvalidContextID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScanner{
		values: []any{
			uuid.New().String(),    // id
			"invalid-context-uuid", // context_id - invalid
			uuid.New().String(),    // run_id
			uuid.New().String(),    // rule_id
			75,                     // confidence
			"PROPOSED",             // status
			(*string)(nil),         // rejected_reason
			(*time.Time)(nil),      // confirmed_at
			now,                    // created_at
			now,                    // updated_at
		},
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse context id")
}

func TestScan_InvalidRunID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScanner{
		values: []any{
			uuid.New().String(), // id
			uuid.New().String(), // context_id
			"invalid-run-uuid",  // run_id - invalid
			uuid.New().String(), // rule_id
			75,                  // confidence
			"PROPOSED",          // status
			(*string)(nil),      // rejected_reason
			(*time.Time)(nil),   // confirmed_at
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse run id")
}

func TestScan_InvalidRuleID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScanner{
		values: []any{
			uuid.New().String(), // id
			uuid.New().String(), // context_id
			uuid.New().String(), // run_id
			"invalid-rule-uuid", // rule_id - invalid
			75,                  // confidence
			"PROPOSED",          // status
			(*string)(nil),      // rejected_reason
			(*time.Time)(nil),   // confirmed_at
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse rule id")
}

func TestScan_InvalidConfidence(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScanner{
		values: []any{
			uuid.New().String(), // id
			uuid.New().String(), // context_id
			uuid.New().String(), // run_id
			uuid.New().String(), // rule_id
			150,                 // confidence - invalid (out of range)
			"PROPOSED",          // status
			(*string)(nil),      // rejected_reason
			(*time.Time)(nil),   // confirmed_at
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse confidence")
}

func TestScan_InvalidStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScanner{
		values: []any{
			uuid.New().String(), // id
			uuid.New().String(), // context_id
			uuid.New().String(), // run_id
			uuid.New().String(), // rule_id
			75,                  // confidence
			"INVALID_STATUS",    // status - invalid
			(*string)(nil),      // rejected_reason
			(*time.Time)(nil),   // confirmed_at
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse status")
}

func TestScan_NilOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScanner{
		values: []any{
			uuid.New().String(), // id
			uuid.New().String(), // context_id
			uuid.New().String(), // run_id
			uuid.New().String(), // rule_id
			75,                  // confidence
			"CONFIRMED",         // status
			(*string)(nil),      // rejected_reason - nil
			(*time.Time)(nil),   // confirmed_at - nil
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Nil(t, entity.RejectedReason)
	assert.Nil(t, entity.ConfirmedAt)
	assert.Equal(t, "CONFIRMED", entity.Status.String())
}

func TestListByRunID_CursorWithNonIDSort(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	filter := matchingRepos.CursorFilter{
		Limit:     10,
		Cursor:    "some-cursor",
		SortBy:    "created_at",
		SortOrder: "desc",
	}

	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	assert.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
	require.Error(t, err)
}

func TestCalculateMatchGroupSortPagination_PropagatesCalculatorError(t *testing.T) {
	t.Parallel()

	_, err := calculateMatchGroupSortPagination(
		true,
		true,
		true,
		sortColumnCreatedAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		uuid.New().String(),
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		uuid.New().String(),
		func(
			_ bool,
			_ bool,
			_ bool,
			_ string,
			_ string,
			_ string,
			_ string,
			_ string,
		) (string, string, error) {
			return "", "", errTestQuery
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, errTestQuery)
}

func TestCalculateMatchGroupSortPagination_NilCalculator(t *testing.T) {
	t.Parallel()

	_, err := calculateMatchGroupSortPagination(
		true,
		true,
		true,
		sortColumnCreatedAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		uuid.New().String(),
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		uuid.New().String(),
		nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, pgcommon.ErrSortCursorCalculatorRequired)
}

func TestMatchGroupSortValue_NilGroup(t *testing.T) {
	t.Parallel()

	assert.Empty(t, matchGroupSortValue(nil, sortColumnCreatedAt))
}

func TestListByRunID_DefaultLimit(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	filter := matchingRepos.CursorFilter{
		Limit: -1,
	}

	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	assert.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestListByRunID_ZeroLimit(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	filter := matchingRepos.CursorFilter{
		Limit: 0,
	}

	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	assert.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreateBatch_PrepareError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO match_groups").WillReturnError(errTestPrepare)
	mock.ExpectRollback()

	result, err := repo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "prepare insert match group")
}

func TestCreateBatch_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO match_groups").
		ExpectExec().
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	result, err := repo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "insert match group")
}

func TestListByRunID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectQuery("SELECT").WillReturnError(errTestQuery)

	filter := matchingRepos.CursorFilter{Limit: 10}
	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestListByRunID_ReturnsRowsThroughBoundaryValidation(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	groupID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()
	ruleStr := ruleID.String()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "run_id", "rule_id", "confidence", "status",
		"rejected_reason", "confirmed_at", "created_at", "updated_at",
	}).AddRow(
		groupID.String(), contextID.String(), runID.String(), &ruleStr,
		80, "CONFIRMED", nil, &now, now, now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	results, pagination, err := repo.ListByRunID(ctx, contextID, runID, matchingRepos.CursorFilter{Limit: 10})

	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, groupID, results[0].ID)
	assert.Equal(t, contextID, results[0].ContextID)
	assert.Equal(t, runID, results[0].RunID)
	assert.Empty(t, pagination.Prev, "first page should have no prev cursor")
}

func TestListByRunID_LimitCappedAtMaximum(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"})

	mock.ExpectQuery(fmt.Sprintf("LIMIT %d", constants.MaximumPaginationLimit+1)).WillReturnRows(rows)

	filter := matchingRepos.CursorFilter{Limit: constants.MaximumPaginationLimit + 1}
	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	require.NoError(t, err)
	assert.Empty(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestFindByID_SuccessWithMock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	groupID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow(groupID.String(), contextID.String(), runID.String(), ruleID.String(), 75, "CONFIRMED", nil, &now, now, now)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, err := repo.FindByID(ctx, contextID, groupID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, groupID, result.ID)
	assert.Equal(t, "CONFIRMED", result.Status.String())
}

func TestFindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}))

	result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestFindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectQuery("SELECT").WillReturnError(errTestQuery)

	result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
}

func TestPostgreSQLModel_ToEntity_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &PostgreSQLModel{
		ID:         uuid.New().String(),
		ContextID:  uuid.New().String(),
		RunID:      uuid.New().String(),
		RuleID:     ptrStr(uuid.New().String()),
		Confidence: 85,
		Status:     "PROPOSED",
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, 85, entity.Confidence.Value())
	assert.Equal(t, "PROPOSED", entity.Status.String())
}

func TestPostgreSQLModel_ToEntity_WithOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	reason := "rejected for testing"
	model := &PostgreSQLModel{
		ID:             uuid.New().String(),
		ContextID:      uuid.New().String(),
		RunID:          uuid.New().String(),
		RuleID:         ptrStr(uuid.New().String()),
		Confidence:     50,
		Status:         "REJECTED",
		RejectedReason: &reason,
		ConfirmedAt:    &now,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, "REJECTED", entity.Status.String())
	require.NotNil(t, entity.RejectedReason)
	assert.Equal(t, reason, *entity.RejectedReason)
	require.NotNil(t, entity.ConfirmedAt)
}

func TestNewPostgreSQLModel_SuccessWithMock(t *testing.T) {
	t.Parallel()

	group := createTestMatchGroup(t)

	model, err := NewPostgreSQLModel(group)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, group.ID.String(), model.ID)
	assert.Equal(t, group.ContextID.String(), model.ContextID)
	assert.Equal(t, group.RunID.String(), model.RunID)
	require.NotNil(t, model.RuleID)
	assert.Equal(t, group.RuleID.String(), *model.RuleID)
	assert.Equal(t, group.Confidence.Value(), model.Confidence)
}

func TestListByRunID_EmptyResult(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}),
	)

	filter := matchingRepos.CursorFilter{Limit: 10}
	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 0)
	assert.Empty(t, pagination.Next)
}

func TestUpdate_SuccessWithMock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow(group.ID.String(), group.ContextID.String(), group.RunID.String(), group.RuleID.String(), group.Confidence.Value(), group.Status.String(), nil, nil, group.CreatedAt, group.UpdatedAt)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.Update(ctx, group)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, group.ID, result.ID)
}

func TestUpdate_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	result, err := repo.Update(ctx, group)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "update match group")
}

func TestUpdate_NoRowsAffected(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, group)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestUpdate_RowsAffectedError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewErrorResult(errTestExec))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, group)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "rows affected")
}

func TestUpdate_SelectAfterUpdateError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT").WillReturnError(errTestQuery)
	mock.ExpectRollback()

	result, err := repo.Update(ctx, group)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestUpdateWithTx_SuccessWithMock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow(group.ID.String(), group.ContextID.String(), group.RunID.String(), group.RuleID.String(), group.Confidence.Value(), group.Status.String(), nil, nil, group.CreatedAt, group.UpdatedAt)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, err := repo.UpdateWithTx(ctx, tx, group)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, group.ID, result.ID)
}

func TestUpdateWithTx_ExecError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnError(errTestExec)

	result, err := repo.UpdateWithTx(ctx, tx, group)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "update match group")
}

func TestUpdateWithTx_NoRowsAffected(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	group := createTestMatchGroup(t)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("UPDATE match_groups").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))

	result, err := repo.UpdateWithTx(ctx, tx, group)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestUpdateWithTx_WithValidEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()
	group := createTestMatchGroup(t)

	result, err := repo.UpdateWithTx(ctx, nil, group)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestUpdateWithTx_InvalidTxWithValidEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()
	group := createTestMatchGroup(t)

	invalidTx := &mockInvalidTx{}

	result, err := repo.UpdateWithTx(ctx, invalidTx, group)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestListByRunID_SuccessWithResults(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	groupID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow(groupID.String(), contextID.String(), runID.String(), ruleID.String(), 75, "PROPOSED", nil, nil, now, now)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	filter := matchingRepos.CursorFilter{Limit: 10}
	result, pagination, err := repo.ListByRunID(ctx, contextID, runID, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 1)
	assert.Equal(t, groupID, result[0].ID)
	assert.Empty(t, pagination.Next)
}

func TestListByRunID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow("invalid-uuid", uuid.New().String(), uuid.New().String(), uuid.New().String(), 75, "PROPOSED", nil, nil, time.Now(), time.Now())

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	filter := matchingRepos.CursorFilter{Limit: 10}
	result, pagination, err := repo.ListByRunID(ctx, uuid.New(), uuid.New(), filter)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestListByRunID_RowsIterationError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	groupID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow(groupID.String(), contextID.String(), runID.String(), ruleID.String(), 75, "PROPOSED", nil, nil, now, now).
		RowError(0, errTestQuery)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	filter := matchingRepos.CursorFilter{Limit: 10}
	result, pagination, err := repo.ListByRunID(ctx, contextID, runID, filter)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestListByRunID_SortByStatus(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	groupID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow(groupID.String(), contextID.String(), runID.String(), ruleID.String(), 75, "PROPOSED", nil, nil, now, now)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	filter := matchingRepos.CursorFilter{Limit: 10, SortBy: "status", SortOrder: "asc"}
	result, pagination, err := repo.ListByRunID(ctx, contextID, runID, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 1)
	assert.Empty(t, pagination.Next)
}

func TestListByRunID_SortByCreatedAt(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	groupID := uuid.New()
	ruleID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow(groupID.String(), contextID.String(), runID.String(), ruleID.String(), 75, "PROPOSED", nil, nil, now, now)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	filter := matchingRepos.CursorFilter{Limit: 10, SortBy: "created_at", SortOrder: "desc"}
	result, pagination, err := repo.ListByRunID(ctx, contextID, runID, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 1)
	assert.Empty(t, pagination.Next)
}

func TestCreateBatch_AllNilGroups(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()
	groups := []*matchingEntities.MatchGroup{nil, nil, nil}

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO match_groups")
	mock.ExpectCommit()

	result, err := repo.CreateBatch(ctx, groups)

	require.NoError(t, err)
	require.Empty(t, result)
}

func TestCreateBatchWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.CreateBatchWithTx(ctx, nil, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestFindByID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := context.Background()

	rows := sqlmock.NewRows([]string{"id", "context_id", "run_id", "rule_id", "confidence", "status", "rejected_reason", "confirmed_at", "created_at", "updated_at"}).
		AddRow("invalid-uuid", uuid.New().String(), uuid.New().String(), uuid.New().String(), 75, "PROPOSED", nil, nil, time.Now(), time.Now())

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse id")
}
