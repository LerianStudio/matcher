//go:build unit

package match_run

import (
	"context"
	"encoding/base64"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestQuery = errors.New("query error")
	errTestExec  = errors.New("exec error")
)

func setupRepositoryWithMock(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

func createTestMatchRun(t *testing.T) *matchingEntities.MatchRun {
	t.Helper()

	ctx := context.Background()
	contextID := uuid.New()
	run, err := matchingEntities.NewMatchRun(ctx, contextID, value_objects.MatchRunModeCommit)
	require.NoError(t, err)

	return run
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with valid provider", func(t *testing.T) {
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

func TestCreate_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.Create(ctx, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreate_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.Create(ctx, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreate_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.Create(ctx, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchRunEntityNeeded)
}

func TestCreateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.CreateWithTx(ctx, nil, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCreateWithTx_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.CreateWithTx(ctx, nil, nil)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchRunEntityNeeded)
}

func TestCreateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.CreateWithTx(ctx, nil, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestUpdate_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.Update(ctx, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestUpdate_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.Update(ctx, run)

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
	require.ErrorIs(t, err, ErrMatchRunEntityNeeded)
}

func TestUpdateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.UpdateWithTx(ctx, nil, run)

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
	require.ErrorIs(t, err, ErrMatchRunEntityNeeded)
}

func TestUpdateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.UpdateWithTx(ctx, nil, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
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

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestListByContextID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()

	results, pagination, err := repo.ListByContextID(ctx, uuid.New(), matchingRepos.CursorFilter{})

	assert.Nil(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestListByContextID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	results, pagination, err := repo.ListByContextID(ctx, uuid.New(), matchingRepos.CursorFilter{})

	assert.Nil(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestListByContextID_InvalidCursor(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()

	invalidCursor := "not-valid-base64-!@#$%"

	results, pagination, err := repo.ListByContextID(ctx, uuid.New(), matchingRepos.CursorFilter{
		Cursor: invalidCursor,
		Limit:  10,
	})

	assert.Nil(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cursor")
}

func TestListByContextID_MalformedCursor(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()

	malformedCursor := base64.StdEncoding.EncodeToString([]byte("invalid-json"))

	results, pagination, err := repo.ListByContextID(ctx, uuid.New(), matchingRepos.CursorFilter{
		Cursor: malformedCursor,
		Limit:  10,
	})

	assert.Nil(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cursor")
}

func TestWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()

	err := repo.WithTx(ctx, func(_ matchingRepos.Tx) error {
		return nil
	})

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	err := repo.WithTx(ctx, func(_ matchingRepos.Tx) error {
		return nil
	})

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestWithTx_NilFunction(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.WithTx(ctx, nil)

	require.NoError(t, err)
}

func TestCreateWithTx_NilTxPassthrough(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.CreateWithTx(ctx, nil, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestUpdateWithTx_NilTxPassthrough(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()
	run := createTestMatchRun(t)

	result, err := repo.UpdateWithTx(ctx, nil, run)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestScan_InvalidData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid",
		uuid.New().String(),
		"COMMIT",
		"PROCESSING",
		time.Now().UTC(),
		nil,
		[]byte("{}"),
		nil,
		time.Now().UTC(),
		time.Now().UTC(),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	assert.True(t, sqlRows.Next())

	entity, err := scan(sqlRows)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse id")
}

func TestScan_InvalidContextID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(),
		"invalid-context-uuid",
		"COMMIT",
		"PROCESSING",
		time.Now().UTC(),
		nil,
		[]byte("{}"),
		nil,
		time.Now().UTC(),
		time.Now().UTC(),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	assert.True(t, sqlRows.Next())

	entity, err := scan(sqlRows)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse context id")
}

func TestScan_InvalidMode(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(),
		uuid.New().String(),
		"INVALID_MODE",
		"PROCESSING",
		time.Now().UTC(),
		nil,
		[]byte("{}"),
		nil,
		time.Now().UTC(),
		time.Now().UTC(),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	assert.True(t, sqlRows.Next())

	entity, err := scan(sqlRows)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse mode")
}

func TestScan_InvalidStatus(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(),
		uuid.New().String(),
		"COMMIT",
		"INVALID_STATUS",
		time.Now().UTC(),
		nil,
		[]byte("{}"),
		nil,
		time.Now().UTC(),
		time.Now().UTC(),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	assert.True(t, sqlRows.Next())

	entity, err := scan(sqlRows)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse status")
}

func TestScan_InvalidStats(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(),
		uuid.New().String(),
		"COMMIT",
		"PROCESSING",
		time.Now().UTC(),
		nil,
		[]byte("not-valid-json"),
		nil,
		time.Now().UTC(),
		time.Now().UTC(),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	assert.True(t, sqlRows.Next())

	entity, err := scan(sqlRows)

	assert.Nil(t, entity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal stats")
}

func TestScan_ValidData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	runID := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()
	completedAt := now.Add(time.Hour)
	failureReason := "test failure"

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		runID.String(),
		contextID.String(),
		"DRY_RUN",
		"COMPLETED",
		now,
		&completedAt,
		[]byte(`{"matched":10,"unmatched":5}`),
		&failureReason,
		now,
		now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	assert.True(t, sqlRows.Next())

	entity, err := scan(sqlRows)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, runID, entity.ID)
	assert.Equal(t, contextID, entity.ContextID)
	assert.Equal(t, value_objects.MatchRunModeDryRun, entity.Mode)
	assert.Equal(t, value_objects.MatchRunStatusCompleted, entity.Status)
	assert.Equal(t, 10, entity.Stats["matched"])
	assert.Equal(t, 5, entity.Stats["unmatched"])
	assert.NotNil(t, entity.FailureReason)
	assert.Equal(t, failureReason, *entity.FailureReason)
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	var _ matchingRepos.MatchRunRepository = repo

	assert.NotNil(t, repo)
}

func TestListByContextID_NilProvider_WithNegativeLimit(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	filter := matchingRepos.CursorFilter{
		Limit: -1,
	}

	result, pagination, err := repo.ListByContextID(ctx, uuid.New(), filter)

	assert.Nil(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestListByContextID_NilProvider_WithZeroLimit(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	filter := matchingRepos.CursorFilter{
		Limit: 0,
	}

	result, pagination, err := repo.ListByContextID(ctx, uuid.New(), filter)

	assert.Nil(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

var errMockDBScanError = errors.New("database scan error")

func TestScan_ScanError(t *testing.T) {
	t.Parallel()

	scanner := &mockScannerMatchRun{
		scanErr: errMockDBScanError,
	}

	entity, err := scan(scanner)

	assert.Nil(t, entity)
	require.ErrorIs(t, err, errMockDBScanError)
}

func TestScan_NilOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	scanner := &mockScannerMatchRun{
		values: []any{
			uuid.New().String(), // id
			uuid.New().String(), // context_id
			"COMMIT",            // mode
			"PROCESSING",        // status
			now,                 // started_at
			(*time.Time)(nil),   // completed_at - nil
			[]byte("{}"),        // stats
			(*string)(nil),      // failure_reason - nil
			now,                 // created_at
			now,                 // updated_at
		},
	}

	entity, err := scan(scanner)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Nil(t, entity.CompletedAt)
	assert.Nil(t, entity.FailureReason)
	assert.Equal(t, value_objects.MatchRunModeCommit, entity.Mode)
	assert.Equal(t, value_objects.MatchRunStatusProcessing, entity.Status)
}

func TestScan_WithAllOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	completedAt := now.Add(time.Hour)
	failureReason := "test failure reason"

	scanner := &mockScannerMatchRun{
		values: []any{
			uuid.New().String(),                   // id
			uuid.New().String(),                   // context_id
			"DRY_RUN",                             // mode
			"FAILED",                              // status
			now,                                   // started_at
			&completedAt,                          // completed_at
			[]byte(`{"matched":5,"unmatched":3}`), // stats
			&failureReason,                        // failure_reason
			now,                                   // created_at
			now,                                   // updated_at
		},
	}

	entity, err := scan(scanner)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.NotNil(t, entity.CompletedAt)
	assert.NotNil(t, entity.FailureReason)
	assert.Equal(t, failureReason, *entity.FailureReason)
	assert.Equal(t, value_objects.MatchRunModeDryRun, entity.Mode)
	assert.Equal(t, value_objects.MatchRunStatusFailed, entity.Status)
	assert.Equal(t, 5, entity.Stats["matched"])
	assert.Equal(t, 3, entity.Stats["unmatched"])
}

func TestCreate_WithCompletedRun(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	run := createTestMatchRun(t)
	now := time.Now().UTC()
	run.CompletedAt = &now
	run.Status = value_objects.MatchRunStatusCompleted

	result, err := repo.Create(ctx, run)

	assert.Nil(t, result)
	require.Error(t, err)
}

func TestCreate_WithFailedRun(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	run := createTestMatchRun(t)
	failureReason := "some failure"
	run.FailureReason = &failureReason
	run.Status = value_objects.MatchRunStatusFailed

	result, err := repo.Create(ctx, run)

	assert.Nil(t, result)
	require.Error(t, err)
}

func TestUpdate_WithCompletedRun(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	run := createTestMatchRun(t)
	now := time.Now().UTC()
	run.CompletedAt = &now
	run.Status = value_objects.MatchRunStatusCompleted
	run.Stats = map[string]int{"matched": 10, "unmatched": 2}

	result, err := repo.Update(ctx, run)

	assert.Nil(t, result)
	require.Error(t, err)
}

var errMockScanValueMismatch = errors.New("scan: value count mismatch")

type mockScannerMatchRun struct {
	values    []any
	scanErr   error
	callCount int
}

//nolint:gocyclo,cyclop // mock scanner needs to handle multiple types
func (scanner *mockScannerMatchRun) Scan(dest ...any) error {
	scanner.callCount++

	if scanner.scanErr != nil {
		return scanner.scanErr
	}

	if len(dest) != len(scanner.values) {
		return errMockScanValueMismatch
	}

	for i, val := range scanner.values {
		switch typedDest := dest[i].(type) {
		case *string:
			if v, ok := val.(string); ok {
				*typedDest = v
			}
		case *[]byte:
			if v, ok := val.([]byte); ok {
				*typedDest = v
			}
		case **string:
			if v, ok := val.(*string); ok {
				*typedDest = v
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

func TestNewPostgreSQLModel_Success(t *testing.T) {
	t.Parallel()

	run := createTestMatchRun(t)

	model, err := NewPostgreSQLModel(run)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, run.ID.String(), model.ID)
	assert.Equal(t, run.ContextID.String(), model.ContextID)
	assert.Equal(t, run.Mode.String(), model.Mode)
	assert.Equal(t, run.Status.String(), model.Status)
}

func TestNewPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewPostgreSQLModel(nil)

	assert.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchRunEntityNeeded)
}

func TestPostgreSQLModel_ToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *PostgreSQLModel

	entity, err := model.ToEntity()

	assert.Nil(t, entity)
	require.ErrorIs(t, err, ErrMatchRunModelNeeded)
}

func TestPostgreSQLModel_RoundTripWithStats(t *testing.T) {
	t.Parallel()

	run := createTestMatchRun(t)
	run.Stats = map[string]int{"matched": 15, "unmatched": 5}

	model, err := NewPostgreSQLModel(run)
	require.NoError(t, err)

	back, err := model.ToEntity()
	require.NoError(t, err)

	assert.Equal(t, run.ID, back.ID)
	assert.Equal(t, run.ContextID, back.ContextID)
	assert.Equal(t, run.Mode, back.Mode)
	assert.Equal(t, run.Status, back.Status)
	assert.Equal(t, run.Stats["matched"], back.Stats["matched"])
	assert.Equal(t, run.Stats["unmatched"], back.Stats["unmatched"])
}

func TestCreate_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	run := createTestMatchRun(t)
	now := time.Now().UTC()

	insertQuery := regexp.QuoteMeta(
		"INSERT INTO match_runs (id, context_id, mode, status, started_at, completed_at, stats, failure_reason, created_at, updated_at)",
	)
	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			run.ID.String(),
			run.ContextID.String(),
			run.Mode.String(),
			run.Status.String(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		run.ID.String(),
		run.ContextID.String(),
		run.Mode.String(),
		run.Status.String(),
		now,
		nil,
		[]byte("{}"),
		nil,
		now,
		now,
	)
	mock.ExpectQuery(selectQuery).
		WithArgs(run.ContextID.String(), run.ID.String()).
		WillReturnRows(rows)

	mock.ExpectCommit()

	result, err := repo.Create(context.Background(), run)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, run.ID, result.ID)
	assert.Equal(t, run.ContextID, result.ContextID)
}

func TestCreate_InsertError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	run := createTestMatchRun(t)

	insertQuery := regexp.QuoteMeta("INSERT INTO match_runs")

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			run.ID.String(),
			run.ContextID.String(),
			run.Mode.String(),
			run.Status.String(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	result, err := repo.Create(context.Background(), run)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert match run")
}

func TestCreate_SelectError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	run := createTestMatchRun(t)

	insertQuery := regexp.QuoteMeta("INSERT INTO match_runs")
	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			run.ID.String(),
			run.ContextID.String(),
			run.Mode.String(),
			run.Status.String(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(selectQuery).
		WithArgs(run.ContextID.String(), run.ID.String()).
		WillReturnError(errTestQuery)

	mock.ExpectRollback()

	result, err := repo.Create(context.Background(), run)

	assert.Nil(t, result)
	require.Error(t, err)
}

func TestCreateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	run := createTestMatchRun(t)
	now := time.Now().UTC()

	insertQuery := regexp.QuoteMeta("INSERT INTO match_runs")
	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectBegin()

	mock.ExpectExec(insertQuery).
		WithArgs(
			run.ID.String(),
			run.ContextID.String(),
			run.Mode.String(),
			run.Status.String(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		run.ID.String(),
		run.ContextID.String(),
		run.Mode.String(),
		run.Status.String(),
		now,
		nil,
		[]byte("{}"),
		nil,
		now,
		now,
	)
	mock.ExpectQuery(selectQuery).
		WithArgs(run.ContextID.String(), run.ID.String()).
		WillReturnRows(rows)

	mock.ExpectCommit()

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.CreateWithTx(context.Background(), tx, run)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, run.ID, result.ID)

	require.NoError(t, tx.Commit())
}

func TestUpdate_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	run := createTestMatchRun(t)
	run.Status = value_objects.MatchRunStatusCompleted
	now := time.Now().UTC()
	run.CompletedAt = &now
	run.Stats = map[string]int{"matched": 10, "unmatched": 2}

	updateQuery := regexp.QuoteMeta(
		"UPDATE match_runs SET status=$1, completed_at=$2, stats=$3, failure_reason=$4, updated_at=$5 WHERE context_id=$6 AND id=$7",
	)
	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			run.Status.String(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			run.ContextID.String(),
			run.ID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		run.ID.String(),
		run.ContextID.String(),
		run.Mode.String(),
		run.Status.String(),
		run.StartedAt,
		run.CompletedAt,
		[]byte(`{"matched":10,"unmatched":2}`),
		nil,
		now,
		now,
	)
	mock.ExpectQuery(selectQuery).
		WithArgs(run.ContextID.String(), run.ID.String()).
		WillReturnRows(rows)

	mock.ExpectCommit()

	result, err := repo.Update(context.Background(), run)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, run.ID, result.ID)
	assert.Equal(t, value_objects.MatchRunStatusCompleted, result.Status)
}

func TestUpdate_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	run := createTestMatchRun(t)
	run.Status = value_objects.MatchRunStatusCompleted
	now := time.Now().UTC()
	run.CompletedAt = &now

	updateQuery := regexp.QuoteMeta("UPDATE match_runs")

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			run.Status.String(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			run.ContextID.String(),
			run.ID.String(),
		).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	result, err := repo.Update(context.Background(), run)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update match run")
}

func TestUpdate_NoRowsAffected(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	run := createTestMatchRun(t)
	run.Status = value_objects.MatchRunStatusCompleted
	now := time.Now().UTC()
	run.CompletedAt = &now

	updateQuery := regexp.QuoteMeta("UPDATE match_runs")

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			run.Status.String(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			run.ContextID.String(),
			run.ID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	result, err := repo.Update(context.Background(), run)

	assert.Nil(t, result)
	require.Error(t, err)
}

func TestUpdateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	run := createTestMatchRun(t)
	run.Status = value_objects.MatchRunStatusCompleted
	now := time.Now().UTC()
	run.CompletedAt = &now

	updateQuery := regexp.QuoteMeta("UPDATE match_runs")
	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			run.Status.String(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			nil,
			sqlmock.AnyArg(),
			run.ContextID.String(),
			run.ID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		run.ID.String(),
		run.ContextID.String(),
		run.Mode.String(),
		run.Status.String(),
		run.StartedAt,
		run.CompletedAt,
		[]byte("{}"),
		nil,
		now,
		now,
	)
	mock.ExpectQuery(selectQuery).
		WithArgs(run.ContextID.String(), run.ID.String()).
		WillReturnRows(rows)

	mock.ExpectCommit()

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.UpdateWithTx(context.Background(), tx, run)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, run.ID, result.ID)

	require.NoError(t, tx.Commit())
}

func TestFindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()
	runID := uuid.New()
	now := time.Now().UTC()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		runID.String(),
		contextID.String(),
		"COMMIT",
		"PROCESSING",
		now,
		nil,
		[]byte("{}"),
		nil,
		now,
		now,
	)
	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), runID.String()).
		WillReturnRows(rows)

	result, err := repo.FindByID(context.Background(), contextID, runID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, runID, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, value_objects.MatchRunModeCommit, result.Mode)
	assert.Equal(t, value_objects.MatchRunStatusProcessing, result.Status)
}

func TestFindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()
	runID := uuid.New()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), runID.String()).
		WillReturnError(errTestQuery)

	result, err := repo.FindByID(context.Background(), contextID, runID)

	assert.Nil(t, result)
	require.Error(t, err)
}

func TestFindByID_WithCompletedRun(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()
	runID := uuid.New()
	now := time.Now().UTC()
	completedAt := now.Add(time.Hour)
	failureReason := "test failure"

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM match_runs WHERE context_id=$1 AND id=$2",
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		runID.String(),
		contextID.String(),
		"DRY_RUN",
		"FAILED",
		now,
		&completedAt,
		[]byte(`{"matched":5,"unmatched":3}`),
		&failureReason,
		now,
		now,
	)
	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), runID.String()).
		WillReturnRows(rows)

	result, err := repo.FindByID(context.Background(), contextID, runID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, runID, result.ID)
	assert.Equal(t, value_objects.MatchRunModeDryRun, result.Mode)
	assert.Equal(t, value_objects.MatchRunStatusFailed, result.Status)
	assert.NotNil(t, result.CompletedAt)
	assert.NotNil(t, result.FailureReason)
	assert.Equal(t, failureReason, *result.FailureReason)
	assert.Equal(t, 5, result.Stats["matched"])
	assert.Equal(t, 3, result.Stats["unmatched"])
}

func TestListByContextID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()
	runID1 := uuid.New()
	runID2 := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		runID1.String(),
		contextID.String(),
		"COMMIT",
		"COMPLETED",
		now,
		&now,
		[]byte("{}"),
		nil,
		now,
		now,
	).AddRow(
		runID2.String(),
		contextID.String(),
		"DRY_RUN",
		"PROCESSING",
		now,
		nil,
		[]byte("{}"),
		nil,
		now,
		now,
	)

	mock.ExpectQuery("SELECT .+ FROM match_runs").
		WithArgs(contextID.String()).
		WillReturnRows(rows)

	results, pagination, err := repo.ListByContextID(
		context.Background(),
		contextID,
		matchingRepos.CursorFilter{
			Limit: 20,
		},
	)

	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, runID1, results[0].ID)
	assert.Equal(t, runID2, results[1].ID)
	assert.Empty(t, pagination.Prev)
}

func TestListByContextID_EmptyResult(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	})

	mock.ExpectQuery("SELECT .+ FROM match_runs").
		WithArgs(contextID.String()).
		WillReturnRows(rows)

	results, pagination, err := repo.ListByContextID(
		context.Background(),
		contextID,
		matchingRepos.CursorFilter{
			Limit: 20,
		},
	)

	require.NoError(t, err)
	assert.Empty(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestListByContextID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()

	mock.ExpectQuery("SELECT .+ FROM match_runs").
		WithArgs(contextID.String()).
		WillReturnError(errTestQuery)

	results, pagination, err := repo.ListByContextID(
		context.Background(),
		contextID,
		matchingRepos.CursorFilter{
			Limit: 20,
		},
	)

	assert.Nil(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.Error(t, err)
}

func TestListByContextID_WithPagination(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()
	now := time.Now().UTC()

	runIDs := make([]uuid.UUID, 3)
	rowsData := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	})
	for i := 0; i < 3; i++ {
		runIDs[i] = uuid.New()
		rowsData = rowsData.AddRow(
			runIDs[i].String(),
			contextID.String(),
			"COMMIT",
			"COMPLETED",
			now,
			&now,
			[]byte("{}"),
			nil,
			now,
			now,
		)
	}

	mock.ExpectQuery("SELECT .+ FROM match_runs").
		WithArgs(contextID.String()).
		WillReturnRows(rowsData)

	results, pagination, err := repo.ListByContextID(
		context.Background(),
		contextID,
		matchingRepos.CursorFilter{
			Limit: 2,
		},
	)

	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.NotEmpty(t, pagination.Next)
}

func TestListByContextID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid",
		contextID.String(),
		"COMMIT",
		"COMPLETED",
		now,
		nil,
		[]byte("{}"),
		nil,
		now,
		now,
	)

	mock.ExpectQuery("SELECT .+ FROM match_runs").
		WithArgs(contextID.String()).
		WillReturnRows(rows)

	results, pagination, err := repo.ListByContextID(
		context.Background(),
		contextID,
		matchingRepos.CursorFilter{
			Limit: 20,
		},
	)

	assert.Nil(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
	require.Error(t, err)
}

func TestWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectCommit()

	err := repo.WithTx(context.Background(), func(tx matchingRepos.Tx) error {
		require.NotNil(t, tx)
		return nil
	})

	require.NoError(t, err)
}

func TestWithTx_CallbackError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	errCallback := errors.New("callback error")

	mock.ExpectBegin()
	mock.ExpectRollback()

	err := repo.WithTx(context.Background(), func(tx matchingRepos.Tx) error {
		return errCallback
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "callback error")
}

func TestListByContextID_WithValidCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	contextID := uuid.New()
	cursorID := uuid.New()
	runID := uuid.New()
	now := time.Now().UTC()

	cursorJSON := `{"id":"` + cursorID.String() + `","direction":"next"}`
	validCursor := base64.StdEncoding.EncodeToString([]byte(cursorJSON))

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "mode", "status", "started_at",
		"completed_at", "stats", "failure_reason", "created_at", "updated_at",
	}).AddRow(
		runID.String(),
		contextID.String(),
		"COMMIT",
		"COMPLETED",
		now,
		&now,
		[]byte("{}"),
		nil,
		now,
		now,
	)

	mock.ExpectQuery("SELECT .+ FROM match_runs").
		WillReturnRows(rows)

	results, _, err := repo.ListByContextID(
		context.Background(),
		contextID,
		matchingRepos.CursorFilter{
			Cursor: validCursor,
			Limit:  10,
		},
	)

	require.NoError(t, err)
	require.Len(t, results, 1)
}
