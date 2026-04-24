// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package exception

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

func TestRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	missingID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	mock.ExpectQuery(query).WithArgs(missingID.String()).WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(ctx, missingID)
	require.ErrorIs(t, err, entities.ErrExceptionNotFound)
}

func TestRepository_FindByID_NullableFields(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()
	transactionID := uuid.New()
	createdAt := time.Now().UTC().Add(-time.Hour)
	updatedAt := time.Now().UTC()

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	rows := sqlmock.NewRows([]string{
		"id",
		"transaction_id",
		"severity",
		"status",
		"external_system",
		"external_issue_id",
		"assigned_to",
		"due_at",
		"resolution_notes",
		"resolution_type",
		"resolution_reason",
		"reason",
		"version",
		"created_at",
		"updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		createdAt,
		updatedAt,
	)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, exceptionID)
	require.NoError(t, err)
	require.Equal(t, exceptionID, result.ID)
	require.Equal(t, transactionID, result.TransactionID)
	require.Nil(t, result.ExternalSystem)
	require.Nil(t, result.ExternalIssueID)
	require.Nil(t, result.AssignedTo)
	require.Nil(t, result.DueAt)
	require.Nil(t, result.ResolutionNotes)
	require.Nil(t, result.Reason)
}

func TestRepository_FindByIDs_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	transactionID := uuid.New()
	createdAt := time.Now().UTC().Add(-time.Hour)
	updatedAt := time.Now().UTC()

	// FindByIDs builds `SELECT ... FROM exceptions WHERE id IN ($1,$2,$3)`
	// via squirrel. The regex tolerates squirrel's exact whitespace.
	query := `SELECT id, transaction_id, severity, status, external_system, external_issue_id, assigned_to, due_at, resolution_notes, resolution_type, resolution_reason, reason, version, created_at, updated_at FROM exceptions WHERE id IN \(\$1,\$2,\$3\)`

	rows := sqlmock.NewRows([]string{
		"id",
		"transaction_id",
		"severity",
		"status",
		"external_system",
		"external_issue_id",
		"assigned_to",
		"due_at",
		"resolution_notes",
		"resolution_type",
		"resolution_reason",
		"reason",
		"version",
		"created_at",
		"updated_at",
	})

	for _, id := range ids {
		rows.AddRow(
			id.String(),
			transactionID.String(),
			"HIGH",
			"OPEN",
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			sql.NullTime{},
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			int64(1),
			createdAt,
			updatedAt,
		)
	}

	expectedArgs := []driver.Value{ids[0].String(), ids[1].String(), ids[2].String()}

	mock.ExpectQuery(query).
		WithArgs(expectedArgs...).
		WillReturnRows(rows)

	result, err := repo.FindByIDs(ctx, ids)
	require.NoError(t, err)
	require.Len(t, result, len(ids))

	// The slice ordering mirrors the rows mock returned them in.
	for i, id := range ids {
		assert.Equal(t, id, result[i].ID)
	}
}

func TestRepository_FindByIDs_EmptyInput(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	result, err := repo.FindByIDs(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, result)

	result, err = repo.FindByIDs(ctx, []uuid.UUID{})
	require.NoError(t, err)
	assert.Empty(t, result)
	// no sqlmock expectations -- empty input short-circuits before any DB call.
}

func TestRepository_Update_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	query := regexp.QuoteMeta(`
			UPDATE exceptions SET
				severity = $2,
				status = $3,
				external_system = $4,
				external_issue_id = $5,
				assigned_to = $6,
				due_at = $7,
				resolution_notes = $8,
				resolution_type = $9,
				resolution_reason = $10,
				reason = $11,
				version = version + 1,
				updated_at = $12
			WHERE id = $1 AND version = $13
		`)

	existsQuery := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)`)

	mock.ExpectBegin()
	mock.ExpectExec(query).
		WithArgs(
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(existsQuery).
		WithArgs(exception.ID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectRollback()

	_, err = repo.Update(ctx, exception)
	require.ErrorIs(t, err, entities.ErrExceptionNotFound)
}

func TestRepository_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("FindByID", func(t *testing.T) {
		t.Parallel()

		_, err := repo.FindByID(ctx, uuid.New())
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("FindByIDs", func(t *testing.T) {
		t.Parallel()

		_, err := repo.FindByIDs(ctx, []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("List", func(t *testing.T) {
		t.Parallel()

		_, _, err := repo.List(
			ctx,
			repositories.ExceptionFilter{},
			repositories.CursorFilter{Limit: 10},
		)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("Update", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			sharedexception.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		_, err = repo.Update(ctx, exception)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("UpdateWithTx", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			sharedexception.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		_, err = repo.UpdateWithTx(ctx, nil, exception)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("ExistsForTenant", func(t *testing.T) {
		t.Parallel()

		_, err := repo.ExistsForTenant(ctx, uuid.New())
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_NilEntity(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	t.Run("Update nil entity", func(t *testing.T) {
		t.Parallel()

		_, err := repo.Update(ctx, nil)
		require.ErrorIs(t, err, entities.ErrExceptionNil)
	})
}

func TestRepository_UpdateWithTx_NilEntity(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = repo.UpdateWithTx(ctx, tx, nil)
	require.ErrorIs(t, err, entities.ErrExceptionNil)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_UpdateWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	_, err = repo.UpdateWithTx(ctx, nil, exception)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_SentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
		{"ErrExceptionNotFound", entities.ErrExceptionNotFound},
		{"ErrConcurrentModification", ErrConcurrentModification},
		{"ErrTransactionRequired", ErrTransactionRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestNormalizeSortColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"", "id"},
		{"id", "id"},
		{"ID", "id"},
		{"created_at", "created_at"},
		{"CREATED_AT", "created_at"},
		{"updated_at", "updated_at"},
		{"severity", "severity"},
		{"SEVERITY", "severity"},
		{"status", "status"},
		{"STATUS", "status"},
		{"unknown", "id"},
		{"invalid_column", "id"},
		{"  id  ", "id"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			result := normalizeSortColumn(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestPrepareListParams(t *testing.T) {
	t.Parallel()

	t.Run("default values", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{})

		assert.Equal(t, constants.DefaultPaginationLimit, params.limit)
		assert.Equal(t, "id", params.sortColumn)
		assert.True(t, params.useIDCursor)
	})

	t.Run("custom limit", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{Limit: 50})

		assert.Equal(t, 50, params.limit)
	})

	t.Run("negative limit uses default", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{Limit: -5})

		assert.Equal(t, constants.DefaultPaginationLimit, params.limit)
	})

	t.Run("limit above maximum is capped", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{Limit: constants.MaximumPaginationLimit + 1})

		assert.Equal(t, constants.MaximumPaginationLimit, params.limit)
	})

	t.Run("sort by non-id column", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{SortBy: "created_at", Limit: 10})

		assert.Equal(t, "created_at", params.sortColumn)
		assert.False(t, params.useIDCursor)
	})

	t.Run("cursor with non-id sort stores raw cursor", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{
			SortBy: "created_at",
			Cursor: "not-valid-base64!!!",
			Limit:  10,
		})

		assert.Equal(t, "not-valid-base64!!!", params.cursorStr)
		assert.False(t, params.useIDCursor)
	})

	t.Run("sort order ASC", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{SortOrder: "asc", Limit: 10})

		assert.Equal(t, "ASC", params.orderDirection)
	})

	t.Run("sort order DESC", func(t *testing.T) {
		t.Parallel()

		params := prepareListParams(repositories.CursorFilter{SortOrder: "desc", Limit: 10})

		assert.Equal(t, "DESC", params.orderDirection)
	})
}

func TestBuildListQuery(t *testing.T) {
	t.Parallel()

	t.Run("basic query without filters", func(t *testing.T) {
		t.Parallel()

		params := listQueryParams{
			limit:          10,
			sortColumn:     "created_at",
			orderDirection: "DESC",
			useIDCursor:    false,
		}

		query, args, _, err := buildListQuery(repositories.ExceptionFilter{}, params)

		require.NoError(t, err)
		require.NotEmpty(t, query)
		require.Contains(t, query, "SELECT")
		require.Contains(t, query, "FROM exceptions")
		require.Contains(t, query, "ORDER BY")
		require.Contains(t, query, "LIMIT 11")
		require.Empty(t, args)
	})

	t.Run("query with status filter", func(t *testing.T) {
		t.Parallel()

		status := value_objects.ExceptionStatusOpen
		filter := repositories.ExceptionFilter{Status: &status}
		params := listQueryParams{
			limit:          20,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    false,
		}

		query, args, _, err := buildListQuery(filter, params)

		require.NoError(t, err)
		require.Contains(t, query, "status")
		require.Contains(t, args, "OPEN")
	})

	t.Run("query with severity filter", func(t *testing.T) {
		t.Parallel()

		severity := sharedexception.ExceptionSeverityHigh
		filter := repositories.ExceptionFilter{Severity: &severity}
		params := listQueryParams{
			limit:          20,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    false,
		}

		query, args, _, err := buildListQuery(filter, params)

		require.NoError(t, err)
		require.Contains(t, query, "severity")
		require.Contains(t, args, "HIGH")
	})

	t.Run("query with assigned_to filter", func(t *testing.T) {
		t.Parallel()

		assignedTo := "user@example.com"
		filter := repositories.ExceptionFilter{AssignedTo: &assignedTo}
		params := listQueryParams{
			limit:          20,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    false,
		}

		query, args, _, err := buildListQuery(filter, params)

		require.NoError(t, err)
		require.Contains(t, query, "assigned_to")
		require.Contains(t, args, "user@example.com")
	})

	t.Run("query with external_system filter", func(t *testing.T) {
		t.Parallel()

		externalSystem := "JIRA"
		filter := repositories.ExceptionFilter{ExternalSystem: &externalSystem}
		params := listQueryParams{
			limit:          20,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    false,
		}

		query, args, _, err := buildListQuery(filter, params)

		require.NoError(t, err)
		require.Contains(t, query, "external_system")
		require.Contains(t, args, "JIRA")
	})

	t.Run("query with date filters", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		dateFrom := now.Add(-7 * 24 * time.Hour)
		dateTo := now
		filter := repositories.ExceptionFilter{
			DateFrom: &dateFrom,
			DateTo:   &dateTo,
		}
		params := listQueryParams{
			limit:          20,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    false,
		}

		query, args, _, err := buildListQuery(filter, params)

		require.NoError(t, err)
		require.Contains(t, query, "created_at >=")
		require.Contains(t, query, "created_at <=")
		require.Len(t, args, 2)
	})

	t.Run("query with all filters", func(t *testing.T) {
		t.Parallel()

		status := value_objects.ExceptionStatusAssigned
		severity := sharedexception.ExceptionSeverityCritical
		assignedTo := "admin@example.com"
		externalSystem := "ServiceNow"
		now := time.Now().UTC()
		dateFrom := now.Add(-30 * 24 * time.Hour)
		dateTo := now

		filter := repositories.ExceptionFilter{
			Status:         &status,
			Severity:       &severity,
			AssignedTo:     &assignedTo,
			ExternalSystem: &externalSystem,
			DateFrom:       &dateFrom,
			DateTo:         &dateTo,
		}
		params := listQueryParams{
			limit:          50,
			sortColumn:     "severity",
			orderDirection: "DESC",
			useIDCursor:    false,
		}

		query, args, _, err := buildListQuery(filter, params)

		require.NoError(t, err)
		require.NotEmpty(t, query)
		require.Len(t, args, 6)
	})
}

func TestCalculatePagination(t *testing.T) {
	t.Parallel()

	t.Run("empty exceptions returns empty pagination", func(t *testing.T) {
		t.Parallel()

		params := listQueryParams{
			limit:          10,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    true,
		}

		pagination, err := calculatePagination(nil, true, false, params, pkgHTTP.CursorDirectionNext)
		require.NoError(t, err)

		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})

	t.Run("single exception", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			sharedexception.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)

		exceptions := []*entities.Exception{exception}
		params := listQueryParams{
			limit:          10,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    true,
		}

		pagination, err := calculatePagination(exceptions, true, false, params, pkgHTTP.CursorDirectionNext)
		require.NoError(t, err)

		require.NotNil(t, pagination)
	})

	t.Run("multiple exceptions", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		exception1, err := entities.NewException(
			ctx,
			uuid.New(),
			sharedexception.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)

		exception2, err := entities.NewException(
			ctx,
			uuid.New(),
			sharedexception.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		exceptions := []*entities.Exception{exception1, exception2}
		params := listQueryParams{
			limit:          10,
			sortColumn:     "id",
			orderDirection: "ASC",
			useIDCursor:    true,
		}

		pagination, err := calculatePagination(exceptions, true, true, params, pkgHTTP.CursorDirectionNext)
		require.NoError(t, err)

		require.NotNil(t, pagination)
	})
}

func TestCalculateExceptionSortPagination_PropagatesCalculatorError(t *testing.T) {
	t.Parallel()

	_, err := calculateExceptionSortPagination(
		true,
		true,
		true,
		"created_at",
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
			return "", "", sql.ErrTxDone
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrTxDone)
	assert.Contains(t, err.Error(), "calculate sort cursor pagination")
}

func TestCalculatePagination_NilBoundaryRecord(t *testing.T) {
	t.Parallel()

	params := listQueryParams{
		limit:       10,
		sortColumn:  "id",
		useIDCursor: true,
	}

	_, err := calculatePagination(
		[]*entities.Exception{nil},
		true,
		false,
		params,
		pkgHTTP.CursorDirectionNext,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, pgcommon.ErrSortCursorBoundaryRecordNil)
	assert.Contains(t, err.Error(), "validate pagination boundaries")
}

func TestCalculateExceptionSortPagination_NilCalculator(t *testing.T) {
	t.Parallel()

	_, err := calculateExceptionSortPagination(
		true,
		true,
		true,
		"created_at",
		time.Now().UTC().Format(time.RFC3339Nano),
		uuid.New().String(),
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		uuid.New().String(),
		nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, pgcommon.ErrSortCursorCalculatorRequired)
	assert.Contains(t, err.Error(), "calculate sort cursor pagination")
}

func TestExceptionSortValue_NilException(t *testing.T) {
	t.Parallel()

	assert.Empty(t, exceptionSortValue(nil, "created_at"))
}

func TestRepository_Update_ConcurrentModification(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	query := regexp.QuoteMeta(`
			UPDATE exceptions SET
				severity = $2,
				status = $3,
				external_system = $4,
				external_issue_id = $5,
				assigned_to = $6,
				due_at = $7,
				resolution_notes = $8,
				resolution_type = $9,
				resolution_reason = $10,
				reason = $11,
				version = version + 1,
				updated_at = $12
			WHERE id = $1 AND version = $13
		`)

	existsQuery := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)`)

	mock.ExpectBegin()
	mock.ExpectExec(query).
		WithArgs(
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(existsQuery).
		WithArgs(exception.ID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	_, err = repo.Update(ctx, exception)
	require.ErrorIs(t, err, ErrConcurrentModification)
}

func TestRepository_FindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()
	transactionID := uuid.New()
	now := time.Now().UTC()
	externalSystem := "JIRA"
	externalIssueID := "JIRA-123"
	assignedTo := "user@example.com"
	dueAt := now.Add(24 * time.Hour)
	resolutionNotes := "Fixed"
	resolutionType := "AUTO"
	resolutionReason := "Matched"
	reason := "Amount mismatch"

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{String: externalSystem, Valid: true},
		sql.NullString{String: externalIssueID, Valid: true},
		sql.NullString{String: assignedTo, Valid: true},
		sql.NullTime{Time: dueAt, Valid: true},
		sql.NullString{String: resolutionNotes, Valid: true},
		sql.NullString{String: resolutionType, Valid: true},
		sql.NullString{String: resolutionReason, Valid: true},
		sql.NullString{String: reason, Valid: true},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, exceptionID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, exceptionID, result.ID)
	require.Equal(t, transactionID, result.TransactionID)
	require.Equal(t, sharedexception.ExceptionSeverityHigh, result.Severity)
	require.Equal(t, value_objects.ExceptionStatusOpen, result.Status)
	require.NotNil(t, result.ExternalSystem)
	require.Equal(t, externalSystem, *result.ExternalSystem)
	require.NotNil(t, result.AssignedTo)
	require.Equal(t, assignedTo, *result.AssignedTo)
	require.NotNil(t, result.DueAt)
	require.NotNil(t, result.Reason)
	require.Equal(t, reason, *result.Reason)
}

func TestRepository_FindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnError(errors.New("database error"))

	result, err := repo.FindByID(ctx, exceptionID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to find exception")
}

func TestRepository_List_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID1 := uuid.New()
	exceptionID2 := uuid.New()
	transactionID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID1.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	).AddRow(
		exceptionID2.String(),
		transactionID.String(),
		"LOW",
		"ASSIGNED",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, pagination, err := repo.List(
		ctx,
		repositories.ExceptionFilter{},
		repositories.CursorFilter{Limit: 10},
	)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.NotNil(t, pagination)
	require.Equal(t, exceptionID1, result[0].ID)
	require.Equal(t, exceptionID2, result[1].ID)
}

func TestRepository_List_WithStatusFilter(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()
	status := value_objects.ExceptionStatusOpen

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	filter := repositories.ExceptionFilter{Status: &status}
	result, _, err := repo.List(ctx, filter, repositories.CursorFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, value_objects.ExceptionStatusOpen, result[0].Status)
}

func TestRepository_List_EmptyResult(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	})

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, pagination, err := repo.List(
		ctx,
		repositories.ExceptionFilter{},
		repositories.CursorFilter{Limit: 10},
	)
	require.NoError(t, err)
	require.Empty(t, result)
	require.Empty(t, pagination.Next)
	require.Empty(t, pagination.Prev)
}

func TestRepository_List_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("query error"))

	result, _, err := repo.List(
		ctx,
		repositories.ExceptionFilter{},
		repositories.CursorFilter{Limit: 10},
	)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to list exceptions")
}

func TestRepository_Update_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()

	exception := &entities.Exception{
		ID:            exceptionID,
		TransactionID: transactionID,
		Severity:      sharedexception.ExceptionSeverityHigh,
		Status:        value_objects.ExceptionStatusOpen,
		Version:       1,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	updateQuery := regexp.QuoteMeta(`
			UPDATE exceptions SET
				severity = $2,
				status = $3,
				external_system = $4,
				external_issue_id = $5,
				assigned_to = $6,
				due_at = $7,
				resolution_notes = $8,
				resolution_type = $9,
				resolution_reason = $10,
				reason = $11,
				version = version + 1,
				updated_at = $12
			WHERE id = $1 AND version = $13
		`)

	selectQuery := regexp.QuoteMeta(`
		SELECT id, transaction_id, severity, status, external_system, external_issue_id,
		       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
		       reason, version, created_at, updated_at
		FROM exceptions
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(2),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectExec(updateQuery).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(selectQuery).WithArgs(exceptionID.String()).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.Update(ctx, exception)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, exceptionID, result.ID)
	require.Equal(t, int64(2), result.Version)
}

func TestRepository_UpdateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()

	exception := &entities.Exception{
		ID:            exceptionID,
		TransactionID: transactionID,
		Severity:      sharedexception.ExceptionSeverityHigh,
		Status:        value_objects.ExceptionStatusOpen,
		Version:       1,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	updateQuery := regexp.QuoteMeta(`
			UPDATE exceptions SET
				severity = $2,
				status = $3,
				external_system = $4,
				external_issue_id = $5,
				assigned_to = $6,
				due_at = $7,
				resolution_notes = $8,
				resolution_type = $9,
				resolution_reason = $10,
				reason = $11,
				version = version + 1,
				updated_at = $12
			WHERE id = $1 AND version = $13
		`)

	selectQuery := regexp.QuoteMeta(`
		SELECT id, transaction_id, severity, status, external_system, external_issue_id,
		       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
		       reason, version, created_at, updated_at
		FROM exceptions
		WHERE id = $1
	`)

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(2),
		now,
		now,
	)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec(updateQuery).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(selectQuery).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.UpdateWithTx(ctx, tx, exception)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, exceptionID, result.ID)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_ExistsForTenant_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)`)

	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	exists, err := repo.ExistsForTenant(ctx, exceptionID)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestRepository_ExistsForTenant_NotExists(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)`)

	mock.ExpectQuery(query).
		WithArgs(exceptionID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	exists, err := repo.ExistsForTenant(ctx, exceptionID)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRepository_ExistsForTenant_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)`)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnError(errors.New("db error"))

	exists, err := repo.ExistsForTenant(ctx, exceptionID)
	require.Error(t, err)
	require.False(t, exists)
	require.Contains(t, err.Error(), "failed to check exception existence")
}

func TestPrepareListParams_CursorStoredRaw(t *testing.T) {
	t.Parallel()

	params := prepareListParams(repositories.CursorFilter{
		Cursor: "invalid-base64-cursor!!!",
		Limit:  10,
	})

	// Cursor validation is now deferred to pgcommon.ApplyIDCursorPagination.
	assert.Equal(t, "invalid-base64-cursor!!!", params.cursorStr)
}

func TestBuildListQuery_InvalidCursor(t *testing.T) {
	t.Parallel()

	params := listQueryParams{
		limit:          10,
		sortColumn:     "id",
		orderDirection: "ASC",
		useIDCursor:    true,
		cursorStr:      "invalid-base64-cursor!!!",
	}

	_, _, _, err := buildListQuery(repositories.ExceptionFilter{}, params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "apply cursor pagination")
}

func TestRepository_List_WithPagination(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	transactionID := uuid.New()

	var exceptions []uuid.UUID
	for i := 0; i < 11; i++ {
		exceptions = append(exceptions, uuid.New())
	}

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	})

	for _, id := range exceptions {
		rows.AddRow(
			id.String(),
			transactionID.String(),
			"HIGH",
			"OPEN",
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			sql.NullTime{},
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			sql.NullString{},
			int64(1),
			now,
			now,
		)
	}

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, pagination, err := repo.List(
		ctx,
		repositories.ExceptionFilter{},
		repositories.CursorFilter{Limit: 10},
	)
	require.NoError(t, err)
	require.Len(t, result, 10)
	require.NotEmpty(t, pagination.Next)
}

func TestRepository_List_SortByCreatedAt(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	cursor := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "created_at",
		SortOrder: "DESC",
	}
	result, _, err := repo.List(ctx, repositories.ExceptionFilter{}, cursor)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestRepository_FindByID_InvalidSeverity(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"INVALID_SEVERITY",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, exceptionID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parse severity")
}

func TestRepository_FindByID_InvalidStatus(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"INVALID_STATUS",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, exceptionID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parse status")
}

func TestRepository_FindByID_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		"not-a-valid-uuid",
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, exceptionID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

func TestRepository_FindByID_InvalidTransactionID(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()

	query := regexp.QuoteMeta(`
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`)

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		"not-a-valid-uuid",
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	)

	mock.ExpectQuery(query).WithArgs(exceptionID.String()).WillReturnRows(rows)

	result, err := repo.FindByID(ctx, exceptionID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

func TestRepository_List_RowsError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	exceptionID := uuid.New()
	transactionID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "transaction_id", "severity", "status", "external_system", "external_issue_id",
		"assigned_to", "due_at", "resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}).AddRow(
		exceptionID.String(),
		transactionID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	).RowError(0, errors.New("row iteration error"))

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, _, err := repo.List(
		ctx,
		repositories.ExceptionFilter{},
		repositories.CursorFilter{Limit: 10},
	)
	require.Error(t, err)
	require.Nil(t, result)
}
