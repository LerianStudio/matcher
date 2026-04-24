// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package source

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
)

func TestBuildPaginatedSourceQuery_InvalidCursorDirection(t *testing.T) {
	t.Parallel()

	baseQuery := squirrel.Select(strings.Split(sourceColumns, ", ")...).
		From("reconciliation_sources").
		PlaceholderFormat(squirrel.Dollar)

	_, err := buildPaginatedSourceQuery(baseQuery, libHTTP.Cursor{Direction: "sideways"}, 10)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cursor direction rules")
}

func TestBuildPaginatedSourceQuery_BackwardCursorUsesValidatedClauses(t *testing.T) {
	t.Parallel()

	cursorID := uuid.New().String()
	baseQuery := squirrel.Select(strings.Split(sourceColumns, ", ")...).
		From("reconciliation_sources").
		PlaceholderFormat(squirrel.Dollar)

	queryBuilder, err := buildPaginatedSourceQuery(baseQuery, libHTTP.Cursor{
		ID:        cursorID,
		Direction: libHTTP.CursorDirectionPrev,
	}, 10)
	require.NoError(t, err)

	query, args, err := queryBuilder.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "WHERE id < $1")
	assert.Contains(t, query, "ORDER BY id DESC")
	assert.Contains(t, query, "LIMIT 11")
	require.Len(t, args, 1)
	assert.Equal(t, cursorID, args[0])
}

func TestRepository_FindByContextID_BackwardCursorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	cursorID := uuid.New()
	olderID := uuid.New()
	newerID := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmockRowsForSources(contextID, now, configJSON,
		sourceRow{ID: newerID, Name: "Source 2", Type: "GATEWAY", Side: "RIGHT"},
		sourceRow{ID: olderID, Name: "Source 1", Type: "LEDGER", Side: "LEFT"},
	)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT id, context_id, name, type, side, config, created_at, updated_at FROM reconciliation_sources WHERE context_id = $1 AND id < $2 ORDER BY id DESC LIMIT 2",
	)).
		WithArgs(contextID.String(), cursorID.String()).
		WillReturnRows(rows)
	mock.ExpectCommit()

	backwardCursor := encodeLibCommonsTestCursor(t, cursorID, libHTTP.CursorDirectionPrev)
	results, pagination, err := repo.FindByContextID(ctx, contextID, backwardCursor, 1)

	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, newerID, results[0].ID)
	assert.True(t, pagination.Next != "" || pagination.Prev != "")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextID_NonExistentCursorIDStablePage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	nonExistentCursorID := uuid.New()
	firstID := uuid.New()
	secondID := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmockRowsForSources(contextID, now, configJSON,
		sourceRow{ID: firstID, Name: "Source 1", Type: "LEDGER", Side: "LEFT"},
		sourceRow{ID: secondID, Name: "Source 2", Type: "BANK", Side: "RIGHT"},
	)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT id, context_id, name, type, side, config, created_at, updated_at FROM reconciliation_sources WHERE context_id = $1 AND id > $2 ORDER BY id ASC LIMIT 2",
	)).
		WithArgs(contextID.String(), nonExistentCursorID.String()).
		WillReturnRows(rows)
	mock.ExpectCommit()

	forwardCursor := encodeLibCommonsTestCursor(t, nonExistentCursorID, libHTTP.CursorDirectionNext)
	results, pagination, err := repo.FindByContextID(ctx, contextID, forwardCursor, 1)

	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, firstID, results[0].ID)
	assert.NotEmpty(t, pagination.Next)
	require.NoError(t, mock.ExpectationsWereMet())
}

type sourceRow struct {
	ID   uuid.UUID
	Name string
	Type string
	Side string
}

func sqlmockRowsForSources(
	contextID uuid.UUID,
	now time.Time,
	configJSON []byte,
	rows ...sourceRow,
) *sqlmock.Rows {
	result := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	})

	for _, row := range rows {
		result.AddRow(row.ID.String(), contextID.String(), row.Name, row.Type, row.Side, configJSON, now, now)
	}

	return result
}
