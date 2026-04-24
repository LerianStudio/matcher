// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestBuildPaginationArgs(t *testing.T) {
	t.Parallel()

	t.Run("uses filter values", func(t *testing.T) {
		t.Parallel()

		filter := entities.ReportFilter{
			Cursor:    "",
			SortOrder: "DESC",
			Limit:     25,
		}

		args, err := buildPaginationArgs(filter)

		require.NoError(t, err)
		assert.Equal(t, "DESC", args.orderDirection)
		assert.Equal(t, 25, args.limit)
	})

	t.Run("invalid cursor returns error", func(t *testing.T) {
		t.Parallel()

		filter := entities.ReportFilter{
			Cursor: "not-valid-base64-cursor",
		}

		_, err := buildPaginationArgs(filter)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cursor format")
	})
}

func TestBuildVariancePaginationArgs(t *testing.T) {
	t.Parallel()

	t.Run("uses variance filter values", func(t *testing.T) {
		t.Parallel()

		mockProvider := &mockInfrastructureProvider{}
		repo := NewRepository(mockProvider)
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			Cursor:    "",
			SortOrder: "DESC",
			Limit:     50,
		}

		args, err := repo.buildVariancePaginationArgs(filter)

		require.NoError(t, err)
		assert.Equal(t, "DESC", args.orderDirection)
		assert.Equal(t, 50, args.limit)
	})

	t.Run("defaults to ASC for invalid sort order", func(t *testing.T) {
		t.Parallel()

		mockProvider := &mockInfrastructureProvider{}
		repo := NewRepository(mockProvider)
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SortOrder: "INVALID",
			Limit:     100,
		}

		args, err := repo.buildVariancePaginationArgs(filter)

		require.NoError(t, err)
		assert.Equal(t, "ASC", args.orderDirection)
	})
}

func TestPaginateReportItems(t *testing.T) {
	t.Parallel()

	t.Run("empty items returns empty result", func(t *testing.T) {
		t.Parallel()

		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			Cursor:    "",
			Limit:     10,
		}
		args := paginationArgs{
			orderDirection: "ASC",
			limit:          10,
		}

		items, pagination, err := paginateReportItems(
			filter,
			args,
			[]*entities.MatchedItem{},
			func(item *entities.MatchedItem) string { return item.TransactionID.String() },
		)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})

	t.Run("items within limit returns items with pagination", func(t *testing.T) {
		t.Parallel()

		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			Cursor:    "",
			Limit:     10,
		}
		args := paginationArgs{
			orderDirection: "ASC",
			limit:          10,
			// Match production default from buildGenericPaginationArgs:
			// first-page requests always have CursorDirectionNext.
			cursor: libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext},
		}

		txID1 := uuid.New()
		txID2 := uuid.New()
		testItems := []*entities.MatchedItem{
			{TransactionID: txID1},
			{TransactionID: txID2},
		}

		items, pagination, err := paginateReportItems(
			filter,
			args,
			testItems,
			func(item *entities.MatchedItem) string { return item.TransactionID.String() },
		)

		require.NoError(t, err)
		assert.Len(t, items, 2)
		assert.Empty(t, pagination.Prev)
	})
}

func TestPaginateVarianceItems(t *testing.T) {
	t.Parallel()

	t.Run("empty items returns empty result", func(t *testing.T) {
		t.Parallel()

		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			Cursor:    "",
			Limit:     10,
		}
		args := paginationArgs{
			orderDirection: "ASC",
			limit:          10,
		}

		items, pagination, err := paginateVarianceItems(
			filter,
			args,
			[]*entities.VarianceReportRow{},
		)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})

	t.Run("items within limit returns items with pagination", func(t *testing.T) {
		t.Parallel()

		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			Cursor:    "",
			Limit:     10,
		}
		args := paginationArgs{
			orderDirection: "ASC",
			limit:          10,
			// Match production default from buildGenericPaginationArgs:
			// first-page requests always have CursorDirectionNext.
			cursor: libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext},
		}

		sourceID := uuid.New()
		testItems := []*entities.VarianceReportRow{
			{SourceID: sourceID, Currency: "USD", FeeScheduleID: uuid.New(), FeeScheduleName: "FLAT"},
		}

		items, pagination, err := paginateVarianceItems(
			filter,
			args,
			testItems,
		)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Empty(t, pagination.Prev)
	})

	t.Run("previous-direction pagination returns bounded page", func(t *testing.T) {
		t.Parallel()

		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			Cursor:    "some-cursor",
			Limit:     2,
		}
		args := paginationArgs{
			orderDirection: "ASC",
			limit:          2,
			cursor: libHTTP.Cursor{
				Direction: libHTTP.CursorDirectionPrev,
			},
		}

		testItems := []*entities.VarianceReportRow{
			{SourceID: uuid.New(), Currency: "USD", FeeScheduleID: uuid.New(), FeeScheduleName: "Alpha"},
			{SourceID: uuid.New(), Currency: "USD", FeeScheduleID: uuid.New(), FeeScheduleName: "Beta"},
			{SourceID: uuid.New(), Currency: "USD", FeeScheduleID: uuid.New(), FeeScheduleName: "Gamma"},
		}

		items, pagination, err := paginateVarianceItems(filter, args, testItems)

		require.NoError(t, err)
		assert.Len(t, items, 2)
		assert.True(t, pagination.Next != "" || pagination.Prev != "")
	})
}

func TestApplyVarianceCursor(t *testing.T) {
	t.Parallel()

	t.Run("valid cursor returns updated query and args", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		feeScheduleID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440001")
		afterKey := "550e8400-e29b-41d4-a716-446655440000:USD:" + feeScheduleID.String()

		cf, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.NoError(t, err)
		assert.Contains(t, cf.query, "AND (t.source_id, fv.currency, fv.fee_schedule_id) > ($2, $3, $4)")
		require.Len(t, cf.args, 4)
		assert.Equal(t, uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), cf.args[1])
		assert.Equal(t, "USD", cf.args[2])
		assert.Equal(t, feeScheduleID, cf.args[3])
		assert.Equal(t, 5, cf.argIdx)
	})

	t.Run("malformed cursor with wrong part count returns error", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "bad:cursor"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "expected 3 parts, got 2")
	})

	t.Run("invalid UUID in cursor returns error", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "not-a-uuid:USD:550e8400-e29b-41d4-a716-446655440001"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "source_id is not a valid UUID")
	})

	t.Run("invalid currency in cursor returns error", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "550e8400-e29b-41d4-a716-446655440000:usd:550e8400-e29b-41d4-a716-446655440001"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "currency is not a valid 3-letter ISO code")
	})

	t.Run("invalid fee schedule id in cursor returns error", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "550e8400-e29b-41d4-a716-446655440000:USD:"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "fee_schedule_id is not a valid UUID")
	})

	t.Run("empty cursor returns unchanged filter", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}

		cf, err := applyVarianceCursor("", baseQuery, baseArgs, 2)

		require.NoError(t, err)
		assert.Equal(t, baseQuery, cf.query)
		assert.Equal(t, baseArgs, cf.args)
		assert.Equal(t, 2, cf.argIdx)
	})

	t.Run("malformed fee schedule id in cursor returns error", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "550e8400-e29b-41d4-a716-446655440000:USD:not-a-uuid"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "fee_schedule_id is not a valid UUID")
	})
}
