// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestListMatched_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New(), Limit: 10}

		items, pagination, err := repo.ListMatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil, Limit: 10}

		items, pagination, err := repo.ListMatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})

	t.Run("negative limit returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.New(), Limit: -1}

		items, pagination, err := repo.ListMatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrLimitMustBePositive)
	})

	t.Run("limit exceeds maximum returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.New(), Limit: 1001}

		items, pagination, err := repo.ListMatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrLimitExceedsMaximum)
	})
}

func TestListUnmatched_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New(), Limit: 10}

		items, pagination, err := repo.ListUnmatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil, Limit: 10}

		items, pagination, err := repo.ListUnmatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestCountMatched_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New()}

		count, err := repo.CountMatched(ctx, filter)

		assert.Equal(t, int64(0), count)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil}

		count, err := repo.CountMatched(ctx, filter)

		assert.Equal(t, int64(0), count)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestCountUnmatched_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New()}

		count, err := repo.CountUnmatched(ctx, filter)

		assert.Equal(t, int64(0), count)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil}

		count, err := repo.CountUnmatched(ctx, filter)

		assert.Equal(t, int64(0), count)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestListMatchedPage_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New()}

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil}

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestListUnmatchedPage_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New()}

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil}

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestListVariancePage_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.VarianceReportFilter{ContextID: uuid.New()}

		items, nextKey, err := repo.ListVariancePage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.VarianceReportFilter{ContextID: uuid.Nil}

		items, nextKey, err := repo.ListVariancePage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}
