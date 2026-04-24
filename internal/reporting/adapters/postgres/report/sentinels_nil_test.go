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

func TestRepositoryMethods_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	t.Run("validateFilter returns ErrRepositoryNotInitialized", func(t *testing.T) {
		t.Parallel()

		filter := &entities.ReportFilter{ContextID: uuid.New()}

		err := repo.validateFilter(filter)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with provider", func(t *testing.T) {
		t.Parallel()

		provider := &mockInfrastructureProvider{}
		repo := NewRepository(provider)

		assert.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("creates repository with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		assert.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrContextIDRequired", ErrContextIDRequired},
		{"ErrLimitMustBePositive", ErrLimitMustBePositive},
		{"ErrOffsetMustBeNonNegative", ErrOffsetMustBeNonNegative},
		{"ErrLimitExceedsMaximum", ErrLimitExceedsMaximum},
		{"ErrExportLimitExceeded", ErrExportLimitExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestRepository_NilReceiverChecks(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()
	filter := entities.ReportFilter{ContextID: uuid.New()}
	varianceFilter := entities.VarianceReportFilter{ContextID: uuid.New()}

	t.Run("ListMatched nil receiver", func(t *testing.T) {
		t.Parallel()

		items, pagination, err := repo.ListMatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("ListUnmatched nil receiver", func(t *testing.T) {
		t.Parallel()

		items, pagination, err := repo.ListUnmatched(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetSummary nil receiver", func(t *testing.T) {
		t.Parallel()

		summary, err := repo.GetSummary(ctx, filter)

		assert.Nil(t, summary)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetVarianceReport nil receiver", func(t *testing.T) {
		t.Parallel()

		items, pagination, err := repo.GetVarianceReport(ctx, varianceFilter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("ListMatchedForExport nil receiver", func(t *testing.T) {
		t.Parallel()

		items, err := repo.ListMatchedForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("ListUnmatchedForExport nil receiver", func(t *testing.T) {
		t.Parallel()

		items, err := repo.ListUnmatchedForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("ListVarianceForExport nil receiver", func(t *testing.T) {
		t.Parallel()

		items, err := repo.ListVarianceForExport(ctx, varianceFilter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("ListMatchedPage nil receiver", func(t *testing.T) {
		t.Parallel()

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("ListUnmatchedPage nil receiver", func(t *testing.T) {
		t.Parallel()

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("ListVariancePage nil receiver", func(t *testing.T) {
		t.Parallel()

		items, nextKey, err := repo.ListVariancePage(ctx, varianceFilter, "", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("CountMatched nil receiver", func(t *testing.T) {
		t.Parallel()

		count, err := repo.CountMatched(ctx, filter)

		assert.Equal(t, int64(0), count)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("CountUnmatched nil receiver", func(t *testing.T) {
		t.Parallel()

		count, err := repo.CountUnmatched(ctx, filter)

		assert.Equal(t, int64(0), count)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})
}
