// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestListMatchedPage_LimitNormalization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("zero limit normalizes to default", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.New()}

		_, _, err := repo.ListMatchedPage(ctx, filter, "", 0)

		require.Error(t, err)
		assert.NotContains(t, err.Error(), "limit")
	})

	t.Run("exceeding max limit normalizes to max", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.New()}

		_, _, err := repo.ListMatchedPage(ctx, filter, "", 5000)

		require.Error(t, err)
		assert.NotContains(t, err.Error(), "limit exceeds")
	})
}

func TestListUnmatchedPage_LimitNormalization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("zero limit normalizes to default", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.New()}

		_, _, err := repo.ListUnmatchedPage(ctx, filter, "", 0)

		require.Error(t, err)
		assert.NotContains(t, err.Error(), "limit")
	})

	t.Run("negative limit normalizes to default", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.New()}

		_, _, err := repo.ListUnmatchedPage(ctx, filter, "", -5)

		require.Error(t, err)
		assert.NotContains(t, err.Error(), "limit")
	})
}

func TestListVariancePage_LimitNormalization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("zero limit normalizes to default", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.VarianceReportFilter{ContextID: uuid.New()}

		_, _, err := repo.ListVariancePage(ctx, filter, "", 0)

		require.Error(t, err)
		assert.NotContains(t, err.Error(), "limit")
	})

	t.Run("exceeding max limit normalizes to max", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.VarianceReportFilter{ContextID: uuid.New()}

		_, _, err := repo.ListVariancePage(ctx, filter, "", 5000)

		require.Error(t, err)
		assert.NotContains(t, err.Error(), "limit exceeds")
	})
}

func TestListMatchedPage_AfterKeyParsing(t *testing.T) {
	t.Parallel()

	t.Run("invalid afterKey returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, "invalid-uuid", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.Error(t, err)
	})
}

func TestListUnmatchedPage_AfterKeyParsing(t *testing.T) {
	t.Parallel()

	t.Run("invalid afterKey returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, "invalid-uuid", 100)

		assert.Nil(t, items)
		assert.Empty(t, nextKey)
		require.Error(t, err)
	})
}
