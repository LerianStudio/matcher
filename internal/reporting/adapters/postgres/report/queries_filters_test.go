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
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestListMatchedPage_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, _, err := repo.ListMatchedPage(ctx, filter, "", 100)
		require.Error(t, err)
	})
}

func TestListUnmatchedPage_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, _, err := repo.ListUnmatchedPage(ctx, filter, "", 100)
		require.Error(t, err)
	})

	t.Run("with status filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		status := "PENDING"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, _, err := repo.ListUnmatchedPage(ctx, filter, "", 100)
		require.Error(t, err)
	})
}

func TestListVariancePage_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, _, err := repo.ListVariancePage(ctx, filter, "", 100)
		require.Error(t, err)
	})
}

func TestListMatchedForExport_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, err := repo.ListMatchedForExport(ctx, filter, 100)
		require.Error(t, err)
	})
}

func TestListUnmatchedForExport_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID and status filters", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		status := "EXCEPTION"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, err := repo.ListUnmatchedForExport(ctx, filter, 100)
		require.Error(t, err)
	})
}

func TestListVarianceForExport_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, err := repo.ListVarianceForExport(ctx, filter, 100)
		require.Error(t, err)
	})
}

func TestGetSummary_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		_, err := repo.GetSummary(ctx, filter)
		require.Error(t, err)
	})

	t.Run("with status filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		status := "PENDING"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		_, err := repo.GetSummary(ctx, filter)
		require.Error(t, err)
	})
}

func TestGetVarianceReport_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		_, _, err := repo.GetVarianceReport(ctx, filter)
		require.Error(t, err)
	})
}

func TestCountMatched_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, err := repo.CountMatched(ctx, filter)
		require.Error(t, err)
	})
}

func TestCountUnmatched_WithFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, err := repo.CountUnmatched(ctx, filter)
		require.Error(t, err)
	})

	t.Run("with status filter", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		status := "PENDING"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		_, err := repo.CountUnmatched(ctx, filter)
		require.Error(t, err)
	})
}
