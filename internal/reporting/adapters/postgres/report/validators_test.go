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

func TestValidateFilter_NilContextID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	filter := &entities.ReportFilter{
		ContextID: uuid.Nil,
	}

	err := repo.validateFilter(filter)

	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestValidateFilter_LimitZeroDefaultsTo100(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.ReportFilter{
		ContextID: uuid.New(),
		Limit:     0,
	}

	err := repo.validateFilter(filter)

	require.NoError(t, err)
	assert.Equal(t, 100, filter.Limit)
}

func TestValidateFilter_LimitExceedsMaximum(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.ReportFilter{
		ContextID: uuid.New(),
		Limit:     1001,
	}

	err := repo.validateFilter(filter)

	require.ErrorIs(t, err, ErrLimitExceedsMaximum)
}

func TestValidateFilter_NegativeLimit(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.ReportFilter{
		ContextID: uuid.New(),
		Limit:     -1,
	}

	err := repo.validateFilter(filter)

	require.ErrorIs(t, err, ErrLimitMustBePositive)
}

func TestValidateFilter_ContextIDRequired(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.ReportFilter{
		ContextID: uuid.Nil,
		Limit:     10,
	}

	err := repo.validateFilter(filter)

	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestValidateVarianceFilter_LimitZeroDefaultsTo100(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.VarianceReportFilter{
		ContextID: uuid.New(),
		Limit:     0,
	}

	err := repo.validateVarianceFilter(filter)

	require.NoError(t, err)
	assert.Equal(t, 100, filter.Limit)
}

func TestValidateVarianceFilter_LimitExceedsMaximum(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.VarianceReportFilter{
		ContextID: uuid.New(),
		Limit:     1001,
	}

	err := repo.validateVarianceFilter(filter)

	require.ErrorIs(t, err, ErrLimitExceedsMaximum)
}

func TestValidateVarianceFilter_ContextIDRequired(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.VarianceReportFilter{
		ContextID: uuid.Nil,
		Limit:     10,
	}

	err := repo.validateVarianceFilter(filter)

	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestBuildGenericPaginationArgs(t *testing.T) {
	t.Parallel()

	t.Run("empty cursor and sortOrder defaults to ASC", func(t *testing.T) {
		t.Parallel()

		args, err := buildGenericPaginationArgs("", "", 50)

		require.NoError(t, err)
		assert.Equal(t, "ASC", args.orderDirection)
		assert.Equal(t, 50, args.limit)
		assert.Equal(t, libHTTP.CursorDirectionNext, args.cursor.Direction)
	})

	t.Run("sortOrder ASC preserved", func(t *testing.T) {
		t.Parallel()

		args, err := buildGenericPaginationArgs("", "ASC", 100)

		require.NoError(t, err)
		assert.Equal(t, "ASC", args.orderDirection)
	})

	t.Run("sortOrder DESC preserved", func(t *testing.T) {
		t.Parallel()

		args, err := buildGenericPaginationArgs("", "DESC", 100)

		require.NoError(t, err)
		assert.Equal(t, "DESC", args.orderDirection)
	})

	t.Run("sortOrder lowercase converted to uppercase", func(t *testing.T) {
		t.Parallel()

		args, err := buildGenericPaginationArgs("", "desc", 100)

		require.NoError(t, err)
		assert.Equal(t, "DESC", args.orderDirection)
	})

	t.Run("invalid sortOrder defaults to ASC", func(t *testing.T) {
		t.Parallel()

		args, err := buildGenericPaginationArgs("", "INVALID", 100)

		require.NoError(t, err)
		assert.Equal(t, "ASC", args.orderDirection)
	})

	t.Run("zero limit defaults to 100", func(t *testing.T) {
		t.Parallel()

		args, err := buildGenericPaginationArgs("", "ASC", 0)

		require.NoError(t, err)
		assert.Equal(t, 100, args.limit)
	})

	t.Run("negative limit defaults to 100", func(t *testing.T) {
		t.Parallel()

		args, err := buildGenericPaginationArgs("", "ASC", -5)

		require.NoError(t, err)
		assert.Equal(t, 100, args.limit)
	})

	t.Run("invalid cursor returns error", func(t *testing.T) {
		t.Parallel()

		_, err := buildGenericPaginationArgs("invalid-cursor", "ASC", 50)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cursor format")
	})
}

func TestSafeLimitForPage(t *testing.T) {
	t.Parallel()

	t.Run("positive value returns uint64", func(t *testing.T) {
		t.Parallel()

		result := safeLimitForPage(100)
		assert.Equal(t, uint64(100), result)
	})

	t.Run("zero returns zero", func(t *testing.T) {
		t.Parallel()

		result := safeLimitForPage(0)
		assert.Equal(t, uint64(0), result)
	})

	t.Run("negative value returns zero", func(t *testing.T) {
		t.Parallel()

		result := safeLimitForPage(-10)
		assert.Equal(t, uint64(0), result)
	})

	t.Run("large value converts correctly", func(t *testing.T) {
		t.Parallel()

		result := safeLimitForPage(10000)
		assert.Equal(t, uint64(10000), result)
	})
}

func TestNormalizeLimit(t *testing.T) {
	t.Parallel()

	t.Run("zero returns default", func(t *testing.T) {
		t.Parallel()

		result := normalizeLimit(0)
		assert.Equal(t, defaultLimit, result)
	})

	t.Run("negative returns default", func(t *testing.T) {
		t.Parallel()

		result := normalizeLimit(-5)
		assert.Equal(t, defaultLimit, result)
	})

	t.Run("within range returns same value", func(t *testing.T) {
		t.Parallel()

		result := normalizeLimit(500)
		assert.Equal(t, 500, result)
	})

	t.Run("exceeds max returns max", func(t *testing.T) {
		t.Parallel()

		result := normalizeLimit(maxLimit + 1)
		assert.Equal(t, maxLimit, result)
	})

	t.Run("at max returns max", func(t *testing.T) {
		t.Parallel()

		result := normalizeLimit(maxLimit)
		assert.Equal(t, maxLimit, result)
	})
}

func TestValidateFilter_ValidInput(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.ReportFilter{
		ContextID: uuid.New(),
		Limit:     50,
	}

	err := repo.validateFilter(filter)

	require.NoError(t, err)
	assert.Equal(t, 50, filter.Limit)
}

func TestValidateFilter_MaxLimit(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.ReportFilter{
		ContextID: uuid.New(),
		Limit:     1000,
	}

	err := repo.validateFilter(filter)

	require.NoError(t, err)
	assert.Equal(t, 1000, filter.Limit)
}

func TestValidateVarianceFilter_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	filter := &entities.VarianceReportFilter{
		ContextID: uuid.New(),
		Limit:     10,
	}

	err := repo.validateVarianceFilter(filter)

	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestValidateVarianceFilter_NegativeLimit(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.VarianceReportFilter{
		ContextID: uuid.New(),
		Limit:     -5,
	}

	err := repo.validateVarianceFilter(filter)

	require.ErrorIs(t, err, ErrLimitMustBePositive)
}

func TestValidateVarianceFilter_ValidInput(t *testing.T) {
	t.Parallel()

	mockProvider := &mockInfrastructureProvider{}
	repo := NewRepository(mockProvider)
	filter := &entities.VarianceReportFilter{
		ContextID: uuid.New(),
		Limit:     200,
	}

	err := repo.validateVarianceFilter(filter)

	require.NoError(t, err)
	assert.Equal(t, 200, filter.Limit)
}
