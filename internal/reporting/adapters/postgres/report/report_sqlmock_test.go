//go:build unit

package report

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestScanFailed                = errors.New("scan failed")
	errTestUnexpectedScanValuesCount = errors.New("unexpected scan values count")
	errTestUnsupportedScanDestType   = errors.New("unsupported scan dest type")
	errTestValueNotUUID              = errors.New("value is not uuid.UUID")
	errTestValueNotString            = errors.New("value is not string")
	errTestValueNotDecimal           = errors.New("value is not decimal.Decimal")
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

func TestScanVarianceRow(t *testing.T) {
	t.Parallel()

	totalExpected := decimal.NewFromInt(100)
	netVariance := decimal.NewFromInt(20)
	row, err := scanVarianceRow(fakeVarianceScanner{
		values: []any{
			uuid.New(),
			"USD",
			"FLAT",
			totalExpected,
			decimal.NewFromInt(80),
			netVariance,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "USD", row.Currency)
	assert.Equal(t, "FLAT", row.FeeType)
	require.NotNil(t, row.VariancePct)
	assert.True(
		t,
		row.VariancePct.Equal(netVariance.Div(totalExpected).Mul(decimal.NewFromInt(100))),
	)
}

func TestScanVarianceRow_Error(t *testing.T) {
	t.Parallel()

	_, err := scanVarianceRow(fakeVarianceScanner{err: errTestScanFailed})
	require.ErrorIs(t, err, errTestScanFailed)
}

type mockInfrastructureProvider struct{}

func (m *mockInfrastructureProvider) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

type fakeVarianceScanner struct {
	values []any
	err    error
}

func (scanner fakeVarianceScanner) Scan(dest ...any) error {
	if scanner.err != nil {
		return scanner.err
	}

	if len(dest) != len(scanner.values) {
		return errTestUnexpectedScanValuesCount
	}

	for idx, value := range scanner.values {
		switch target := dest[idx].(type) {
		case *uuid.UUID:
			v, ok := value.(uuid.UUID)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotUUID)
			}

			*target = v
		case *string:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotString)
			}

			*target = v
		case *decimal.Decimal:
			v, ok := value.(decimal.Decimal)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotDecimal)
			}

			*target = v
		default:
			return fmt.Errorf("%w: %T", errTestUnsupportedScanDestType, dest[idx])
		}
	}

	return nil
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

func TestScanVarianceRow_ZeroExpected(t *testing.T) {
	t.Parallel()

	row, err := scanVarianceRow(fakeVarianceScanner{
		values: []any{
			uuid.New(),
			"EUR",
			"PERCENTAGE",
			decimal.Zero,
			decimal.NewFromInt(50),
			decimal.NewFromInt(50),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "EUR", row.Currency)
	assert.Equal(t, "PERCENTAGE", row.FeeType)
	require.Nil(t, row.VariancePct)
}

func TestScanVarianceRow_NegativeVariance(t *testing.T) {
	t.Parallel()

	totalExpected := decimal.NewFromInt(200)
	netVariance := decimal.NewFromInt(-40)
	row, err := scanVarianceRow(fakeVarianceScanner{
		values: []any{
			uuid.New(),
			"GBP",
			"TIERED",
			totalExpected,
			decimal.NewFromInt(240),
			netVariance,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "GBP", row.Currency)
	require.NotNil(t, row.VariancePct)
	expectedPct := netVariance.Div(totalExpected).Mul(decimal.NewFromInt(100))
	assert.True(t, row.VariancePct.Equal(expectedPct))
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

func TestListMatchedForExport_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New()}

		items, err := repo.ListMatchedForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil}

		items, err := repo.ListMatchedForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestListUnmatchedForExport_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New()}

		items, err := repo.ListUnmatchedForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil}

		items, err := repo.ListUnmatchedForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestListVarianceForExport_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.VarianceReportFilter{ContextID: uuid.New()}

		items, err := repo.ListVarianceForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.VarianceReportFilter{ContextID: uuid.Nil}

		items, err := repo.ListVarianceForExport(ctx, filter, 100)

		assert.Nil(t, items)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})
}

func TestGetSummary_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.ReportFilter{ContextID: uuid.New()}

		summary, err := repo.GetSummary(ctx, filter)

		assert.Nil(t, summary)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.Nil, Limit: 10}

		summary, err := repo.GetSummary(ctx, filter)

		assert.Nil(t, summary)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})

	t.Run("negative limit returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.ReportFilter{ContextID: uuid.New(), Limit: -1}

		summary, err := repo.GetSummary(ctx, filter)

		assert.Nil(t, summary)
		require.ErrorIs(t, err, ErrLimitMustBePositive)
	})
}

func TestGetVarianceReport_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		filter := entities.VarianceReportFilter{ContextID: uuid.New()}

		items, pagination, err := repo.GetVarianceReport(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("nil context ID returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.VarianceReportFilter{ContextID: uuid.Nil, Limit: 10}

		items, pagination, err := repo.GetVarianceReport(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrContextIDRequired)
	})

	t.Run("limit exceeds maximum returns error", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(&mockInfrastructureProvider{})
		filter := entities.VarianceReportFilter{ContextID: uuid.New(), Limit: 1001}

		items, pagination, err := repo.GetVarianceReport(ctx, filter)

		assert.Nil(t, items)
		assert.Empty(t, pagination.Next)
		require.ErrorIs(t, err, ErrLimitExceedsMaximum)
	})
}

func TestScanMatchedItem(t *testing.T) {
	t.Parallel()

	t.Run("successful scan", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()
		amount := decimal.NewFromFloat(123.45)
		currency := "USD"
		date := time.Now().UTC()

		scanner := &fakeMatchedScanner{
			values: []any{txID, groupID, sourceID, amount, currency, date},
		}

		item, err := scanMatchedItem(scanner)

		require.NoError(t, err)
		require.NotNil(t, item)
		assert.Equal(t, txID, item.TransactionID)
		assert.Equal(t, groupID, item.MatchGroupID)
		assert.Equal(t, sourceID, item.SourceID)
		assert.True(t, amount.Equal(item.Amount))
		assert.Equal(t, currency, item.Currency)
		assert.Equal(t, date, item.Date)
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()

		scanner := &fakeMatchedScanner{err: errTestScanFailed}

		item, err := scanMatchedItem(scanner)

		assert.Nil(t, item)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scanning matched item")
	})
}

func TestScanUnmatchedItem(t *testing.T) {
	t.Parallel()

	t.Run("successful scan with all fields", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		sourceID := uuid.New()
		amount := decimal.NewFromFloat(99.99)
		currency := "EUR"
		status := "PENDING"
		date := time.Now().UTC()
		exceptionID := uuid.New()
		dueAt := time.Now().UTC().Add(24 * time.Hour).UTC()

		scanner := &fakeUnmatchedScanner{
			values: []any{txID, sourceID, amount, currency, status, date, &exceptionID, &dueAt},
		}

		item, err := scanUnmatchedItem(scanner)

		require.NoError(t, err)
		require.NotNil(t, item)
		assert.Equal(t, txID, item.TransactionID)
		assert.Equal(t, sourceID, item.SourceID)
		assert.True(t, amount.Equal(item.Amount))
		assert.Equal(t, currency, item.Currency)
		assert.Equal(t, status, item.Status)
		assert.Equal(t, date, item.Date)
		require.NotNil(t, item.ExceptionID)
		assert.Equal(t, exceptionID, *item.ExceptionID)
		require.NotNil(t, item.DueAt)
	})

	t.Run("successful scan with nil optional fields", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		sourceID := uuid.New()
		amount := decimal.NewFromFloat(50.00)
		currency := "GBP"
		status := "UNMATCHED"
		date := time.Now().UTC()

		scanner := &fakeUnmatchedScanner{
			values: []any{txID, sourceID, amount, currency, status, date, nil, nil},
		}

		item, err := scanUnmatchedItem(scanner)

		require.NoError(t, err)
		require.NotNil(t, item)
		assert.Equal(t, txID, item.TransactionID)
		assert.Nil(t, item.ExceptionID)
		assert.Nil(t, item.DueAt)
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()

		scanner := &fakeUnmatchedScanner{err: errTestScanFailed}

		item, err := scanUnmatchedItem(scanner)

		assert.Nil(t, item)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scanning unmatched item")
	})
}

func TestApplyUnmatchedExportFilters(t *testing.T) {
	t.Parallel()

	t.Run("no filters applied", func(t *testing.T) {
		t.Parallel()

		filter := entities.ReportFilter{}
		query := squirrel.Select("*").From("transactions")

		result := applyUnmatchedExportFilters(query, filter)

		sql, _, err := result.ToSql()
		require.NoError(t, err)
		assert.NotContains(t, sql, "source_id")
		assert.NotContains(t, sql, "status")
	})

	t.Run("source ID filter applied", func(t *testing.T) {
		t.Parallel()

		sourceID := uuid.New()
		filter := entities.ReportFilter{SourceID: &sourceID}
		query := squirrel.Select("*").From("transactions").PlaceholderFormat(squirrel.Dollar)

		result := applyUnmatchedExportFilters(query, filter)

		sql, args, err := result.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "source_id")
		require.Len(t, args, 1)
	})

	t.Run("status filter applied", func(t *testing.T) {
		t.Parallel()

		status := "PENDING"
		filter := entities.ReportFilter{Status: &status}
		query := squirrel.Select("*").From("transactions").PlaceholderFormat(squirrel.Dollar)

		result := applyUnmatchedExportFilters(query, filter)

		sql, args, err := result.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "status")
		assert.Contains(t, args, status)
	})

	t.Run("both filters applied", func(t *testing.T) {
		t.Parallel()

		sourceID := uuid.New()
		status := "EXCEPTION"
		filter := entities.ReportFilter{SourceID: &sourceID, Status: &status}
		query := squirrel.Select("*").From("transactions").PlaceholderFormat(squirrel.Dollar)

		result := applyUnmatchedExportFilters(query, filter)

		sql, args, err := result.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "source_id")
		assert.Contains(t, sql, "status")
		require.Len(t, args, 2)
		assert.Contains(t, args, status)
	})
}

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
			{SourceID: sourceID, Currency: "USD", FeeType: "FLAT"},
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
}

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

type fakeMatchedScanner struct {
	values []any
	err    error
}

func (scanner *fakeMatchedScanner) Scan(dest ...any) error {
	if scanner.err != nil {
		return scanner.err
	}

	if len(dest) != len(scanner.values) {
		return errTestUnexpectedScanValuesCount
	}

	for idx, value := range scanner.values {
		switch target := dest[idx].(type) {
		case *uuid.UUID:
			v, ok := value.(uuid.UUID)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotUUID)
			}

			*target = v
		case *string:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotString)
			}

			*target = v
		case *decimal.Decimal:
			v, ok := value.(decimal.Decimal)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotDecimal)
			}

			*target = v
		case *time.Time:
			v, ok := value.(time.Time)
			if !ok {
				return fmt.Errorf("value at index %d: expected time.Time", idx)
			}

			*target = v
		default:
			return fmt.Errorf("%w: %T", errTestUnsupportedScanDestType, dest[idx])
		}
	}

	return nil
}

type fakeUnmatchedScanner struct {
	values []any
	err    error
}

func (scanner *fakeUnmatchedScanner) Scan(dest ...any) error {
	if scanner.err != nil {
		return scanner.err
	}

	if len(dest) != len(scanner.values) {
		return errTestUnexpectedScanValuesCount
	}

	for idx, value := range scanner.values {
		switch target := dest[idx].(type) {
		case *uuid.UUID:
			v, ok := value.(uuid.UUID)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotUUID)
			}

			*target = v
		case *string:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotString)
			}

			*target = v
		case *decimal.Decimal:
			v, ok := value.(decimal.Decimal)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotDecimal)
			}

			*target = v
		case *time.Time:
			v, ok := value.(time.Time)
			if !ok {
				return fmt.Errorf("value at index %d: expected time.Time", idx)
			}

			*target = v
		case **uuid.UUID:
			if value == nil {
				*target = nil
			} else {
				v, ok := value.(*uuid.UUID)
				if !ok {
					return fmt.Errorf("value at index %d: expected *uuid.UUID", idx)
				}

				*target = v
			}
		case **time.Time:
			if value == nil {
				*target = nil
			} else {
				v, ok := value.(*time.Time)
				if !ok {
					return fmt.Errorf("value at index %d: expected *time.Time", idx)
				}

				*target = v
			}
		default:
			return fmt.Errorf("%w: %T", errTestUnsupportedScanDestType, dest[idx])
		}
	}

	return nil
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

func setupReportRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

func TestListMatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		contextID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: contextID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}).
				AddRow(txID, groupID, sourceID, "100.00", "USD", time.Now().UTC()))

		items, _, err := repo.ListMatched(ctx, filter)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
	})

	t.Run("empty results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, pagination, err := repo.ListMatched(ctx, filter)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
	})
}

func TestListUnmatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		txID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}).
				AddRow(txID, sourceID, "50.00", "EUR", "PENDING", time.Now().UTC(), nil, nil))

		items, _, err := repo.ListUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
	})

	t.Run("with source ID and status filters", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		status := "PENDING"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))

		items, pagination, err := repo.ListUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
	})
}

func TestGetSummary_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(10, "1000.00"))
		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(5, "500.00"))

		summary, err := repo.GetSummary(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Equal(t, 10, summary.MatchedCount)
		assert.Equal(t, 5, summary.UnmatchedCount)
	})
}

func TestGetVarianceReport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_type", "total_expected", "total_actual", "net_variance"}).
				AddRow(sourceID, "USD", "FLAT", "100.00", "95.00", "-5.00"))

		items, _, err := repo.GetVarianceReport(ctx, filter)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, "USD", items[0].Currency)
		assert.Equal(t, "FLAT", items[0].FeeType)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_type", "total_expected", "total_actual", "net_variance"}))

		items, pagination, err := repo.GetVarianceReport(ctx, filter)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
	})
}

func TestCountMatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful count", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(42))

		count, err := repo.CountMatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(42), count)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(15))

		count, err := repo.CountMatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(15), count)
	})
}

func TestCountUnmatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful count", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(28))

		count, err := repo.CountUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(28), count)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(12))

		count, err := repo.CountUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(12), count)
	})

	t.Run("with status filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		status := "EXCEPTION"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))

		count, err := repo.CountUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(7), count)
	})
}

func TestListMatchedForExport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful export", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}).
				AddRow(txID, groupID, sourceID, "100.00", "USD", time.Now().UTC()))

		items, err := repo.ListMatchedForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, err := repo.ListMatchedForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListUnmatchedForExport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful export", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}).
				AddRow(txID, sourceID, "75.00", "GBP", "PENDING", time.Now().UTC(), nil, nil))

		items, err := repo.ListUnmatchedForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
	})
}

func TestListVarianceForExport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful export", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_type", "total_expected", "total_actual", "net_variance"}).
				AddRow(sourceID, "USD", "FLAT", "200.00", "190.00", "-10.00"))

		items, err := repo.ListVarianceForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Len(t, items, 1)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_type", "total_expected", "total_actual", "net_variance"}))

		items, err := repo.ListVarianceForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListMatchedPage_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful page query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}).
				AddRow(txID, groupID, sourceID, "100.00", "USD", time.Now().UTC()))

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Empty(t, nextKey)
	})

	t.Run("with valid afterKey", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		afterKey := uuid.New().String()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, afterKey, 10)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, nextKey)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, _, err := repo.ListMatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListUnmatchedPage_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful page query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}).
				AddRow(txID, sourceID, "50.00", "EUR", "PENDING", time.Now().UTC(), nil, nil))

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Empty(t, nextKey)
	})

	t.Run("with valid afterKey", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		afterKey := uuid.New().String()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, afterKey, 10)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, nextKey)
	})

	t.Run("with source ID and status filters", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		status := "EXCEPTION"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))

		items, _, err := repo.ListUnmatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListVariancePage_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful page query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_type", "total_expected", "total_actual", "net_variance"}).
				AddRow(sourceID, "USD", "FLAT", "100.00", "95.00", "-5.00"))

		items, nextKey, err := repo.ListVariancePage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Empty(t, nextKey)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_type", "total_expected", "total_actual", "net_variance"}))

		items, _, err := repo.ListVariancePage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestApplyVarianceCursor(t *testing.T) {
	t.Parallel()

	t.Run("valid cursor returns updated query and args", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "550e8400-e29b-41d4-a716-446655440000:USD:FLAT"

		cf, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.NoError(t, err)
		assert.Contains(t, cf.query, "AND (t.source_id, fv.currency, r.structure_type) > ($2, $3, $4)")
		require.Len(t, cf.args, 4)
		assert.Equal(t, uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), cf.args[1])
		assert.Equal(t, "USD", cf.args[2])
		assert.Equal(t, "FLAT", cf.args[3])
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
		afterKey := "not-a-uuid:USD:FLAT"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "source_id is not a valid UUID")
	})

	t.Run("invalid currency in cursor returns error", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "550e8400-e29b-41d4-a716-446655440000:usd:FLAT"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "currency is not a valid 3-letter ISO code")
	})

	t.Run("invalid fee type in cursor returns error", func(t *testing.T) {
		t.Parallel()

		baseQuery := "SELECT * FROM t WHERE ctx = $1"
		baseArgs := []any{"ctx-val"}
		afterKey := "550e8400-e29b-41d4-a716-446655440000:USD:UNKNOWN"

		_, err := applyVarianceCursor(afterKey, baseQuery, baseArgs, 2)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidVarianceCursor)
		assert.Contains(t, err.Error(), "fee type \"UNKNOWN\" is not a recognized structure type")
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
}
