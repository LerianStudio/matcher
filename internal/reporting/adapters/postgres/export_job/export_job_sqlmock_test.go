//go:build unit

package export_job

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

type fakeInfraProvider struct{}

func (f *fakeInfraProvider) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	return nil, nil
}

func (f *fakeInfraProvider) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	return nil, nil
}

func (f *fakeInfraProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (f *fakeInfraProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

var _ ports.InfrastructureProvider = (*fakeInfraProvider)(nil)

func TestNewRepository(t *testing.T) {
	t.Parallel()

	provider := &fakeInfraProvider{}
	repo := NewRepository(provider)

	assert.NotNil(t, repo)
	assert.Equal(t, provider, repo.provider)
}

func TestNewRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	assert.NotNil(t, repo)
	assert.Nil(t, repo.provider)
}

func TestSafeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int
		expected uint64
	}{
		{
			name:     "positive value",
			input:    10,
			expected: 10,
		},
		{
			name:     "zero value",
			input:    0,
			expected: 0,
		},
		{
			name:     "negative value",
			input:    -5,
			expected: 0,
		},
		{
			name:     "large positive value",
			input:    1000000,
			expected: 1000000,
		},
		{
			name:     "one",
			input:    1,
			expected: 1,
		},
		{
			name:     "negative one",
			input:    -1,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := safeLimit(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportJobColumns(t *testing.T) {
	t.Parallel()

	columns := exportJobColumns()

	expectedColumns := []string{
		"id", "tenant_id", "context_id", "report_type", "format", "filter",
		"status", "records_written", "bytes_written", "file_key", "file_name",
		"sha256", "error", "attempts", "next_retry_at", "created_at", "started_at", "finished_at", "expires_at", "updated_at",
	}

	assert.Equal(t, expectedColumns, columns)
	assert.Len(t, columns, 20)
}

func TestExportJobColumnsString(t *testing.T) {
	t.Parallel()

	columnsStr := exportJobColumnsString()

	expected := "id, tenant_id, context_id, report_type, format, filter, status, records_written, bytes_written, file_key, file_name, sha256, error, attempts, next_retry_at, created_at, started_at, finished_at, expires_at, updated_at"

	assert.Equal(t, expected, columnsStr)
}

func TestExportJobColumnsConsistency(t *testing.T) {
	t.Parallel()

	columns := exportJobColumns()
	columnsStr := exportJobColumnsString()

	for _, col := range columns {
		assert.Contains(t, columnsStr, col, "column %s should be in columns string", col)
	}
}

func TestStringToNullString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected sql.NullString
	}{
		{
			name:     "empty string returns invalid NullString",
			input:    "",
			expected: sql.NullString{String: "", Valid: false},
		},
		{
			name:     "non-empty string returns valid NullString",
			input:    "test",
			expected: sql.NullString{String: "test", Valid: true},
		},
		{
			name:     "whitespace only returns valid NullString",
			input:    "   ",
			expected: sql.NullString{String: "   ", Valid: true},
		},
		{
			name:     "single character returns valid NullString",
			input:    "a",
			expected: sql.NullString{String: "a", Valid: true},
		},
		{
			name:  "long string returns valid NullString",
			input: "this is a very long string with lots of characters",
			expected: sql.NullString{
				String: "this is a very long string with lots of characters",
				Valid:  true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := pgcommon.StringToNullString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimePtrToNullTime(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name     string
		input    *time.Time
		expected sql.NullTime
	}{
		{
			name:     "nil time returns invalid NullTime",
			input:    nil,
			expected: sql.NullTime{Time: time.Time{}, Valid: false},
		},
		{
			name:     "valid time returns valid NullTime",
			input:    &now,
			expected: sql.NullTime{Time: now, Valid: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := pgcommon.TimePtrToNullTime(tt.input)
			assert.Equal(t, tt.expected.Valid, result.Valid)

			if tt.expected.Valid {
				assert.Equal(t, tt.expected.Time, result.Time)
			}
		})
	}
}

func TestTimePtrToNullTime_ZeroTime(t *testing.T) {
	t.Parallel()

	zeroTime := time.Time{}
	result := pgcommon.TimePtrToNullTime(&zeroTime)

	assert.True(t, result.Valid)
	assert.True(t, result.Time.IsZero())
}

func TestExportJobTableConstant(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "export_jobs", exportJobsTable)
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfraProvider{})
	assert.NotNil(t, repo)
}

func TestExportJobFilter_ToJSON(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	status := entities.ExportJobStatusQueued
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	tests := []struct {
		name   string
		filter entities.ExportJobFilter
	}{
		{
			name: "empty filter",
			filter: entities.ExportJobFilter{
				DateFrom: time.Time{},
				DateTo:   time.Time{},
			},
		},
		{
			name: "filter with dates only",
			filter: entities.ExportJobFilter{
				DateFrom: dateFrom,
				DateTo:   dateTo,
			},
		},
		{
			name: "filter with all fields",
			filter: entities.ExportJobFilter{
				DateFrom: dateFrom,
				DateTo:   dateTo,
				SourceID: &sourceID,
				Status:   &status,
			},
		},
		{
			name: "filter with source_id only",
			filter: entities.ExportJobFilter{
				DateFrom: dateFrom,
				DateTo:   dateTo,
				SourceID: &sourceID,
			},
		},
		{
			name: "filter with status only",
			filter: entities.ExportJobFilter{
				DateFrom: dateFrom,
				DateTo:   dateTo,
				Status:   &status,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			jsonBytes, err := tt.filter.ToJSON()
			require.NoError(t, err)
			assert.NotEmpty(t, jsonBytes)

			parsedFilter, err := entities.ExportJobFilterFromJSON(jsonBytes)
			require.NoError(t, err)

			assert.Equal(t, tt.filter.DateFrom.UTC(), parsedFilter.DateFrom.UTC())
			assert.Equal(t, tt.filter.DateTo.UTC(), parsedFilter.DateTo.UTC())

			if tt.filter.SourceID != nil {
				require.NotNil(t, parsedFilter.SourceID)
				assert.Equal(t, *tt.filter.SourceID, *parsedFilter.SourceID)
			} else {
				assert.Nil(t, parsedFilter.SourceID)
			}

			if tt.filter.Status != nil {
				require.NotNil(t, parsedFilter.Status)
				assert.Equal(t, *tt.filter.Status, *parsedFilter.Status)
			} else {
				assert.Nil(t, parsedFilter.Status)
			}
		})
	}
}

func TestExportJobFilterFromJSON_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "empty bytes",
			input: []byte{},
		},
		{
			name:  "invalid json",
			input: []byte("not valid json"),
		},
		{
			name:  "incomplete json",
			input: []byte("{\"date_from\":"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := entities.ExportJobFilterFromJSON(tt.input)
			require.Error(t, err)
		})
	}
}

func TestNewExportJob_ValidFormats(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	formats := []entities.ExportFormat{
		entities.ExportFormatCSV,
		entities.ExportFormatJSON,
		entities.ExportFormatXML,
		entities.ExportFormatPDF,
	}

	ctx := context.Background()

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()

			job, err := entities.NewExportJob(
				ctx,
				tenantID,
				contextID,
				entities.ExportReportTypeMatched,
				format,
				filter,
			)
			require.NoError(t, err)
			assert.NotNil(t, job)
			assert.Equal(t, format, job.Format)
			assert.Equal(t, entities.ExportJobStatusQueued, job.Status)
		})
	}
}

func TestNewExportJob_ValidReportTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	reportTypes := []entities.ExportReportType{
		entities.ExportReportTypeMatched,
		entities.ExportReportTypeUnmatched,
		entities.ExportReportTypeSummary,
		entities.ExportReportTypeVariance,
		entities.ExportReportTypeExceptions,
	}

	for _, reportType := range reportTypes {
		t.Run(string(reportType), func(t *testing.T) {
			t.Parallel()

			job, err := entities.NewExportJob(
				ctx,
				tenantID,
				contextID,
				reportType,
				entities.ExportFormatCSV,
				filter,
			)
			require.NoError(t, err)
			assert.NotNil(t, job)
			assert.Equal(t, reportType, job.ReportType)
		})
	}
}

func TestNewExportJob_InvalidFormat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{}

	job, err := entities.NewExportJob(
		ctx,
		tenantID,
		contextID,
		entities.ExportReportTypeMatched,
		"INVALID",
		filter,
	)
	assert.Nil(t, job)
	require.Error(t, err)
}

func TestNewExportJob_InvalidReportType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{}

	job, err := entities.NewExportJob(
		ctx,
		tenantID,
		contextID,
		"INVALID",
		entities.ExportFormatCSV,
		filter,
	)
	assert.Nil(t, job)
	require.Error(t, err)
}

func TestExportJob_StatusTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	t.Run("MarkRunning", func(t *testing.T) {
		t.Parallel()

		job, err := entities.NewExportJob(
			ctx,
			tenantID,
			contextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)
		assert.Equal(t, entities.ExportJobStatusQueued, job.Status)
		assert.Nil(t, job.StartedAt)

		job.MarkRunning()

		assert.Equal(t, entities.ExportJobStatusRunning, job.Status)
		assert.NotNil(t, job.StartedAt)
	})

	t.Run("MarkSucceeded", func(t *testing.T) {
		t.Parallel()

		job, err := entities.NewExportJob(
			ctx,
			tenantID,
			contextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		job.MarkRunning()
		job.MarkSucceeded("file-key", "export.csv", "sha256hash", 100, 5000)

		assert.Equal(t, entities.ExportJobStatusSucceeded, job.Status)
		assert.Equal(t, "file-key", job.FileKey)
		assert.Equal(t, "export.csv", job.FileName)
		assert.Equal(t, "sha256hash", job.SHA256)
		assert.Equal(t, int64(100), job.RecordsWritten)
		assert.Equal(t, int64(5000), job.BytesWritten)
		assert.NotNil(t, job.FinishedAt)
	})

	t.Run("MarkFailed", func(t *testing.T) {
		t.Parallel()

		job, err := entities.NewExportJob(
			ctx,
			tenantID,
			contextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		job.MarkRunning()
		job.MarkFailed("something went wrong")

		assert.Equal(t, entities.ExportJobStatusFailed, job.Status)
		assert.Equal(t, "something went wrong", job.Error)
		assert.NotNil(t, job.FinishedAt)
	})

	t.Run("MarkExpired", func(t *testing.T) {
		t.Parallel()

		job, err := entities.NewExportJob(
			ctx,
			tenantID,
			contextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		job.MarkRunning()
		job.MarkSucceeded("key", "file", "hash", 10, 100)
		job.MarkExpired()

		assert.Equal(t, entities.ExportJobStatusExpired, job.Status)
	})

	t.Run("MarkCanceled", func(t *testing.T) {
		t.Parallel()

		job, err := entities.NewExportJob(
			ctx,
			tenantID,
			contextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		job.MarkRunning()
		job.MarkCanceled()

		assert.Equal(t, entities.ExportJobStatusCanceled, job.Status)
		assert.NotNil(t, job.FinishedAt)
	})
}

func TestExportJob_IsTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     entities.ExportJobStatus
		isTerminal bool
	}{
		{
			name:       "queued is not terminal",
			status:     entities.ExportJobStatusQueued,
			isTerminal: false,
		},
		{
			name:       "running is not terminal",
			status:     entities.ExportJobStatusRunning,
			isTerminal: false,
		},
		{
			name:       "succeeded is terminal",
			status:     entities.ExportJobStatusSucceeded,
			isTerminal: true,
		},
		{
			name:       "failed is terminal",
			status:     entities.ExportJobStatusFailed,
			isTerminal: true,
		},
		{
			name:       "expired is terminal",
			status:     entities.ExportJobStatusExpired,
			isTerminal: true,
		},
		{
			name:       "canceled is terminal",
			status:     entities.ExportJobStatusCanceled,
			isTerminal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job := &entities.ExportJob{Status: tt.status}
			assert.Equal(t, tt.isTerminal, job.IsTerminal())
		})
	}
}

func TestExportJob_IsDownloadable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		status         entities.ExportJobStatus
		fileKey        string
		isDownloadable bool
	}{
		{
			name:           "succeeded with file key is downloadable",
			status:         entities.ExportJobStatusSucceeded,
			fileKey:        "some-file-key",
			isDownloadable: true,
		},
		{
			name:           "succeeded without file key is not downloadable",
			status:         entities.ExportJobStatusSucceeded,
			fileKey:        "",
			isDownloadable: false,
		},
		{
			name:           "failed with file key is not downloadable",
			status:         entities.ExportJobStatusFailed,
			fileKey:        "some-file-key",
			isDownloadable: false,
		},
		{
			name:           "queued is not downloadable",
			status:         entities.ExportJobStatusQueued,
			fileKey:        "",
			isDownloadable: false,
		},
		{
			name:           "running is not downloadable",
			status:         entities.ExportJobStatusRunning,
			fileKey:        "",
			isDownloadable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job := &entities.ExportJob{Status: tt.status, FileKey: tt.fileKey}
			assert.Equal(t, tt.isDownloadable, job.IsDownloadable())
		})
	}
}

func TestExportJob_UpdateProgress(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := entities.NewExportJob(
		ctx,
		tenantID,
		contextID,
		entities.ExportReportTypeMatched,
		entities.ExportFormatCSV,
		filter,
	)
	require.NoError(t, err)

	originalUpdatedAt := job.UpdatedAt

	job.UpdateProgress(50, 2500)

	assert.Equal(t, int64(50), job.RecordsWritten)
	assert.Equal(t, int64(2500), job.BytesWritten)
	assert.False(t, job.UpdatedAt.IsZero(), "UpdatedAt should be set")
	assert.True(t, job.UpdatedAt.After(originalUpdatedAt) || job.UpdatedAt.Equal(originalUpdatedAt),
		"UpdatedAt should be equal to or after original")
}

func TestIsValidExportFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   entities.ExportFormat
		expected bool
	}{
		{name: "CSV is valid", format: entities.ExportFormatCSV, expected: true},
		{name: "JSON is valid", format: entities.ExportFormatJSON, expected: true},
		{name: "XML is valid", format: entities.ExportFormatXML, expected: true},
		{name: "PDF is valid", format: entities.ExportFormatPDF, expected: true},
		{name: "lowercase csv is invalid", format: "csv", expected: false},
		{name: "empty string is invalid", format: "", expected: false},
		{name: "unknown format is invalid", format: "XLSX", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, entities.IsValidExportFormat(tt.format))
		})
	}
}

func TestIsValidReportType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reportType entities.ExportReportType
		expected   bool
	}{
		{name: "MATCHED is valid", reportType: entities.ExportReportTypeMatched, expected: true},
		{
			name:       "UNMATCHED is valid",
			reportType: entities.ExportReportTypeUnmatched,
			expected:   true,
		},
		{name: "SUMMARY is valid", reportType: entities.ExportReportTypeSummary, expected: true},
		{name: "VARIANCE is valid", reportType: entities.ExportReportTypeVariance, expected: true},
		{name: "lowercase matched is invalid", reportType: "matched", expected: false},
		{name: "empty string is invalid", reportType: "", expected: false},
		{name: "unknown type is invalid", reportType: "CUSTOM", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, entities.IsValidReportType(tt.reportType))
		})
	}
}

func TestIsStreamableFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   entities.ExportFormat
		expected bool
	}{
		{name: "CSV is streamable", format: entities.ExportFormatCSV, expected: true},
		{name: "JSON is streamable", format: entities.ExportFormatJSON, expected: true},
		{name: "XML is streamable", format: entities.ExportFormatXML, expected: true},
		{name: "PDF is not streamable", format: entities.ExportFormatPDF, expected: false},
		{name: "unknown format is not streamable", format: "XLSX", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, entities.IsStreamableFormat(tt.format))
		})
	}
}

func TestGenerateFileName(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("12345678-1234-1234-1234-123456789012")
	dateFrom := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 3, 20, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		reportType entities.ExportReportType
		format     entities.ExportFormat
		expected   string
	}{
		{
			name:       "CSV format",
			reportType: entities.ExportReportTypeMatched,
			format:     entities.ExportFormatCSV,
			expected:   "MATCHED_12345678_20240115-20240320.csv",
		},
		{
			name:       "JSON format",
			reportType: entities.ExportReportTypeUnmatched,
			format:     entities.ExportFormatJSON,
			expected:   "UNMATCHED_12345678_20240115-20240320.json",
		},
		{
			name:       "XML format",
			reportType: entities.ExportReportTypeSummary,
			format:     entities.ExportFormatXML,
			expected:   "SUMMARY_12345678_20240115-20240320.xml",
		},
		{
			name:       "PDF format",
			reportType: entities.ExportReportTypeVariance,
			format:     entities.ExportFormatPDF,
			expected:   "VARIANCE_12345678_20240115-20240320.pdf",
		},
		{
			name:       "unknown format defaults to dat",
			reportType: entities.ExportReportTypeMatched,
			format:     "UNKNOWN",
			expected:   "MATCHED_12345678_20240115-20240320.dat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := entities.GenerateFileName(
				tt.reportType,
				tt.format,
				contextID,
				dateFrom,
				dateTo,
			)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportJob_DefaultExpiry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	beforeCreation := time.Now().UTC()
	job, err := entities.NewExportJob(
		ctx,
		tenantID,
		contextID,
		entities.ExportReportTypeMatched,
		entities.ExportFormatCSV,
		filter,
	)
	require.NoError(t, err)

	afterCreation := time.Now().UTC()

	expectedExpiryMin := beforeCreation.Add(entities.DefaultExportExpiry)
	expectedExpiryMax := afterCreation.Add(entities.DefaultExportExpiry)

	assert.True(t, job.ExpiresAt.After(expectedExpiryMin) || job.ExpiresAt.Equal(expectedExpiryMin))
	assert.True(
		t,
		job.ExpiresAt.Before(expectedExpiryMax) || job.ExpiresAt.Equal(expectedExpiryMax),
	)
}

func TestExportJob_InitialState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := entities.ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := entities.NewExportJob(
		ctx,
		tenantID,
		contextID,
		entities.ExportReportTypeMatched,
		entities.ExportFormatCSV,
		filter,
	)
	require.NoError(t, err)

	assert.NotEqual(t, uuid.Nil, job.ID)
	assert.Equal(t, tenantID, job.TenantID)
	assert.Equal(t, contextID, job.ContextID)
	assert.Equal(t, entities.ExportReportTypeMatched, job.ReportType)
	assert.Equal(t, entities.ExportFormatCSV, job.Format)
	assert.Equal(t, entities.ExportJobStatusQueued, job.Status)
	assert.Equal(t, int64(0), job.RecordsWritten)
	assert.Equal(t, int64(0), job.BytesWritten)
	assert.Empty(t, job.FileKey)
	assert.Empty(t, job.FileName)
	assert.Empty(t, job.SHA256)
	assert.Empty(t, job.Error)
	assert.Nil(t, job.StartedAt)
	assert.Nil(t, job.FinishedAt)
	assert.False(t, job.CreatedAt.IsZero())
	assert.False(t, job.ExpiresAt.IsZero())
	assert.False(t, job.UpdatedAt.IsZero())
}

func TestExportJobErrors(t *testing.T) {
	t.Parallel()

	require.Error(t, repositories.ErrExportJobNotFound)
	assert.Equal(t, "export job not found", repositories.ErrExportJobNotFound.Error())
}

func TestRepository_ScanExportJob_ErrNoRows(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{}))

	row := db.QueryRow("SELECT 1")
	job, err := scanExportJob(row)

	assert.Nil(t, job)
	require.ErrorIs(t, err, repositories.ErrExportJobNotFound)
}

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

func TestRepository_Create_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful create", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		job, err := entities.NewExportJob(
			ctx,
			uuid.New(),
			uuid.New(),
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			entities.ExportJobFilter{
				DateFrom: time.Now().UTC().Add(-24 * time.Hour),
				DateTo:   time.Now().UTC(),
			},
		)
		require.NoError(t, err)

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO export_jobs").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err = repo.Create(ctx, job)

		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, job.ID, "job ID should be set")
		assert.Equal(t, entities.ExportReportTypeMatched, job.ReportType, "report type should be preserved")
		assert.Equal(t, entities.ExportFormatCSV, job.Format, "format should be preserved")
		assert.Equal(t, entities.ExportJobStatusQueued, job.Status, "status should be queued")
		assert.False(t, job.CreatedAt.IsZero(), "CreatedAt should be set")
		assert.False(t, job.UpdatedAt.IsZero(), "UpdatedAt should be set")
	})
}

func TestRepository_GetByID_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("not found returns error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		jobID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

		job, err := repo.GetByID(ctx, jobID)

		assert.Nil(t, job)
		require.Error(t, err)
	})
}

func TestRepository_List_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("list with status filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		statusStr := string(entities.ExportJobStatusQueued)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows(exportJobColumns()))
		mock.ExpectCommit()

		jobs, _, err := repo.List(ctx, &statusStr, nil, 10)

		require.NoError(t, err)
		assert.Empty(t, jobs)
	})

	t.Run("list without status filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows(exportJobColumns()))
		mock.ExpectCommit()

		jobs, _, err := repo.List(ctx, nil, nil, 10)

		require.NoError(t, err)
		assert.Empty(t, jobs)
	})
}

func TestRepository_ListByContext_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("list by context", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		contextID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows(exportJobColumns()))
		mock.ExpectCommit()

		jobs, err := repo.ListByContext(ctx, contextID, 10)

		require.NoError(t, err)
		assert.Empty(t, jobs)
	})
}

func TestRepository_ListExpired_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("list expired jobs", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows(exportJobColumns()))
		mock.ExpectCommit()

		jobs, err := repo.ListExpired(ctx, 10)

		require.NoError(t, err)
		assert.Empty(t, jobs)
	})
}

func TestRepository_ClaimNextQueued_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("no queued jobs returns nil", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()

		mock.ExpectBegin()
		mock.ExpectQuery("UPDATE export_jobs").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

		job, err := repo.ClaimNextQueued(ctx)

		assert.Nil(t, job)
		require.NoError(t, err)
	})
}

func TestRepository_Delete_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful delete", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		jobID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM export_jobs").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err := repo.Delete(ctx, jobID)

		require.NoError(t, err)
	})
}

func TestRepository_Update_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("update not found returns error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		job, err := entities.NewExportJob(
			ctx,
			uuid.New(),
			uuid.New(),
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			entities.ExportJobFilter{
				DateFrom: time.Now().UTC().Add(-24 * time.Hour),
				DateTo:   time.Now().UTC(),
			},
		)
		require.NoError(t, err)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE export_jobs").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err = repo.Update(ctx, job)

		require.Error(t, err)
	})

	t.Run("successful update", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		job, err := entities.NewExportJob(
			ctx,
			uuid.New(),
			uuid.New(),
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			entities.ExportJobFilter{
				DateFrom: time.Now().UTC().Add(-24 * time.Hour),
				DateTo:   time.Now().UTC(),
			},
		)
		require.NoError(t, err)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE export_jobs").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err = repo.Update(ctx, job)

		require.NoError(t, err)
	})
}

func TestRepository_UpdateProgress_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful progress update", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		jobID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE export_jobs").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err := repo.UpdateProgress(ctx, jobID, 100, 5000)

		require.NoError(t, err)
	})
}

func TestRepository_RequeueForRetry_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful requeue", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		job, err := entities.NewExportJob(
			ctx,
			uuid.New(),
			uuid.New(),
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			entities.ExportJobFilter{
				DateFrom: time.Now().UTC().Add(-24 * time.Hour),
				DateTo:   time.Now().UTC(),
			},
		)
		require.NoError(t, err)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE export_jobs").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err = repo.RequeueForRetry(ctx, job)

		require.NoError(t, err)
	})

	t.Run("requeue not found returns error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupRepository(t)
		defer finish()

		ctx := context.Background()
		job, err := entities.NewExportJob(
			ctx,
			uuid.New(),
			uuid.New(),
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			entities.ExportJobFilter{
				DateFrom: time.Now().UTC().Add(-24 * time.Hour),
				DateTo:   time.Now().UTC(),
			},
		)
		require.NoError(t, err)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE export_jobs").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err = repo.RequeueForRetry(ctx, job)

		require.Error(t, err)
	})
}
