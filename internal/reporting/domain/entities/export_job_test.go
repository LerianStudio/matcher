//go:build unit

package entities

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedtestutil "github.com/LerianStudio/matcher/internal/shared/testutil"
)

// mockClock creates a clock that returns the given time.
func mockClock(t time.Time) Clock {
	return func() time.Time { return t }
}

func TestNewExportJob_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := NewExportJob(
		ctx,
		tenantID,
		contextID,
		ExportReportTypeMatched,
		ExportFormatCSV,
		filter,
	)

	require.NoError(t, err)
	assert.NotNil(t, job)
	assert.NotEqual(t, uuid.Nil, job.ID)
	assert.Equal(t, tenantID, job.TenantID)
	assert.Equal(t, contextID, job.ContextID)
	assert.Equal(t, ExportReportTypeMatched, job.ReportType)
	assert.Equal(t, ExportFormatCSV, job.Format)
	assert.Equal(t, ExportJobStatusQueued, job.Status)
	assert.Equal(t, filter, job.Filter)
}

func TestNewExportJob_InvalidFormat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := NewExportJob(ctx, tenantID, contextID, ExportReportTypeMatched, "INVALID", filter)

	assert.Nil(t, job)
	require.Error(t, err)
}

func TestNewExportJob_InvalidReportType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := NewExportJob(ctx, tenantID, contextID, "INVALID", ExportFormatCSV, filter)

	assert.Nil(t, job)
	require.Error(t, err)
}

func TestNewExportJob_AllFormats(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	formats := []string{ExportFormatCSV, ExportFormatJSON, ExportFormatXML, ExportFormatPDF}

	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			t.Parallel()

			tenantID := uuid.New()
			contextID := uuid.New()
			filter := ExportJobFilter{
				DateFrom: time.Now().UTC().Add(-24 * time.Hour),
				DateTo:   time.Now().UTC(),
			}

			job, err := NewExportJob(
				ctx,
				tenantID,
				contextID,
				ExportReportTypeMatched,
				format,
				filter,
			)

			require.NoError(t, err)
			assert.Equal(t, format, job.Format)
		})
	}
}

func TestNewExportJob_AllReportTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reportTypes := []string{
		ExportReportTypeMatched,
		ExportReportTypeUnmatched,
		ExportReportTypeSummary,
		ExportReportTypeVariance,
	}

	for _, reportType := range reportTypes {
		t.Run(reportType, func(t *testing.T) {
			t.Parallel()

			tenantID := uuid.New()
			contextID := uuid.New()
			filter := ExportJobFilter{
				DateFrom: time.Now().UTC().Add(-24 * time.Hour),
				DateTo:   time.Now().UTC(),
			}

			job, err := NewExportJob(ctx, tenantID, contextID, reportType, ExportFormatCSV, filter)

			require.NoError(t, err)
			assert.Equal(t, reportType, job.ReportType)
		})
	}
}

func TestExportJob_MarkRunning(t *testing.T) {
	t.Parallel()

	fixed := sharedtestutil.FixedTime()
	job := createTestExportJobWithClock(t, mockClock(fixed))
	job.MarkRunning()

	assert.Equal(t, ExportJobStatusRunning, job.Status)
	require.NotNil(t, job.StartedAt)
	assert.Equal(t, fixed, *job.StartedAt)
	assert.Equal(t, fixed, job.UpdatedAt)
	assert.Equal(t, 1, job.Attempts)
}

func TestExportJob_MarkRunning_IncrementsAttempts(t *testing.T) {
	t.Parallel()

	fixed := sharedtestutil.FixedTime()
	job := createTestExportJobWithClock(t, mockClock(fixed))

	assert.Equal(t, 0, job.Attempts)

	job.MarkRunning()
	assert.Equal(t, 1, job.Attempts)

	job.Status = ExportJobStatusQueued
	job.MarkRunning()
	assert.Equal(t, 2, job.Attempts)

	job.Status = ExportJobStatusQueued
	job.MarkRunning()
	assert.Equal(t, 3, job.Attempts)
}

func TestExportJob_MarkSucceeded(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	finishedAt := startedAt.Add(2 * time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	job.MarkRunning()

	fileKey := "exports/test-file.csv"
	fileName := "test-file.csv"
	sha256 := "abc123hash"
	recordsWritten := int64(100)
	bytesWritten := int64(5000)

	// Advance the clock for the finish time
	job.SetClock(mockClock(finishedAt))
	job.MarkSucceeded(fileKey, fileName, sha256, recordsWritten, bytesWritten)

	assert.Equal(t, ExportJobStatusSucceeded, job.Status)
	assert.Equal(t, fileKey, job.FileKey)
	assert.Equal(t, fileName, job.FileName)
	assert.Equal(t, sha256, job.SHA256)
	assert.Equal(t, recordsWritten, job.RecordsWritten)
	assert.Equal(t, bytesWritten, job.BytesWritten)
	require.NotNil(t, job.FinishedAt)
	assert.Equal(t, finishedAt, *job.FinishedAt)
	assert.Equal(t, finishedAt, job.UpdatedAt)
}

func TestExportJob_MarkFailed(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	failedAt := startedAt.Add(time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	job.MarkRunning()

	errMsg := "connection timeout"

	// Advance the clock for the failure time
	job.SetClock(mockClock(failedAt))
	job.MarkFailed(errMsg)

	assert.Equal(t, ExportJobStatusFailed, job.Status)
	assert.Equal(t, errMsg, job.Error)
	require.NotNil(t, job.FinishedAt)
	assert.Equal(t, failedAt, *job.FinishedAt)
	assert.Equal(t, failedAt, job.UpdatedAt)
}

func TestExportJob_MarkExpired(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	succeededAt := startedAt.Add(time.Minute)
	expiredAt := startedAt.Add(2 * time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	job.MarkRunning()

	// Advance clock for succeeded
	job.SetClock(mockClock(succeededAt))
	job.MarkSucceeded("key", "file", "hash", 10, 100)

	// Advance clock for expired
	job.SetClock(mockClock(expiredAt))
	job.MarkExpired()

	assert.Equal(t, ExportJobStatusExpired, job.Status)
	assert.Equal(t, expiredAt, job.UpdatedAt)
}

func TestExportJob_MarkCanceled(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	canceledAt := startedAt.Add(time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	job.MarkRunning()

	// Advance clock for canceled
	job.SetClock(mockClock(canceledAt))
	job.MarkCanceled()

	assert.Equal(t, ExportJobStatusCanceled, job.Status)
	require.NotNil(t, job.FinishedAt)
	assert.Equal(t, canceledAt, *job.FinishedAt)
	assert.Equal(t, canceledAt, job.UpdatedAt)
}

func TestExportJob_MarkForRetry(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	retryScheduledAt := startedAt.Add(time.Minute)
	nextRetryAt := startedAt.Add(5 * time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	job.MarkRunning()

	job.SetClock(mockClock(retryScheduledAt))

	errMsg := "temporary failure"

	job.MarkForRetry(errMsg, nextRetryAt)

	assert.Equal(t, ExportJobStatusQueued, job.Status)
	assert.Equal(t, errMsg, job.Error)
	require.NotNil(t, job.NextRetryAt)
	assert.Equal(t, nextRetryAt, *job.NextRetryAt)
	assert.Equal(t, retryScheduledAt, job.UpdatedAt)
	assert.Nil(t, job.FinishedAt)
}

func TestExportJob_UpdateProgress(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	progressAt := startedAt.Add(time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	job.MarkRunning()

	// Advance clock for progress update
	job.SetClock(mockClock(progressAt))
	job.UpdateProgress(50, 2500)

	assert.Equal(t, int64(50), job.RecordsWritten)
	assert.Equal(t, int64(2500), job.BytesWritten)
	assert.Equal(t, progressAt, job.UpdatedAt)
}

func TestExportJob_IsTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		isTerminal bool
	}{
		{name: "queued is not terminal", status: ExportJobStatusQueued, isTerminal: false},
		{name: "running is not terminal", status: ExportJobStatusRunning, isTerminal: false},
		{name: "succeeded is terminal", status: ExportJobStatusSucceeded, isTerminal: true},
		{name: "failed is terminal", status: ExportJobStatusFailed, isTerminal: true},
		{name: "expired is terminal", status: ExportJobStatusExpired, isTerminal: true},
		{name: "canceled is terminal", status: ExportJobStatusCanceled, isTerminal: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job := &ExportJob{Status: tt.status}
			assert.Equal(t, tt.isTerminal, job.IsTerminal())
		})
	}
}

func TestExportJob_IsDownloadable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		status         string
		fileKey        string
		isDownloadable bool
	}{
		{
			name:           "succeeded with file key is downloadable",
			status:         ExportJobStatusSucceeded,
			fileKey:        "key",
			isDownloadable: true,
		},
		{
			name:           "succeeded without file key is not downloadable",
			status:         ExportJobStatusSucceeded,
			fileKey:        "",
			isDownloadable: false,
		},
		{
			name:           "failed with file key is not downloadable",
			status:         ExportJobStatusFailed,
			fileKey:        "key",
			isDownloadable: false,
		},
		{
			name:           "queued is not downloadable",
			status:         ExportJobStatusQueued,
			fileKey:        "",
			isDownloadable: false,
		},
		{
			name:           "running is not downloadable",
			status:         ExportJobStatusRunning,
			fileKey:        "",
			isDownloadable: false,
		},
		{
			name:           "expired is not downloadable",
			status:         ExportJobStatusExpired,
			fileKey:        "key",
			isDownloadable: false,
		},
		{
			name:           "canceled is not downloadable",
			status:         ExportJobStatusCanceled,
			fileKey:        "key",
			isDownloadable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job := &ExportJob{Status: tt.status, FileKey: tt.fileKey}
			assert.Equal(t, tt.isDownloadable, job.IsDownloadable())
		})
	}
}

func TestIsValidExportFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   string
		expected bool
	}{
		{name: "CSV is valid", format: ExportFormatCSV, expected: true},
		{name: "JSON is valid", format: ExportFormatJSON, expected: true},
		{name: "XML is valid", format: ExportFormatXML, expected: true},
		{name: "PDF is valid", format: ExportFormatPDF, expected: true},
		{name: "lowercase csv is invalid", format: "csv", expected: false},
		{name: "empty string is invalid", format: "", expected: false},
		{name: "unknown format is invalid", format: "XLSX", expected: false},
		{name: "whitespace is invalid", format: " ", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, IsValidExportFormat(tt.format))
		})
	}
}

func TestIsValidReportType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reportType string
		expected   bool
	}{
		{name: "MATCHED is valid", reportType: ExportReportTypeMatched, expected: true},
		{name: "UNMATCHED is valid", reportType: ExportReportTypeUnmatched, expected: true},
		{name: "SUMMARY is valid", reportType: ExportReportTypeSummary, expected: true},
		{name: "VARIANCE is valid", reportType: ExportReportTypeVariance, expected: true},
		{name: "lowercase matched is invalid", reportType: "matched", expected: false},
		{name: "empty string is invalid", reportType: "", expected: false},
		{name: "unknown type is invalid", reportType: "CUSTOM", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, IsValidReportType(tt.reportType))
		})
	}
}

func TestIsStreamableFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   string
		expected bool
	}{
		{name: "CSV is streamable", format: ExportFormatCSV, expected: true},
		{name: "JSON is streamable", format: ExportFormatJSON, expected: true},
		{name: "XML is streamable", format: ExportFormatXML, expected: true},
		{name: "PDF is not streamable", format: ExportFormatPDF, expected: false},
		{name: "unknown format is not streamable", format: "XLSX", expected: false},
		{name: "empty is not streamable", format: "", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, IsStreamableFormat(tt.format))
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
		reportType string
		format     string
		expected   string
	}{
		{
			name:       "CSV format",
			reportType: ExportReportTypeMatched,
			format:     ExportFormatCSV,
			expected:   "MATCHED_12345678_20240115-20240320.csv",
		},
		{
			name:       "JSON format",
			reportType: ExportReportTypeUnmatched,
			format:     ExportFormatJSON,
			expected:   "UNMATCHED_12345678_20240115-20240320.json",
		},
		{
			name:       "XML format",
			reportType: ExportReportTypeSummary,
			format:     ExportFormatXML,
			expected:   "SUMMARY_12345678_20240115-20240320.xml",
		},
		{
			name:       "PDF format",
			reportType: ExportReportTypeVariance,
			format:     ExportFormatPDF,
			expected:   "VARIANCE_12345678_20240115-20240320.pdf",
		},
		{
			name:       "unknown format defaults to dat",
			reportType: ExportReportTypeMatched,
			format:     "UNKNOWN",
			expected:   "MATCHED_12345678_20240115-20240320.dat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := GenerateFileName(tt.reportType, tt.format, contextID, dateFrom, dateTo)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportJobFilter_ToJSON(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	status := ExportJobStatusQueued
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	filter := ExportJobFilter{
		DateFrom: dateFrom,
		DateTo:   dateTo,
		SourceID: &sourceID,
		Status:   &status,
	}

	data, err := filter.ToJSON()

	require.NoError(t, err)
	assert.NotEmpty(t, data)

	var decoded ExportJobFilter

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, filter.DateFrom, decoded.DateFrom)
	assert.Equal(t, filter.DateTo, decoded.DateTo)
	assert.Equal(t, *filter.SourceID, *decoded.SourceID)
	assert.Equal(t, *filter.Status, *decoded.Status)
}

func TestExportJobFilter_ToJSON_Minimal(t *testing.T) {
	t.Parallel()

	filter := ExportJobFilter{
		DateFrom: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateTo:   time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	data, err := filter.ToJSON()

	require.NoError(t, err)
	assert.NotEmpty(t, data)
	assert.NotContains(t, string(data), "source_id")
	assert.NotContains(t, string(data), "status")
}

func TestExportJobFilterFromJSON_Success(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	status := ExportJobStatusQueued
	original := ExportJobFilter{
		DateFrom: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateTo:   time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		SourceID: &sourceID,
		Status:   &status,
	}

	data, err := original.ToJSON()
	require.NoError(t, err)

	decoded, err := ExportJobFilterFromJSON(data)

	require.NoError(t, err)
	assert.Equal(t, original.DateFrom, decoded.DateFrom)
	assert.Equal(t, original.DateTo, decoded.DateTo)
}

func TestExportJobFilterFromJSON_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, err := ExportJobFilterFromJSON([]byte("not valid json"))

	require.Error(t, err)
}

func TestExportJobFilterFromJSON_EmptyJSON(t *testing.T) {
	t.Parallel()

	filter, err := ExportJobFilterFromJSON([]byte("{}"))

	require.NoError(t, err)
	assert.True(t, filter.DateFrom.IsZero())
	assert.True(t, filter.DateTo.IsZero())
	assert.Nil(t, filter.SourceID)
	assert.Nil(t, filter.Status)
}

func TestExportJob_DefaultExpiry(t *testing.T) {
	t.Parallel()

	beforeCreation := time.Now().UTC()
	job := createTestExportJob(t)
	afterCreation := time.Now().UTC()

	expectedExpiryMin := beforeCreation.Add(DefaultExportExpiry)
	expectedExpiryMax := afterCreation.Add(DefaultExportExpiry)

	assert.True(t, job.ExpiresAt.After(expectedExpiryMin) || job.ExpiresAt.Equal(expectedExpiryMin))
	assert.True(
		t,
		job.ExpiresAt.Before(expectedExpiryMax) || job.ExpiresAt.Equal(expectedExpiryMax),
	)
}

func TestExportJob_StateTransitions(t *testing.T) {
	t.Parallel()

	t.Run("queued -> running -> succeeded", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		assert.Equal(t, ExportJobStatusQueued, job.Status)
		assert.False(t, job.IsTerminal())

		job.MarkRunning()
		assert.Equal(t, ExportJobStatusRunning, job.Status)
		assert.False(t, job.IsTerminal())

		job.MarkSucceeded("key", "file", "hash", 100, 5000)
		assert.Equal(t, ExportJobStatusSucceeded, job.Status)
		assert.True(t, job.IsTerminal())
		assert.True(t, job.IsDownloadable())
	})

	t.Run("queued -> running -> failed", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)

		job.MarkRunning()
		job.MarkFailed("error occurred")

		assert.Equal(t, ExportJobStatusFailed, job.Status)
		assert.True(t, job.IsTerminal())
		assert.False(t, job.IsDownloadable())
	})

	t.Run("queued -> canceled", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)

		job.MarkCanceled()

		assert.Equal(t, ExportJobStatusCanceled, job.Status)
		assert.True(t, job.IsTerminal())
	})

	t.Run("succeeded -> expired", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		job.MarkRunning()
		job.MarkSucceeded("key", "file", "hash", 100, 5000)

		job.MarkExpired()

		assert.Equal(t, ExportJobStatusExpired, job.Status)
		assert.True(t, job.IsTerminal())
		assert.False(t, job.IsDownloadable())
	})
}

func TestDefaultExportExpiry(t *testing.T) {
	t.Parallel()

	expected := 7 * 24 * time.Hour
	assert.Equal(t, expected, DefaultExportExpiry)
}

func TestExportJobConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "QUEUED", ExportJobStatusQueued)
	assert.Equal(t, "RUNNING", ExportJobStatusRunning)
	assert.Equal(t, "SUCCEEDED", ExportJobStatusSucceeded)
	assert.Equal(t, "FAILED", ExportJobStatusFailed)
	assert.Equal(t, "EXPIRED", ExportJobStatusExpired)
	assert.Equal(t, "CANCELED", ExportJobStatusCanceled)

	assert.Equal(t, "CSV", ExportFormatCSV)
	assert.Equal(t, "JSON", ExportFormatJSON)
	assert.Equal(t, "XML", ExportFormatXML)
	assert.Equal(t, "PDF", ExportFormatPDF)

	assert.Equal(t, "MATCHED", ExportReportTypeMatched)
	assert.Equal(t, "UNMATCHED", ExportReportTypeUnmatched)
	assert.Equal(t, "SUMMARY", ExportReportTypeSummary)
	assert.Equal(t, "VARIANCE", ExportReportTypeVariance)
}

func createTestExportJob(t *testing.T) *ExportJob {
	t.Helper()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	filter := ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := NewExportJob(
		ctx,
		tenantID,
		contextID,
		ExportReportTypeMatched,
		ExportFormatCSV,
		filter,
	)
	require.NoError(t, err)

	return job
}

func createTestExportJobWithClock(t *testing.T, clock Clock) *ExportJob {
	t.Helper()

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	baseTime := clock()
	filter := ExportJobFilter{
		DateFrom: baseTime.Add(-24 * time.Hour),
		DateTo:   baseTime,
	}

	job, err := NewExportJob(
		ctx,
		tenantID,
		contextID,
		ExportReportTypeMatched,
		ExportFormatCSV,
		filter,
		WithClock(clock),
	)
	require.NoError(t, err)

	return job
}
