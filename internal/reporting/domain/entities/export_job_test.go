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

func TestNewExportJob_NilTenantID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	filter := ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := NewExportJob(ctx, uuid.Nil, contextID, ExportReportTypeMatched, ExportFormatCSV, filter)

	assert.Nil(t, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "export job tenant id")
}

func TestNewExportJob_NilContextID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	filter := ExportJobFilter{
		DateFrom: time.Now().UTC().Add(-24 * time.Hour),
		DateTo:   time.Now().UTC(),
	}

	job, err := NewExportJob(ctx, tenantID, uuid.Nil, ExportReportTypeMatched, ExportFormatCSV, filter)

	assert.Nil(t, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "export job context id")
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
	formats := []ExportFormat{ExportFormatCSV, ExportFormatJSON, ExportFormatXML, ExportFormatPDF}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
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
	reportTypes := []ExportReportType{
		ExportReportTypeMatched,
		ExportReportTypeUnmatched,
		ExportReportTypeSummary,
		ExportReportTypeVariance,
		ExportReportTypeExceptions,
	}

	for _, reportType := range reportTypes {
		t.Run(string(reportType), func(t *testing.T) {
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
	require.NoError(t, job.MarkRunning())

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

	require.NoError(t, job.MarkRunning())
	assert.Equal(t, 1, job.Attempts)

	// Simulate retry: running -> queued -> running
	require.NoError(t, job.MarkForRetry("retry", fixed.Add(time.Minute)))
	require.NoError(t, job.MarkRunning())
	assert.Equal(t, 2, job.Attempts)

	require.NoError(t, job.MarkForRetry("retry", fixed.Add(2*time.Minute)))
	require.NoError(t, job.MarkRunning())
	assert.Equal(t, 3, job.Attempts)
}

func TestExportJob_MarkSucceeded(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	finishedAt := startedAt.Add(2 * time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	require.NoError(t, job.MarkRunning())

	fileKey := "exports/test-file.csv"
	fileName := "test-file.csv"
	sha256 := "abc123hash"
	recordsWritten := int64(100)
	bytesWritten := int64(5000)

	// Advance the clock for the finish time
	job.SetClock(mockClock(finishedAt))
	require.NoError(t, job.MarkSucceeded(fileKey, fileName, sha256, recordsWritten, bytesWritten))

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
	require.NoError(t, job.MarkRunning())

	errMsg := "connection timeout"

	// Advance the clock for the failure time
	job.SetClock(mockClock(failedAt))
	require.NoError(t, job.MarkFailed(errMsg))

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
	require.NoError(t, job.MarkRunning())

	// Advance clock for succeeded
	job.SetClock(mockClock(succeededAt))
	require.NoError(t, job.MarkSucceeded("key", "file", "hash", 10, 100))

	// Advance clock for expired
	job.SetClock(mockClock(expiredAt))
	require.NoError(t, job.MarkExpired())

	assert.Equal(t, ExportJobStatusExpired, job.Status)
	assert.Equal(t, expiredAt, job.UpdatedAt)
}

func TestExportJob_MarkCanceled(t *testing.T) {
	t.Parallel()

	startedAt := sharedtestutil.FixedTime()
	canceledAt := startedAt.Add(time.Minute)

	job := createTestExportJobWithClock(t, mockClock(startedAt))
	require.NoError(t, job.MarkRunning())

	// Advance clock for canceled
	job.SetClock(mockClock(canceledAt))
	require.NoError(t, job.MarkCanceled())

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
	require.NoError(t, job.MarkRunning())

	job.SetClock(mockClock(retryScheduledAt))

	errMsg := "temporary failure"

	require.NoError(t, job.MarkForRetry(errMsg, nextRetryAt))

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
	require.NoError(t, job.MarkRunning())

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
		status     ExportJobStatus
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
		status         ExportJobStatus
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

func TestExportJobStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status ExportJobStatus
		want   bool
	}{
		{name: "QUEUED is valid", status: ExportJobStatusQueued, want: true},
		{name: "RUNNING is valid", status: ExportJobStatusRunning, want: true},
		{name: "SUCCEEDED is valid", status: ExportJobStatusSucceeded, want: true},
		{name: "FAILED is valid", status: ExportJobStatusFailed, want: true},
		{name: "EXPIRED is valid", status: ExportJobStatusExpired, want: true},
		{name: "CANCELED is valid", status: ExportJobStatusCanceled, want: true},
		{name: "empty string is invalid", status: ExportJobStatus(""), want: false},
		{name: "lowercase queued is invalid", status: ExportJobStatus("queued"), want: false},
		{name: "arbitrary string is invalid", status: ExportJobStatus("UNKNOWN"), want: false},
		{name: "partial match is invalid", status: ExportJobStatus("QUEUE"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.status.IsValid())
		})
	}
}

func TestExportReportType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reportType ExportReportType
		want       bool
	}{
		{name: "MATCHED is valid", reportType: ExportReportTypeMatched, want: true},
		{name: "UNMATCHED is valid", reportType: ExportReportTypeUnmatched, want: true},
		{name: "SUMMARY is valid", reportType: ExportReportTypeSummary, want: true},
		{name: "VARIANCE is valid", reportType: ExportReportTypeVariance, want: true},
		{name: "EXCEPTIONS is valid", reportType: ExportReportTypeExceptions, want: true},
		{name: "empty string is invalid", reportType: ExportReportType(""), want: false},
		{name: "lowercase matched is invalid", reportType: ExportReportType("matched"), want: false},
		{name: "arbitrary string is invalid", reportType: ExportReportType("CUSTOM"), want: false},
		{name: "partial match is invalid", reportType: ExportReportType("MATCH"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.reportType.IsValid())
		})
	}
}

func TestIsStreamableFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   ExportFormat
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
		reportType ExportReportType
		format     ExportFormat
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
			name:       "exceptions format",
			reportType: ExportReportTypeExceptions,
			format:     ExportFormatJSON,
			expected:   "EXCEPTIONS_12345678_20240115-20240320.json",
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

func TestExportJobFilterFromJSON_InvalidStatus(t *testing.T) {
	t.Parallel()

	_, err := ExportJobFilterFromJSON([]byte(`{"status":"NOPE"}`))

	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidExportJobStatus)
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

		require.NoError(t, job.MarkRunning())
		assert.Equal(t, ExportJobStatusRunning, job.Status)
		assert.False(t, job.IsTerminal())

		require.NoError(t, job.MarkSucceeded("key", "file", "hash", 100, 5000))
		assert.Equal(t, ExportJobStatusSucceeded, job.Status)
		assert.True(t, job.IsTerminal())
		assert.True(t, job.IsDownloadable())
	})

	t.Run("queued -> running -> failed", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)

		require.NoError(t, job.MarkRunning())
		require.NoError(t, job.MarkFailed("error occurred"))

		assert.Equal(t, ExportJobStatusFailed, job.Status)
		assert.True(t, job.IsTerminal())
		assert.False(t, job.IsDownloadable())
	})

	t.Run("queued -> canceled", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)

		require.NoError(t, job.MarkCanceled())

		assert.Equal(t, ExportJobStatusCanceled, job.Status)
		assert.True(t, job.IsTerminal())
	})

	t.Run("succeeded -> expired", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkRunning())
		require.NoError(t, job.MarkSucceeded("key", "file", "hash", 100, 5000))

		require.NoError(t, job.MarkExpired())

		assert.Equal(t, ExportJobStatusExpired, job.Status)
		assert.True(t, job.IsTerminal())
		assert.False(t, job.IsDownloadable())
	})

	t.Run("running -> retry (queued) -> running again", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkRunning())
		require.NoError(t, job.MarkForRetry("temp error", time.Now().UTC().Add(time.Minute)))
		assert.Equal(t, ExportJobStatusQueued, job.Status)

		require.NoError(t, job.MarkRunning())
		assert.Equal(t, ExportJobStatusRunning, job.Status)
	})

	t.Run("failed -> retry (queued) -> running", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkRunning())
		require.NoError(t, job.MarkFailed("first attempt"))

		// Failed jobs can be retried (queued again)
		require.NoError(t, job.MarkForRetry("retry", time.Now().UTC().Add(time.Minute)))
		assert.Equal(t, ExportJobStatusQueued, job.Status)

		require.NoError(t, job.MarkRunning())
		assert.Equal(t, ExportJobStatusRunning, job.Status)
	})
}

func TestExportJob_InvalidStateTransitions(t *testing.T) {
	t.Parallel()

	t.Run("queued -> succeeded is invalid", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		err := job.MarkSucceeded("key", "file", "hash", 100, 5000)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidExportJobTransition)
		assert.Equal(t, ExportJobStatusQueued, job.Status) // unchanged
	})

	t.Run("canceled -> retry is invalid", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkCanceled())

		err := job.MarkForRetry("retry", time.Now().UTC().Add(time.Minute))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidExportJobTransition)
	})

	t.Run("succeeded -> running is invalid", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkRunning())
		require.NoError(t, job.MarkSucceeded("key", "file", "hash", 100, 5000))

		err := job.MarkRunning()
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidExportJobTransition)
	})

	t.Run("canceled -> running is invalid", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkCanceled())

		err := job.MarkRunning()
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidExportJobTransition)
	})

	t.Run("expired -> running is invalid", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkRunning())
		require.NoError(t, job.MarkSucceeded("key", "file", "hash", 100, 5000))
		require.NoError(t, job.MarkExpired())

		err := job.MarkRunning()
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidExportJobTransition)
	})

	t.Run("failed -> failed is invalid", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		require.NoError(t, job.MarkRunning())
		require.NoError(t, job.MarkFailed("first error"))

		err := job.MarkFailed("second error")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidExportJobTransition)
		assert.Equal(t, "first error", job.Error) // unchanged
	})

	t.Run("error message includes transition details", func(t *testing.T) {
		t.Parallel()

		job := createTestExportJob(t)
		err := job.MarkSucceeded("key", "file", "hash", 100, 5000)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "QUEUED")
		assert.Contains(t, err.Error(), "SUCCEEDED")
	})
}

func TestExportJob_NilReceiverGuards(t *testing.T) {
	t.Parallel()

	var job *ExportJob

	nextRetry := time.Now().UTC().Add(time.Minute)

	// Nil receiver should return nil error (no panic, no error)
	require.NoError(t, job.MarkRunning())
	require.NoError(t, job.MarkSucceeded("k", "f", "s", 1, 1))
	require.NoError(t, job.MarkFailed("err"))
	require.NoError(t, job.MarkForRetry("err", nextRetry))
	require.NoError(t, job.MarkExpired())
	require.NoError(t, job.MarkCanceled())
	require.NotPanics(t, func() { job.UpdateProgress(1, 1) })
	require.NotPanics(t, func() { job.SetClock(DefaultClock) })

	assert.False(t, job.IsTerminal())
	assert.False(t, job.IsDownloadable())
}

func TestDefaultExportExpiry(t *testing.T) {
	t.Parallel()

	expected := 7 * 24 * time.Hour
	assert.Equal(t, expected, DefaultExportExpiry)
}

func TestExportJobConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, ExportJobStatus("QUEUED"), ExportJobStatusQueued)
	assert.Equal(t, ExportJobStatus("RUNNING"), ExportJobStatusRunning)
	assert.Equal(t, ExportJobStatus("SUCCEEDED"), ExportJobStatusSucceeded)
	assert.Equal(t, ExportJobStatus("FAILED"), ExportJobStatusFailed)
	assert.Equal(t, ExportJobStatus("EXPIRED"), ExportJobStatusExpired)
	assert.Equal(t, ExportJobStatus("CANCELED"), ExportJobStatusCanceled)

	assert.Equal(t, ExportFormat("CSV"), ExportFormatCSV)
	assert.Equal(t, ExportFormat("JSON"), ExportFormatJSON)
	assert.Equal(t, ExportFormat("XML"), ExportFormatXML)
	assert.Equal(t, ExportFormat("PDF"), ExportFormatPDF)

	assert.Equal(t, ExportReportType("MATCHED"), ExportReportTypeMatched)
	assert.Equal(t, ExportReportType("UNMATCHED"), ExportReportTypeUnmatched)
	assert.Equal(t, ExportReportType("SUMMARY"), ExportReportTypeSummary)
	assert.Equal(t, ExportReportType("VARIANCE"), ExportReportTypeVariance)
	assert.Equal(t, ExportReportType("EXCEPTIONS"), ExportReportTypeExceptions)
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
