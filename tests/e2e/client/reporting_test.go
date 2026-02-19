//go:build e2e

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReportingClient(t *testing.T) {
	t.Parallel()

	baseClient := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)
	reportingClient := NewReportingClient(baseClient)

	assert.NotNil(t, reportingClient)
	assert.Equal(t, baseClient, reportingClient.client)
}

func TestReportingClient_GetDashboardAggregates(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/dashboard", r.URL.Path)
		assert.Equal(t, "2024-01-01", r.URL.Query().Get("date_from"))
		assert.Equal(t, "2024-12-31", r.URL.Query().Get("date_to"))
		assert.Equal(t, http.MethodGet, r.Method)

		resp := DashboardAggregates{
			Volume: &VolumeStatsResponse{
				TotalTransactions:   1000,
				MatchedTransactions: 900,
				UnmatchedCount:      100,
				TotalAmount:         "1000000.00",
				MatchedAmount:       "900000.00",
				UnmatchedAmount:     "100000.00",
			},
			MatchRate: &MatchRateStatsResponse{
				MatchRate:      0.90,
				TotalCount:     1000,
				MatchedCount:   900,
				UnmatchedCount: 100,
			},
			SLA: &SLAStatsResponse{
				TotalExceptions:   10,
				SLAComplianceRate: 0.95,
			},
			UpdatedAt: "2024-01-01T00:00:00Z",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetDashboardAggregates(
		context.Background(),
		"ctx-123",
		"2024-01-01",
		"2024-12-31",
	)

	require.NoError(t, err)
	require.NotNil(t, result.Volume)
	assert.Equal(t, 1000, result.Volume.TotalTransactions)
	require.NotNil(t, result.MatchRate)
	assert.Equal(t, 0.90, result.MatchRate.MatchRate)
}

func TestReportingClient_GetDashboardMetrics(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/dashboard/metrics", r.URL.Path)
		assert.Equal(t, "2026-01-01", r.URL.Query().Get("date_from"))
		assert.Equal(t, "2026-01-31", r.URL.Query().Get("date_to"))

		resp := DashboardMetricsResponse{
			Summary:   map[string]any{"total": 500, "matched": 450},
			UpdatedAt: "2026-01-15T00:00:00Z",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetDashboardMetrics(
		context.Background(),
		"ctx-123",
		"2026-01-01",
		"2026-01-31",
	)

	require.NoError(t, err)
	require.NotNil(t, result.Summary)
	assert.Equal(t, float64(500), result.Summary["total"])
	assert.Equal(t, float64(450), result.Summary["matched"])
	assert.Equal(t, "2026-01-15T00:00:00Z", result.UpdatedAt)
}

func TestReportingClient_GetVolumeStats(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/dashboard/volume", r.URL.Path)
		assert.Equal(t, "2024-01-01", r.URL.Query().Get("date_from"))
		assert.Equal(t, "2024-12-31", r.URL.Query().Get("date_to"))

		resp := VolumeStats{
			Period:        "2024-01",
			TotalVolume:   "1000000.00",
			MatchedVolume: "900000.00",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetVolumeStats(
		context.Background(),
		"ctx-123",
		"2024-01-01",
		"2024-12-31",
	)

	require.NoError(t, err)
	assert.Equal(t, "2024-01", result.Period)
}

func TestReportingClient_GetMatchRateStats(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/dashboard/match-rate", r.URL.Path)
		assert.Equal(t, "2024-01-01", r.URL.Query().Get("date_from"))
		assert.Equal(t, "2024-12-31", r.URL.Query().Get("date_to"))

		resp := MatchRateStats{
			Period:    "2024-01",
			MatchRate: 0.95,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetMatchRateStats(
		context.Background(),
		"ctx-123",
		"2024-01-01",
		"2024-12-31",
	)

	require.NoError(t, err)
	assert.Equal(t, 0.95, result.MatchRate)
}

func TestReportingClient_GetSLAStats(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/dashboard/sla", r.URL.Path)
		assert.Equal(t, "2024-01-01", r.URL.Query().Get("date_from"))
		assert.Equal(t, "2024-12-31", r.URL.Query().Get("date_to"))

		resp := SLAStats{
			AverageMatchTime:  "2h30m",
			SLAComplianceRate: 0.98,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetSLAStats(context.Background(), "ctx-123", "2024-01-01", "2024-12-31")

	require.NoError(t, err)
	assert.Equal(t, "2h30m", result.AverageMatchTime)
	assert.Equal(t, 0.98, result.SLAComplianceRate)
}

func TestReportingClient_ExportMatchedReport(t *testing.T) {
	t.Parallel()

	csvData := "id,amount,status\n1,100.00,matched\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/matched/export", r.URL.Path)
		assert.Equal(t, "2026-01-01", r.URL.Query().Get("date_from"))
		assert.Equal(t, "2026-01-31", r.URL.Query().Get("date_to"))
		w.Write([]byte(csvData))
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ExportMatchedReport(
		context.Background(),
		"ctx-123",
		"2026-01-01",
		"2026-01-31",
	)

	require.NoError(t, err)
	assert.Equal(t, csvData, string(result))
}

func TestReportingClient_ExportUnmatchedReport(t *testing.T) {
	t.Parallel()

	csvData := "id,amount,status\n1,100.00,unmatched\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/unmatched/export", r.URL.Path)
		assert.Equal(t, "2026-01-01", r.URL.Query().Get("date_from"))
		assert.Equal(t, "2026-01-31", r.URL.Query().Get("date_to"))
		w.Write([]byte(csvData))
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ExportUnmatchedReport(
		context.Background(),
		"ctx-123",
		"2026-01-01",
		"2026-01-31",
	)

	require.NoError(t, err)
	assert.Contains(t, string(result), "unmatched")
}

func TestReportingClient_ExportSummaryReport(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/summary/export", r.URL.Path)
		w.Write([]byte("summary data"))
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ExportSummaryReport(context.Background(), "ctx-123", "2026-01-01", "2026-01-31")

	require.NoError(t, err)
	assert.Equal(t, "summary data", string(result))
}

func TestReportingClient_ExportVarianceReport(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/reports/contexts/ctx-123/variance/export", r.URL.Path)
		w.Write([]byte("variance data"))
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ExportVarianceReport(context.Background(), "ctx-123", "2026-01-01", "2026-01-31")

	require.NoError(t, err)
	assert.Equal(t, "variance data", string(result))
}

func TestReportingClient_CreateExportJob(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/contexts/ctx-123/export-jobs", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req CreateExportJobRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "MATCHED", req.ReportType)
		assert.Equal(t, "CSV", req.Format)
		assert.Equal(t, "2024-01-01", req.DateFrom)
		assert.Equal(t, "2024-01-31", req.DateTo)

		resp := CreateExportJobResponse{
			JobID:     "job-123",
			Status:    "QUEUED",
			StatusURL: "/v1/export-jobs/job-123",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.CreateExportJob(context.Background(), "ctx-123", CreateExportJobRequest{
		ReportType: "MATCHED",
		Format:     "CSV",
		DateFrom:   "2024-01-01",
		DateTo:     "2024-01-31",
	})

	require.NoError(t, err)
	assert.Equal(t, "job-123", result.JobID)
	assert.Equal(t, "QUEUED", result.Status)
}

func TestReportingClient_GetExportJob(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/export-jobs/job-123", r.URL.Path)

		resp := ExportJob{
			ID:     "job-123",
			Status: "COMPLETED",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetExportJob(context.Background(), "job-123")

	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", result.Status)
}

func TestReportingClient_ListExportJobs(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/export-jobs", r.URL.Path)

		resp := struct {
			Items []ExportJob `json:"items"`
		}{
			Items: []ExportJob{
				{ID: "job-1", Status: "COMPLETED"},
				{ID: "job-2", Status: "QUEUED"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListExportJobs(context.Background())

	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestReportingClient_CancelExportJob(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/export-jobs/job-123/cancel", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	err := client.CancelExportJob(context.Background(), "job-123")

	require.NoError(t, err)
}

func TestReportingClient_DownloadExportJob(t *testing.T) {
	t.Parallel()

	fileData := "id,amount\n1,100.00\n2,200.00\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/export-jobs/job-123/download", r.URL.Path)
		w.Write([]byte(fileData))
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.DownloadExportJob(context.Background(), "job-123")

	require.NoError(t, err)
	assert.Equal(t, fileData, string(result))
}

func TestReportingClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal error"}`))
	}))
	defer server.Close()

	client := NewReportingClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.GetDashboardAggregates(
		context.Background(),
		"ctx-123",
		"2024-01-01",
		"2024-12-31",
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get dashboard aggregates")
}
