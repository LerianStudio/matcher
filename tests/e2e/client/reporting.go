//go:build e2e

package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// ReportingClient handles reporting API endpoints.
type ReportingClient struct {
	client *Client
}

// NewReportingClient creates a new reporting client.
// Panics if client is nil (test infrastructure — fail fast on misconfiguration).
func NewReportingClient(client *Client) *ReportingClient {
	if client == nil {
		panic("nil client passed to NewReportingClient")
	}

	return &ReportingClient{client: client}
}

// GetDashboardAggregates retrieves dashboard aggregate data.
func (c *ReportingClient) GetDashboardAggregates(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*DashboardAggregates, error) {
	var resp DashboardAggregates
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/dashboard?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get dashboard aggregates: %w", err)
	}
	return &resp, nil
}

// GetVolumeStats retrieves volume statistics.
func (c *ReportingClient) GetVolumeStats(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*VolumeStats, error) {
	var resp VolumeStats
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/dashboard/volume?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get volume stats: %w", err)
	}
	return &resp, nil
}

// GetMatchRateStats retrieves match rate statistics.
func (c *ReportingClient) GetMatchRateStats(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*MatchRateStats, error) {
	var resp MatchRateStats
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/dashboard/match-rate?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get match rate stats: %w", err)
	}
	return &resp, nil
}

// GetSLAStats retrieves SLA statistics.
func (c *ReportingClient) GetSLAStats(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*SLAStats, error) {
	var resp SLAStats
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/dashboard/sla?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get sla stats: %w", err)
	}
	return &resp, nil
}

// GetDashboardMetrics retrieves comprehensive dashboard metrics.
func (c *ReportingClient) GetDashboardMetrics(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*DashboardMetricsResponse, error) {
	var resp DashboardMetricsResponse
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/dashboard/metrics?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get dashboard metrics: %w", err)
	}
	return &resp, nil
}

// ExportMatchedReport downloads the matched transactions report.
func (c *ReportingClient) ExportMatchedReport(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) ([]byte, error) {
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/matched/export?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	_, body, err := c.client.DoRaw(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, fmt.Errorf("export matched report: %w", err)
	}
	return body, nil
}

// ExportUnmatchedReport downloads the unmatched transactions report.
func (c *ReportingClient) ExportUnmatchedReport(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) ([]byte, error) {
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/unmatched/export?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	_, body, err := c.client.DoRaw(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, fmt.Errorf("export unmatched report: %w", err)
	}
	return body, nil
}

// ExportSummaryReport downloads the summary report.
func (c *ReportingClient) ExportSummaryReport(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) ([]byte, error) {
	path := fmt.Sprintf("/v1/reports/contexts/%s/summary/export?date_from=%s&date_to=%s", contextID, dateFrom, dateTo)
	_, body, err := c.client.DoRaw(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, fmt.Errorf("export summary report: %w", err)
	}
	return body, nil
}

// ExportVarianceReport downloads the variance report.
func (c *ReportingClient) ExportVarianceReport(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) ([]byte, error) {
	path := fmt.Sprintf("/v1/reports/contexts/%s/variance/export?date_from=%s&date_to=%s", contextID, dateFrom, dateTo)
	_, body, err := c.client.DoRaw(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, fmt.Errorf("export variance report: %w", err)
	}
	return body, nil
}

// CreateExportJob creates an async export job.
func (c *ReportingClient) CreateExportJob(
	ctx context.Context,
	contextID string,
	req CreateExportJobRequest,
) (*CreateExportJobResponse, error) {
	var resp CreateExportJobResponse
	path := fmt.Sprintf("/v1/contexts/%s/export-jobs", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create export job: %w", err)
	}
	return &resp, nil
}

// GetExportJob retrieves an export job by ID.
func (c *ReportingClient) GetExportJob(ctx context.Context, jobID string) (*ExportJob, error) {
	var resp ExportJob
	path := fmt.Sprintf("/v1/export-jobs/%s", jobID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get export job: %w", err)
	}
	return &resp, nil
}

// ListExportJobs retrieves all export jobs.
func (c *ReportingClient) ListExportJobs(ctx context.Context) ([]ExportJob, error) {
	var resp struct {
		Items []ExportJob `json:"items"`
	}
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/export-jobs", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list export jobs: %w", err)
	}
	return resp.Items, nil
}

// CancelExportJob cancels an export job.
func (c *ReportingClient) CancelExportJob(ctx context.Context, jobID string) error {
	path := fmt.Sprintf("/v1/export-jobs/%s/cancel", jobID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, nil, nil)
	if err != nil {
		return fmt.Errorf("cancel export job: %w", err)
	}
	return nil
}

// DownloadExportJob downloads the export job result.
func (c *ReportingClient) DownloadExportJob(ctx context.Context, jobID string) ([]byte, error) {
	path := fmt.Sprintf("/v1/export-jobs/%s/download", jobID)
	_, body, err := c.client.DoRaw(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, fmt.Errorf("download export job: %w", err)
	}
	return body, nil
}

// GetSourceBreakdown retrieves per-source reconciliation metrics.
func (c *ReportingClient) GetSourceBreakdown(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*SourceBreakdownResponse, error) {
	var resp SourceBreakdownResponse
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/dashboard/source-breakdown?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get source breakdown: %w", err)
	}
	return &resp, nil
}

// GetCashImpact retrieves unreconciled financial exposure.
func (c *ReportingClient) GetCashImpact(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*CashImpactResponse, error) {
	var resp CashImpactResponse
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/dashboard/cash-impact?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get cash impact: %w", err)
	}
	return &resp, nil
}

// CountTransactions retrieves the transaction count for export sizing.
func (c *ReportingClient) CountTransactions(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*ExportCountResponse, error) {
	var resp ExportCountResponse
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/transactions/count?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("count transactions: %w", err)
	}
	return &resp, nil
}

// CountMatches retrieves the match count for export sizing.
func (c *ReportingClient) CountMatches(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*ExportCountResponse, error) {
	var resp ExportCountResponse
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/matches/count?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("count matches: %w", err)
	}
	return &resp, nil
}

// CountExceptions retrieves the exception count for export sizing.
func (c *ReportingClient) CountExceptions(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*ExportCountResponse, error) {
	var resp ExportCountResponse
	path := fmt.Sprintf(
		"/v1/reports/contexts/%s/exceptions/count?date_from=%s&date_to=%s",
		contextID,
		dateFrom,
		dateTo,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("count exceptions: %w", err)
	}
	return &resp, nil
}

// GetMatchedReport retrieves a paginated matched transactions report.
func (c *ReportingClient) GetMatchedReport(
	ctx context.Context,
	contextID string,
	params map[string]string,
) (*PaginatedMatchedReport, error) {
	var resp PaginatedMatchedReport
	path := fmt.Sprintf("/v1/reports/contexts/%s/matched", contextID)
	path = appendQueryParams(path, params)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get matched report: %w", err)
	}
	return &resp, nil
}

// GetUnmatchedReport retrieves a paginated unmatched transactions report.
func (c *ReportingClient) GetUnmatchedReport(
	ctx context.Context,
	contextID string,
	params map[string]string,
) (*PaginatedUnmatchedReport, error) {
	var resp PaginatedUnmatchedReport
	path := fmt.Sprintf("/v1/reports/contexts/%s/unmatched", contextID)
	path = appendQueryParams(path, params)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get unmatched report: %w", err)
	}
	return &resp, nil
}

// GetSummaryReport retrieves the summary report for a reconciliation context.
func (c *ReportingClient) GetSummaryReport(
	ctx context.Context,
	contextID string,
	params map[string]string,
) (*SummaryReportResponse, error) {
	var resp SummaryReportResponse
	path := fmt.Sprintf("/v1/reports/contexts/%s/summary", contextID)
	path = appendQueryParams(path, params)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get summary report: %w", err)
	}
	return &resp, nil
}

// GetVarianceReport retrieves a paginated variance report.
func (c *ReportingClient) GetVarianceReport(
	ctx context.Context,
	contextID string,
	params map[string]string,
) (*PaginatedVarianceReport, error) {
	var resp PaginatedVarianceReport
	path := fmt.Sprintf("/v1/reports/contexts/%s/variance", contextID)
	path = appendQueryParams(path, params)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get variance report: %w", err)
	}
	return &resp, nil
}

// CountUnmatched retrieves the unmatched transaction count for export sizing.
func (c *ReportingClient) CountUnmatched(
	ctx context.Context,
	contextID, dateFrom, dateTo string,
) (*ExportCountResponse, error) {
	var resp ExportCountResponse
	path := appendQueryParams(
		fmt.Sprintf("/v1/reports/contexts/%s/unmatched/count", contextID),
		map[string]string{
			"date_from": dateFrom,
			"date_to":   dateTo,
		},
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("count unmatched: %w", err)
	}
	return &resp, nil
}

// ListExportJobsByContext retrieves export jobs for a specific context.
func (c *ReportingClient) ListExportJobsByContext(
	ctx context.Context,
	contextID string,
	params map[string]string,
) (*ExportJobListPage, error) {
	var resp ExportJobListPage
	path := appendQueryParams(
		fmt.Sprintf("/v1/contexts/%s/export-jobs", contextID),
		params,
	)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list export jobs by context: %w", err)
	}
	return &resp, nil
}

// appendQueryParams appends URL-encoded query parameters from a map to a URL path.
func appendQueryParams(path string, params map[string]string) string {
	if len(params) == 0 {
		return path
	}

	qp := url.Values{}
	for k, v := range params {
		qp.Set(k, v)
	}

	return path + "?" + qp.Encode()
}
