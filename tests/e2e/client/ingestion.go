//nolint:varnamelen,wsl_v5 // Test ingestion client favors concise path composition.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// IngestionClient handles ingestion API endpoints.
type IngestionClient struct {
	client *Client
}

// NewIngestionClient creates a new ingestion client.
func NewIngestionClient(client *Client) *IngestionClient {
	return &IngestionClient{client: client}
}

// UploadFile uploads a file for ingestion.
func (c *IngestionClient) UploadFile(
	ctx context.Context,
	contextID, sourceID, fileName string,
	content []byte,
	format string,
) (*IngestionJob, error) {
	path := fmt.Sprintf("/v1/imports/contexts/%s/sources/%s/upload", contextID, sourceID)
	formFields := map[string]string{"format": format}

	//nolint:bodyclose // DoMultipart reads and closes the response body internally.
	resp, body, err := c.client.DoMultipart(ctx, path, "file", fileName, content, formFields)
	if err != nil {
		return nil, fmt.Errorf("upload file: %w", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		return nil, &APIError{StatusCode: resp.StatusCode, Body: body}
	}

	var job IngestionJob
	if err := json.Unmarshal(body, &job); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &job, nil
}

// UploadCSV is a convenience method for uploading CSV files.
func (c *IngestionClient) UploadCSV(
	ctx context.Context,
	contextID, sourceID, fileName string,
	content []byte,
) (*IngestionJob, error) {
	return c.UploadFile(ctx, contextID, sourceID, fileName, content, "csv")
}

// UploadJSON is a convenience method for uploading JSON files.
func (c *IngestionClient) UploadJSON(
	ctx context.Context,
	contextID, sourceID, fileName string,
	content []byte,
) (*IngestionJob, error) {
	return c.UploadFile(ctx, contextID, sourceID, fileName, content, "json")
}

// GetJob retrieves an ingestion job by ID.
func (c *IngestionClient) GetJob(
	ctx context.Context,
	contextID, jobID string,
) (*IngestionJob, error) {
	var resp IngestionJob
	path := fmt.Sprintf("/v1/imports/contexts/%s/jobs/%s", contextID, jobID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return &resp, nil
}

// ListJobsByContext retrieves all jobs for a context.
func (c *IngestionClient) ListJobsByContext(
	ctx context.Context,
	contextID string,
) ([]IngestionJob, error) {
	var resp struct {
		Items []IngestionJob `json:"items"`
	}
	path := fmt.Sprintf("/v1/imports/contexts/%s/jobs", contextID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return resp.Items, nil
}

// ListTransactionsByJob retrieves all transactions for a job.
// It automatically paginates through all pages to collect all transactions.
func (c *IngestionClient) ListTransactionsByJob(
	ctx context.Context,
	contextID, jobID string,
) ([]Transaction, error) {
	var allItems []Transaction

	basePath := fmt.Sprintf("/v1/imports/contexts/%s/jobs/%s/transactions", contextID, jobID)
	cursor := ""

	for {
		path := basePath
		if cursor != "" {
			path = fmt.Sprintf("%s?cursor=%s", basePath, url.QueryEscape(cursor))
		}

		var resp struct {
			Items      []Transaction `json:"items"`
			NextCursor string        `json:"nextCursor"`
			HasMore    bool          `json:"hasMore"`
		}

		err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
		if err != nil {
			return nil, fmt.Errorf("list transactions: %w", err)
		}

		allItems = append(allItems, resp.Items...)

		if !resp.HasMore || resp.NextCursor == "" {
			break
		}

		cursor = resp.NextCursor
	}

	return allItems, nil
}

// IgnoreTransaction marks a transaction as ignored.
func (c *IngestionClient) IgnoreTransaction(
	ctx context.Context,
	contextID, transactionID string,
	req IgnoreTransactionRequest,
) (*IgnoreTransactionResponse, error) {
	var resp IgnoreTransactionResponse
	path := fmt.Sprintf("/v1/imports/contexts/%s/transactions/%s/ignore", contextID, transactionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("ignore transaction: %w", err)
	}
	return &resp, nil
}

// PreviewFile uploads a file for preview without ingesting.
func (c *IngestionClient) PreviewFile(
	ctx context.Context,
	contextID, sourceID, fileName string,
	content []byte,
	format string,
	maxRows int,
) (*FilePreviewResponse, error) {
	path := fmt.Sprintf("/v1/imports/contexts/%s/sources/%s/preview", contextID, sourceID)
	formFields := map[string]string{"format": format}
	if maxRows > 0 {
		formFields["max_rows"] = strconv.Itoa(maxRows)
	}

	//nolint:bodyclose // DoMultipart reads and closes the response body internally.
	resp, body, err := c.client.DoMultipart(ctx, path, "file", fileName, content, formFields)
	if err != nil {
		return nil, fmt.Errorf("preview file: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &APIError{StatusCode: resp.StatusCode, Body: body}
	}

	var preview FilePreviewResponse
	if err := json.Unmarshal(body, &preview); err != nil {
		return nil, fmt.Errorf("unmarshal preview response: %w", err)
	}

	return &preview, nil
}

// SearchTransactions searches for transactions with filters.
func (c *IngestionClient) SearchTransactions(
	ctx context.Context,
	contextID string,
	params SearchTransactionsParams,
) (*SearchTransactionsResponse, error) {
	var resp SearchTransactionsResponse

	qp := buildSearchTransactionsQuery(params)

	path := fmt.Sprintf("/v1/imports/contexts/%s/transactions/search", contextID)
	if len(qp) > 0 {
		path += "?" + qp.Encode()
	}

	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("search transactions: %w", err)
	}
	return &resp, nil
}

func buildSearchTransactionsQuery(params SearchTransactionsParams) url.Values {
	query := url.Values{}

	stringFilters := map[string]string{
		"q":          params.Query,
		"amount_min": params.AmountMin,
		"amount_max": params.AmountMax,
		"date_from":  params.DateFrom,
		"date_to":    params.DateTo,
		"reference":  params.Reference,
		"currency":   params.Currency,
		"source_id":  params.SourceID,
		"status":     params.Status,
	}

	for key, value := range stringFilters {
		if value != "" {
			query.Set(key, value)
		}
	}

	if params.Limit > 0 {
		query.Set("limit", strconv.Itoa(params.Limit))
	}

	if params.Offset > 0 {
		query.Set("offset", strconv.Itoa(params.Offset))
	}

	return query
}
