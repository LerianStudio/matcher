//go:build e2e

package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

// ExceptionClient handles exception API endpoints.
type ExceptionClient struct {
	client *Client
}

// NewExceptionClient creates a new exception client.
// Panics if client is nil (test infrastructure — fail fast on misconfiguration).
func NewExceptionClient(client *Client) *ExceptionClient {
	if client == nil {
		panic("nil client passed to NewExceptionClient")
	}

	return &ExceptionClient{client: client}
}

// Exception represents an exception in API responses.
type Exception struct {
	ID               string     `json:"id"`
	TransactionID    string     `json:"transactionId"`
	Severity         string     `json:"severity"`
	Status           string     `json:"status"`
	Reason           *string    `json:"reason,omitempty"`
	ExternalSystem   *string    `json:"externalSystem,omitempty"`
	ExternalIssueID  *string    `json:"externalIssueId,omitempty"`
	AssignedTo       *string    `json:"assignedTo,omitempty"`
	DueAt            *time.Time `json:"dueAt,omitempty"`
	ResolutionNotes  *string    `json:"resolutionNotes,omitempty"`
	ResolutionType   *string    `json:"resolutionType,omitempty"`
	ResolutionReason *string    `json:"resolutionReason,omitempty"`
	CreatedAt        time.Time  `json:"createdAt"`
	UpdatedAt        time.Time  `json:"updatedAt"`
}

// ExceptionHistory represents a history event for an exception.
type ExceptionHistory struct {
	ID          string         `json:"id"`
	ExceptionID string         `json:"exceptionId"`
	Action      string         `json:"action"`
	ActorID     string         `json:"actorId,omitempty"`
	ChangedFrom map[string]any `json:"changedFrom,omitempty"`
	ChangedTo   map[string]any `json:"changedTo,omitempty"`
	Notes       string         `json:"notes,omitempty"`
	CreatedAt   time.Time      `json:"createdAt"`
}

// ForceMatchRequest represents the payload for force matching an exception.
type ForceMatchRequest struct {
	OverrideReason string `json:"overrideReason"`
	Notes          string `json:"notes"`
}

// AdjustEntryRequest represents the payload for adjusting an entry.
type AdjustEntryRequest struct {
	ReasonCode  string          `json:"reasonCode"`
	Notes       string          `json:"notes"`
	Amount      decimal.Decimal `json:"amount"`
	Currency    string          `json:"currency"`
	EffectiveAt time.Time       `json:"effectiveAt"`
}

// DispatchRequest represents the payload for dispatching an exception to an external system.
type DispatchRequest struct {
	TargetSystem string `json:"targetSystem"`
	Queue        string `json:"queue,omitempty"`
}

// DispatchResponse represents the response from dispatching an exception.
type DispatchResponse struct {
	ExceptionID       string    `json:"exceptionId"`
	Target            string    `json:"target"`
	ExternalReference string    `json:"externalReference"`
	Acknowledged      bool      `json:"acknowledged"`
	DispatchedAt      time.Time `json:"dispatchedAt"`
}

// ExceptionListFilter contains optional filters for listing exceptions.
type ExceptionListFilter struct {
	Status         string
	Severity       string
	AssignedTo     string
	ExternalSystem string
	DateFrom       *time.Time
	DateTo         *time.Time
	Cursor         string
	Limit          int
	SortBy         string
	SortOrder      string
}

// ForceMatch resolves an exception by forcing a match.
func (c *ExceptionClient) ForceMatch(
	ctx context.Context,
	exceptionID string,
	req ForceMatchRequest,
) (*Exception, error) {
	var resp Exception
	path := fmt.Sprintf("/v1/exceptions/%s/force-match", exceptionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("force match: %w", err)
	}
	return &resp, nil
}

// AdjustEntry resolves an exception by creating an adjustment entry.
func (c *ExceptionClient) AdjustEntry(
	ctx context.Context,
	exceptionID string,
	req AdjustEntryRequest,
) (*Exception, error) {
	var resp Exception
	path := fmt.Sprintf("/v1/exceptions/%s/adjust-entry", exceptionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("adjust entry: %w", err)
	}
	return &resp, nil
}

// GetException retrieves an exception by ID.
func (c *ExceptionClient) GetException(
	ctx context.Context,
	exceptionID string,
) (*Exception, error) {
	var resp Exception
	path := fmt.Sprintf("/v1/exceptions/%s", exceptionID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get exception: %w", err)
	}
	return &resp, nil
}

// ListExceptions retrieves exceptions with optional filters and cursor pagination.
func (c *ExceptionClient) ListExceptions(
	ctx context.Context,
	filter ExceptionListFilter,
) (*ListResponse[Exception], error) {
	var resp ListResponse[Exception]

	params := url.Values{}
	if filter.Status != "" {
		params.Set("status", filter.Status)
	}
	if filter.Severity != "" {
		params.Set("severity", filter.Severity)
	}
	if filter.AssignedTo != "" {
		params.Set("assigned_to", filter.AssignedTo)
	}
	if filter.ExternalSystem != "" {
		params.Set("external_system", filter.ExternalSystem)
	}
	if filter.DateFrom != nil {
		params.Set("date_from", filter.DateFrom.Format(time.RFC3339))
	}
	if filter.DateTo != nil {
		params.Set("date_to", filter.DateTo.Format(time.RFC3339))
	}
	if filter.Cursor != "" {
		params.Set("cursor", filter.Cursor)
	}
	if filter.Limit > 0 {
		params.Set("limit", strconv.Itoa(filter.Limit))
	}
	if filter.SortBy != "" {
		params.Set("sort_by", filter.SortBy)
	}
	if filter.SortOrder != "" {
		params.Set("sort_order", filter.SortOrder)
	}

	path := "/v1/exceptions"
	if len(params) > 0 {
		path += "?" + params.Encode()
	}

	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list exceptions: %w", err)
	}
	return &resp, nil
}

// ListExceptionsByStatus retrieves exceptions filtered by status.
func (c *ExceptionClient) ListExceptionsByStatus(
	ctx context.Context,
	status string,
) (*ListResponse[Exception], error) {
	return c.ListExceptions(ctx, ExceptionListFilter{Status: status, Limit: 100})
}

// ListOpenExceptions retrieves all open exceptions.
func (c *ExceptionClient) ListOpenExceptions(
	ctx context.Context,
) (*ListResponse[Exception], error) {
	return c.ListExceptionsByStatus(ctx, "OPEN")
}

// GetExceptionHistory retrieves the audit history for an exception.
func (c *ExceptionClient) GetExceptionHistory(
	ctx context.Context,
	exceptionID string,
	cursor string,
	limit int,
) (*ListResponse[ExceptionHistory], error) {
	var resp ListResponse[ExceptionHistory]

	params := url.Values{}
	if cursor != "" {
		params.Set("cursor", cursor)
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}

	path := fmt.Sprintf("/v1/exceptions/%s/history", exceptionID)
	if len(params) > 0 {
		path += "?" + params.Encode()
	}

	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get exception history: %w", err)
	}
	return &resp, nil
}

// DispatchToExternal dispatches an exception to an external ticketing system.
func (c *ExceptionClient) DispatchToExternal(
	ctx context.Context,
	exceptionID string,
	req DispatchRequest,
) (*DispatchResponse, error) {
	var resp DispatchResponse
	path := fmt.Sprintf("/v1/exceptions/%s/dispatch", exceptionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("dispatch to external: %w", err)
	}
	return &resp, nil
}

// OpenDispute opens a dispute on an exception.
func (c *ExceptionClient) OpenDispute(
	ctx context.Context,
	exceptionID string,
	req OpenDisputeRequest,
) (*DisputeResponse, error) {
	var resp DisputeResponse
	path := fmt.Sprintf("/v1/exceptions/%s/disputes", exceptionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("open dispute: %w", err)
	}
	return &resp, nil
}

// CloseDispute closes a dispute.
func (c *ExceptionClient) CloseDispute(
	ctx context.Context,
	disputeID string,
	req CloseDisputeRequest,
) (*DisputeResponse, error) {
	var resp DisputeResponse
	path := fmt.Sprintf("/v1/disputes/%s/close", disputeID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("close dispute: %w", err)
	}
	return &resp, nil
}

// SubmitEvidence submits evidence to a dispute.
func (c *ExceptionClient) SubmitEvidence(
	ctx context.Context,
	disputeID string,
	req SubmitEvidenceRequest,
) (*DisputeResponse, error) {
	var resp DisputeResponse
	path := fmt.Sprintf("/v1/disputes/%s/evidence", disputeID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("submit evidence: %w", err)
	}
	return &resp, nil
}

// BulkAssign bulk assigns exceptions to a user.
func (c *ExceptionClient) BulkAssign(
	ctx context.Context,
	req BulkAssignRequest,
) (*BulkActionResponse, error) {
	var resp BulkActionResponse
	err := c.client.DoJSON(ctx, http.MethodPost, "/v1/exceptions/bulk/assign", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("bulk assign: %w", err)
	}
	return &resp, nil
}

// BulkResolve bulk resolves exceptions.
func (c *ExceptionClient) BulkResolve(
	ctx context.Context,
	req BulkResolveRequest,
) (*BulkActionResponse, error) {
	var resp BulkActionResponse
	err := c.client.DoJSON(ctx, http.MethodPost, "/v1/exceptions/bulk/resolve", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("bulk resolve: %w", err)
	}
	return &resp, nil
}

// BulkDispatch bulk dispatches exceptions to external systems.
func (c *ExceptionClient) BulkDispatch(
	ctx context.Context,
	req BulkDispatchRequest,
) (*BulkActionResponse, error) {
	var resp BulkActionResponse
	err := c.client.DoJSON(ctx, http.MethodPost, "/v1/exceptions/bulk/dispatch", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("bulk dispatch: %w", err)
	}
	return &resp, nil
}

// ListComments retrieves comments for an exception.
func (c *ExceptionClient) ListComments(
	ctx context.Context,
	exceptionID string,
) (*ListCommentsResponse, error) {
	var resp ListCommentsResponse
	path := fmt.Sprintf("/v1/exceptions/%s/comments", exceptionID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list comments: %w", err)
	}
	return &resp, nil
}

// AddComment adds a comment to an exception.
func (c *ExceptionClient) AddComment(
	ctx context.Context,
	exceptionID string,
	req AddCommentRequest,
) (*CommentResponse, error) {
	var resp CommentResponse
	path := fmt.Sprintf("/v1/exceptions/%s/comments", exceptionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("add comment: %w", err)
	}
	return &resp, nil
}

// DeleteComment deletes a comment from an exception.
func (c *ExceptionClient) DeleteComment(
	ctx context.Context,
	exceptionID, commentID string,
) error {
	path := fmt.Sprintf("/v1/exceptions/%s/comments/%s", exceptionID, commentID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete comment: %w", err)
	}
	return nil
}

// ListDisputes retrieves disputes with optional filters.
func (c *ExceptionClient) ListDisputes(
	ctx context.Context,
) (*ListDisputesResponse, error) {
	var resp ListDisputesResponse
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/disputes", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list disputes: %w", err)
	}
	return &resp, nil
}

// GetDispute retrieves a dispute by ID.
func (c *ExceptionClient) GetDispute(
	ctx context.Context,
	disputeID string,
) (*DisputeResponse, error) {
	var resp DisputeResponse
	path := fmt.Sprintf("/v1/disputes/%s", disputeID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get dispute: %w", err)
	}
	return &resp, nil
}

// ProcessCallback sends a webhook callback from an external system to update an exception.
func (c *ExceptionClient) ProcessCallback(
	ctx context.Context,
	exceptionID string,
	req ProcessCallbackRequest,
) (*ProcessCallbackResponse, error) {
	return c.ProcessCallbackWithOptions(ctx, exceptionID, req, RequestOptions{})
}

// ProcessCallbackWithOptions sends a webhook callback with explicit request options.
func (c *ExceptionClient) ProcessCallbackWithOptions(
	ctx context.Context,
	exceptionID string,
	req ProcessCallbackRequest,
	opts RequestOptions,
) (*ProcessCallbackResponse, error) {
	var resp ProcessCallbackResponse
	path := fmt.Sprintf("/v1/exceptions/%s/callback", exceptionID)
	err := c.client.DoJSONWithOptions(ctx, http.MethodPost, path, req, &resp, opts)
	if err != nil {
		return nil, fmt.Errorf("process callback: %w", err)
	}
	return &resp, nil
}
