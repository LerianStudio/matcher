//go:build e2e

package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// MatchingClient handles matching API endpoints.
type MatchingClient struct {
	client *Client
}

// NewMatchingClient creates a new matching client.
func NewMatchingClient(client *Client) *MatchingClient {
	return &MatchingClient{client: client}
}

// RunMatch triggers a matching run for a context.
func (c *MatchingClient) RunMatch(
	ctx context.Context,
	contextID, mode string,
) (*RunMatchResponse, error) {
	var resp RunMatchResponse
	path := fmt.Sprintf("/v1/matching/contexts/%s/run", contextID)
	req := RunMatchRequest{Mode: mode}
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("run match: %w", err)
	}
	return &resp, nil
}

// RunMatchCommit triggers a matching run in COMMIT mode.
func (c *MatchingClient) RunMatchCommit(
	ctx context.Context,
	contextID string,
) (*RunMatchResponse, error) {
	return c.RunMatch(ctx, contextID, "COMMIT")
}

// RunMatchDryRun triggers a matching run in DRY_RUN mode.
func (c *MatchingClient) RunMatchDryRun(
	ctx context.Context,
	contextID string,
) (*RunMatchResponse, error) {
	return c.RunMatch(ctx, contextID, "DRY_RUN")
}

// GetMatchRun retrieves a match run by ID.
func (c *MatchingClient) GetMatchRun(
	ctx context.Context,
	contextID, runID string,
) (*MatchRun, error) {
	var resp MatchRun
	path := fmt.Sprintf("/v1/matching/runs/%s?contextId=%s", runID, contextID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get match run: %w", err)
	}
	return &resp, nil
}

// GetMatchRunResults retrieves all match groups for a run (handles pagination automatically).
func (c *MatchingClient) GetMatchRunResults(
	ctx context.Context,
	contextID, runID string,
) ([]MatchGroup, error) {
	const maxLimit = 200

	var allGroups []MatchGroup

	cursor := ""

	for {
		var resp struct {
			Items      []MatchGroup `json:"items"`
			NextCursor string       `json:"nextCursor"`
			HasMore    bool         `json:"hasMore"`
		}

		path := fmt.Sprintf(
			"/v1/matching/runs/%s/groups?contextId=%s&limit=%d",
			runID,
			contextID,
			maxLimit,
		)
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}

		err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
		if err != nil {
			return nil, fmt.Errorf("get match run results: %w", err)
		}

		allGroups = append(allGroups, resp.Items...)

		if !resp.HasMore || resp.NextCursor == "" {
			break
		}

		cursor = resp.NextCursor
	}

	return allGroups, nil
}

// ListMatchRuns retrieves all match runs for a context.
func (c *MatchingClient) ListMatchRuns(ctx context.Context, contextID string) ([]MatchRun, error) {
	var resp struct {
		Items []MatchRun `json:"items"`
	}
	path := fmt.Sprintf("/v1/matching/contexts/%s/runs", contextID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list match runs: %w", err)
	}
	return resp.Items, nil
}

// ManualMatch creates a manual match from selected transactions.
func (c *MatchingClient) ManualMatch(
	ctx context.Context,
	contextID string,
	req ManualMatchRequest,
) (*ManualMatchResponse, error) {
	var resp ManualMatchResponse
	path := fmt.Sprintf("/v1/matching/manual?contextId=%s", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("manual match: %w", err)
	}
	return &resp, nil
}

// CreateAdjustment creates a matching adjustment.
func (c *MatchingClient) CreateAdjustment(
	ctx context.Context,
	contextID string,
	req CreateAdjustmentRequest,
) (*AdjustmentResponse, error) {
	var resp AdjustmentResponse
	path := fmt.Sprintf("/v1/matching/adjustments?contextId=%s", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create adjustment: %w", err)
	}
	return &resp, nil
}

// UnmatchGroup breaks/unmatches a match group.
func (c *MatchingClient) UnmatchGroup(
	ctx context.Context,
	contextID, matchGroupID string,
	req UnmatchRequest,
) error {
	path := fmt.Sprintf("/v1/matching/groups/%s?contextId=%s", matchGroupID, contextID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, req, nil)
	if err != nil {
		return fmt.Errorf("unmatch group: %w", err)
	}
	return nil
}
