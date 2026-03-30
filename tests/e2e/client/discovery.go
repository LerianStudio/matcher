//go:build e2e

package client

import (
	"context"
	"fmt"
	"net/http"
)

// DiscoveryClient handles discovery API endpoints.
type DiscoveryClient struct {
	client *Client
}

// NewDiscoveryClient creates a new discovery client.
// Panics if client is nil (test infrastructure — fail fast on misconfiguration).
func NewDiscoveryClient(client *Client) *DiscoveryClient {
	if client == nil {
		panic("nil client passed to NewDiscoveryClient")
	}

	return &DiscoveryClient{client: client}
}

// GetStatus retrieves the current discovery service status.
func (c *DiscoveryClient) GetStatus(ctx context.Context) (*DiscoveryStatusResponse, error) {
	var resp DiscoveryStatusResponse
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/discovery/status", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get discovery status: %w", err)
	}
	return &resp, nil
}

// ListConnections retrieves all discovered Fetcher connections.
func (c *DiscoveryClient) ListConnections(ctx context.Context) (*DiscoveryConnectionListResponse, error) {
	var resp DiscoveryConnectionListResponse
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/discovery/connections", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list discovery connections: %w", err)
	}
	return &resp, nil
}

// GetConnection retrieves a single discovered connection by ID.
func (c *DiscoveryClient) GetConnection(ctx context.Context, connectionID string) (*DiscoveryConnectionResponse, error) {
	var resp DiscoveryConnectionResponse
	path := fmt.Sprintf("/v1/discovery/connections/%s", connectionID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get discovery connection: %w", err)
	}
	return &resp, nil
}

// GetConnectionSchema retrieves the discovered schema for a connection.
func (c *DiscoveryClient) GetConnectionSchema(ctx context.Context, connectionID string) (*DiscoveryConnectionSchemaResponse, error) {
	var resp DiscoveryConnectionSchemaResponse
	path := fmt.Sprintf("/v1/discovery/connections/%s/schema", connectionID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get connection schema: %w", err)
	}
	return &resp, nil
}

// TestConnection tests connectivity for a discovered connection.
func (c *DiscoveryClient) TestConnection(ctx context.Context, connectionID string) (*DiscoveryTestConnectionResponse, error) {
	var resp DiscoveryTestConnectionResponse
	path := fmt.Sprintf("/v1/discovery/connections/%s/test", connectionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("test connection: %w", err)
	}
	return &resp, nil
}

// StartExtraction creates an extraction request for a discovered connection.
func (c *DiscoveryClient) StartExtraction(
	ctx context.Context,
	connectionID string,
	req DiscoveryStartExtractionRequest,
) (*DiscoveryExtractionResponse, error) {
	var resp DiscoveryExtractionResponse
	path := fmt.Sprintf("/v1/discovery/connections/%s/extractions", connectionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("start extraction: %w", err)
	}
	return &resp, nil
}

// GetExtraction retrieves an extraction request by ID.
func (c *DiscoveryClient) GetExtraction(ctx context.Context, extractionID string) (*DiscoveryExtractionResponse, error) {
	var resp DiscoveryExtractionResponse
	path := fmt.Sprintf("/v1/discovery/extractions/%s", extractionID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get extraction: %w", err)
	}
	return &resp, nil
}

// PollExtraction polls Fetcher for the latest extraction status.
func (c *DiscoveryClient) PollExtraction(ctx context.Context, extractionID string) (*DiscoveryExtractionResponse, error) {
	var resp DiscoveryExtractionResponse
	path := fmt.Sprintf("/v1/discovery/extractions/%s/poll", extractionID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("poll extraction: %w", err)
	}
	return &resp, nil
}

// RefreshDiscovery forces an immediate sync with the Fetcher service.
func (c *DiscoveryClient) RefreshDiscovery(ctx context.Context) (*DiscoveryRefreshResponse, error) {
	var resp DiscoveryRefreshResponse
	err := c.client.DoJSON(ctx, http.MethodPost, "/v1/discovery/refresh", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("refresh discovery: %w", err)
	}
	return &resp, nil
}
