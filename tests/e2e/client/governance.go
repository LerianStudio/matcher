//go:build e2e

package client

import (
	"context"
	"fmt"
	"net/http"
)

// GovernanceClient handles governance API endpoints.
type GovernanceClient struct {
	client *Client
}

// NewGovernanceClient creates a new governance client.
func NewGovernanceClient(client *Client) *GovernanceClient {
	return &GovernanceClient{client: client}
}

// GetAuditLog retrieves an audit log by ID.
func (c *GovernanceClient) GetAuditLog(ctx context.Context, id string) (*AuditLog, error) {
	var resp AuditLog
	path := fmt.Sprintf("/v1/governance/audit-logs/%s", id)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get audit log: %w", err)
	}
	return &resp, nil
}

// ListAuditLogsByEntity retrieves audit logs for a specific entity.
func (c *GovernanceClient) ListAuditLogsByEntity(
	ctx context.Context,
	entityType, entityID string,
) ([]AuditLog, error) {
	var resp struct {
		Items []AuditLog `json:"items"`
	}
	path := fmt.Sprintf("/v1/governance/entities/%s/%s/audit-logs", entityType, entityID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list audit logs: %w", err)
	}
	return resp.Items, nil
}

// ListAuditLogs retrieves audit logs with optional filters.
func (c *GovernanceClient) ListAuditLogs(
	ctx context.Context,
	params map[string]string,
) ([]AuditLog, error) {
	var resp struct {
		Items []AuditLog `json:"items"`
	}
	path := "/v1/governance/audit-logs"
	if len(params) > 0 {
		path += "?"
		first := true
		for k, v := range params {
			if !first {
				path += "&"
			}
			path += k + "=" + v
			first = false
		}
	}
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list audit logs: %w", err)
	}
	return resp.Items, nil
}

// ListAuditLogsByAction retrieves audit logs filtered by action type.
func (c *GovernanceClient) ListAuditLogsByAction(
	ctx context.Context,
	action string,
) ([]AuditLog, error) {
	return c.ListAuditLogs(ctx, map[string]string{"action": action})
}

// ListAuditLogsByEntityType retrieves audit logs filtered by entity type.
func (c *GovernanceClient) ListAuditLogsByEntityType(
	ctx context.Context,
	entityType string,
) ([]AuditLog, error) {
	return c.ListAuditLogs(ctx, map[string]string{"entity_type": entityType})
}

// ListArchives retrieves governance archives.
func (c *GovernanceClient) ListArchives(ctx context.Context) ([]ArchiveMetadata, error) {
	var resp struct {
		Items []ArchiveMetadata `json:"items"`
	}
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/governance/archives", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list archives: %w", err)
	}
	return resp.Items, nil
}

// DownloadArchive retrieves download info for a specific archive.
func (c *GovernanceClient) DownloadArchive(
	ctx context.Context,
	archiveID string,
) (*ArchiveDownloadResponse, error) {
	var resp ArchiveDownloadResponse
	path := fmt.Sprintf("/v1/governance/archives/%s/download", archiveID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("download archive: %w", err)
	}
	return &resp, nil
}
