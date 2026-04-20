//nolint:perfsprint,varnamelen,wsl_v5 // Test governance client favors concise path composition.
package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
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
		qp := url.Values{}
		for k, v := range params {
			qp.Set(k, v)
		}

		path += "?" + qp.Encode()
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

// GetActorMapping retrieves an actor mapping by actor ID.
func (c *GovernanceClient) GetActorMapping(
	ctx context.Context,
	actorID string,
) (*ActorMappingResponse, error) {
	var resp ActorMappingResponse
	path := fmt.Sprintf("/v1/governance/actor-mappings/%s", url.PathEscape(actorID))
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get actor mapping: %w", err)
	}
	return &resp, nil
}

// UpsertActorMapping creates or updates an actor mapping.
func (c *GovernanceClient) UpsertActorMapping(
	ctx context.Context,
	actorID string,
	req UpsertActorMappingRequest,
) (*ActorMappingResponse, error) {
	var resp ActorMappingResponse
	path := fmt.Sprintf("/v1/governance/actor-mappings/%s", url.PathEscape(actorID))
	err := c.client.DoJSON(ctx, http.MethodPut, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("upsert actor mapping: %w", err)
	}
	return &resp, nil
}

// DeleteActorMapping permanently removes an actor mapping.
func (c *GovernanceClient) DeleteActorMapping(
	ctx context.Context,
	actorID string,
) error {
	path := fmt.Sprintf("/v1/governance/actor-mappings/%s", url.PathEscape(actorID))
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete actor mapping: %w", err)
	}
	return nil
}

// PseudonymizeActor replaces PII fields with [REDACTED] for GDPR compliance.
func (c *GovernanceClient) PseudonymizeActor(
	ctx context.Context,
	actorID string,
) error {
	path := fmt.Sprintf("/v1/governance/actor-mappings/%s/pseudonymize", url.PathEscape(actorID))
	err := c.client.DoJSON(ctx, http.MethodPost, path, nil, nil)
	if err != nil {
		return fmt.Errorf("pseudonymize actor: %w", err)
	}
	return nil
}
