package dto

import (
	"encoding/json"

	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// AuditLogResponse represents an audit log entry in API responses.
// @Description Immutable audit log entry for governance tracking
type AuditLogResponse struct {
	// Unique identifier for the audit log entry
	ID string `json:"id"                example:"550e8400-e29b-41d4-a716-446655440000"`
	// Tenant ID that owns this audit log entry
	TenantID string `json:"tenantId"          example:"550e8400-e29b-41d4-a716-446655440001"`
	// Type of entity that was modified
	EntityType string `json:"entityType"        example:"reconciliation_context"`
	// ID of the entity that was modified
	EntityID string `json:"entityId"          example:"550e8400-e29b-41d4-a716-446655440002"`
	// Action that was performed
	Action string `json:"action"            example:"CREATE"`
	// ID of the actor who performed the action
	ActorID *string `json:"actorId,omitempty" example:"user@example.com"`
	// Changes made to the entity. When the diff exceeded the outbox payload
	// cap, Changes carries a truncation marker envelope instead of the full
	// diff, and Truncated + OriginalSize expose the marker metadata as
	// first-class fields.
	Changes json.RawMessage `json:"changes" validate:"omitempty,max=10000" maxItems:"10000"`
	// Truncated is true when the original audit diff exceeded the outbox
	// payload cap and was replaced with a truncation marker. Consumers should
	// treat Changes as a metadata envelope (not the full diff) in this case.
	Truncated bool `json:"truncated"         example:"false"`
	// OriginalSize is the byte size of the original diff before truncation.
	// Zero when Truncated is false, or when the marker was malformed.
	OriginalSize int64 `json:"originalSize"      example:"0"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"         example:"2025-01-15T10:30:00Z"`
}

// ListAuditLogsResponse represents the paginated list of audit logs.
// @Description Paginated list of audit log entries
type ListAuditLogsResponse struct {
	// List of audit log entries
	Items []AuditLogResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	sharedhttp.CursorResponse
}
