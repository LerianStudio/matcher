package dto

import (
	"encoding/json"
	"time"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// auditChangesKey names the nested object under which the governance
// consumer stores the caller-supplied Changes map (see
// internal/governance/adapters/audit/consumer.buildChangesPayload).
// Truncation markers live one level deeper under
// sharedDomain.TruncatedMarkerKey.
const auditChangesKey = "changes"

// AuditLogToResponse converts an AuditLog entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func AuditLogToResponse(log *entities.AuditLog) AuditLogResponse {
	if log == nil {
		return AuditLogResponse{Changes: json.RawMessage("{}")}
	}

	truncated, originalSize := extractTruncationMarkers(log.Changes)

	return AuditLogResponse{
		ID:           log.ID.String(),
		TenantID:     log.TenantID.String(),
		EntityType:   log.EntityType,
		EntityID:     log.EntityID.String(),
		Action:       log.Action,
		ActorID:      log.ActorID,
		Changes:      log.Changes,
		Truncated:    truncated,
		OriginalSize: originalSize,
		CreatedAt:    log.CreatedAt.Format(time.RFC3339),
	}
}

// extractTruncationMarkers inspects the audit changes envelope for the
// truncation markers embedded by the configuration and exception audit
// publishers. The envelope shape produced by the governance consumer is:
//
//	{"entity_type":"...", "changes": {"_truncated": true, "_originalSize": N, ...}}
//
// Malformed payloads (invalid JSON, unexpected shapes, non-numeric
// OriginalSize) degrade gracefully: if the _truncated flag is clearly true
// the caller still sees Truncated=true, but OriginalSize falls back to zero
// rather than surfacing garbage to API consumers.
func extractTruncationMarkers(payload []byte) (truncated bool, originalSize int64) {
	if len(payload) == 0 {
		return false, 0
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return false, 0
	}

	changesRaw, ok := envelope[auditChangesKey]
	if !ok {
		return false, 0
	}

	var changes map[string]json.RawMessage
	if err := json.Unmarshal(changesRaw, &changes); err != nil {
		return false, 0
	}

	truncatedRaw, ok := changes[sharedDomain.TruncatedMarkerKey]
	if !ok {
		return false, 0
	}

	if err := json.Unmarshal(truncatedRaw, &truncated); err != nil || !truncated {
		return false, 0
	}

	sizeRaw, ok := changes[sharedDomain.TruncatedOriginalSizeKey]
	if !ok {
		return true, 0
	}

	// Tolerate malformed _originalSize (e.g. string or missing) without
	// dropping the truncation flag — the flag itself is the critical signal.
	if err := json.Unmarshal(sizeRaw, &originalSize); err != nil {
		return true, 0
	}

	return true, originalSize
}

// AuditLogsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func AuditLogsToResponse(logs []*entities.AuditLog) []AuditLogResponse {
	result := make([]AuditLogResponse, 0, len(logs))

	for _, log := range logs {
		if log != nil {
			result = append(result, AuditLogToResponse(log))
		}
	}

	return result
}
