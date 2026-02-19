package dto

import (
	"encoding/json"
	"time"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// AuditLogToResponse converts an AuditLog entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func AuditLogToResponse(log *entities.AuditLog) AuditLogResponse {
	if log == nil {
		return AuditLogResponse{Changes: json.RawMessage("{}")}
	}

	return AuditLogResponse{
		ID:         log.ID.String(),
		TenantID:   log.TenantID.String(),
		EntityType: log.EntityType,
		EntityID:   log.EntityID.String(),
		Action:     log.Action,
		ActorID:    log.ActorID,
		Changes:    log.Changes,
		CreatedAt:  log.CreatedAt.Format(time.RFC3339),
	}
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
