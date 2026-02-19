// Package http provides HTTP handlers for configuration management.
package http

import (
	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// ListContextsResponse represents a cursor-paginated list of reconciliation contexts.
// @Description Cursor-paginated list of reconciliation contexts
type ListContextsResponse struct {
	// List of reconciliation contexts
	Items []dto.ReconciliationContextResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	// Cursor pagination fields
	sharedhttp.CursorResponse
}

// ListSourcesResponse represents a cursor-paginated list of reconciliation sources.
// @Description Cursor-paginated list of reconciliation sources
type ListSourcesResponse struct {
	// List of reconciliation sources with field map status
	Items []dto.SourceWithFieldMapStatusResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	// Cursor pagination fields
	sharedhttp.CursorResponse
}

// ListFieldMapsResponse represents a list of field mappings.
// @Description List of field mappings for a source
type ListFieldMapsResponse struct {
	// List of field mappings
	Items []dto.FieldMapResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
}

// ListMatchRulesResponse represents a cursor-paginated list of match rules.
// @Description Cursor-paginated list of match rules
type ListMatchRulesResponse struct {
	// List of match rules
	Items []dto.MatchRuleResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	// Cursor pagination fields
	sharedhttp.CursorResponse
}

// toContextValues converts a slice of pointers to response DTOs for JSON serialization.
// This ensures empty slices serialize as [] instead of null.
func toContextValues(
	contexts []*entities.ReconciliationContext,
) []dto.ReconciliationContextResponse {
	return dto.ReconciliationContextsToResponse(contexts)
}

// toSourceValuesWithFieldMaps converts a slice of pointers to response DTOs with field map status.
func toSourceValuesWithFieldMaps(
	sources []*entities.ReconciliationSource,
	fieldMapsExist map[uuid.UUID]bool,
) []dto.SourceWithFieldMapStatusResponse {
	return dto.SourcesToResponseWithFieldMaps(sources, fieldMapsExist)
}

// toMatchRuleValues converts a slice of pointers to response DTOs for JSON serialization.
func toMatchRuleValues(rules []*entities.MatchRule) []dto.MatchRuleResponse {
	return dto.MatchRulesToResponse(rules)
}
