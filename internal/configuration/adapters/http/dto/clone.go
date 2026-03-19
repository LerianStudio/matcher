package dto

import (
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// CloneContextRequest is the request body for cloning a reconciliation context.
// @Description Request payload for cloning a context with its configuration
type CloneContextRequest struct {
	// Name for the cloned context
	Name string `json:"name" validate:"required,min=1,max=100" example:"Bank Reconciliation Q1 (Copy)" minLength:"1" maxLength:"100"`
	// Whether to include sources and field maps in the clone (default: true)
	IncludeSources *bool `json:"includeSources,omitempty" example:"true"`
	// Whether to include match rules in the clone (default: true)
	IncludeRules *bool `json:"includeRules,omitempty" example:"true"`
}

// CloneContextResponse is the response body for a clone operation.
// @Description Result of cloning a reconciliation context
type CloneContextResponse struct {
	// The newly created context
	Context ReconciliationContextResponse `json:"context"`
	// Number of sources cloned
	SourcesCloned int `json:"sourcesCloned" example:"3"`
	// Number of match rules cloned
	RulesCloned int `json:"rulesCloned" example:"5"`
	// Number of field maps cloned
	FieldMapsCloned int `json:"fieldMapsCloned" example:"3"`
}

// CloneResultToResponse converts an entities.CloneResult to a CloneContextResponse.
func CloneResultToResponse(result *entities.CloneResult) CloneContextResponse {
	if result == nil {
		return CloneContextResponse{
			Context: ReconciliationContextResponse{},
		}
	}

	return CloneContextResponse{
		Context:         ReconciliationContextToResponse(result.Context),
		SourcesCloned:   result.SourcesCloned,
		RulesCloned:     result.RulesCloned,
		FieldMapsCloned: result.FieldMapsCloned,
	}
}
