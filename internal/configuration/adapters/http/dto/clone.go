package dto

import (
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
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
	// Whether to clone fee schedules (true) or keep original references (false) (default: true)
	IncludeFeeSchedules *bool `json:"includeFeeSchedules,omitempty" example:"true"`
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
	// Number of fee schedules cloned
	FeeSchedulesCloned int `json:"feeSchedulesCloned" example:"2"`
}

// CloneResultToResponse converts a command.CloneResult to a CloneContextResponse.
func CloneResultToResponse(result *command.CloneResult) CloneContextResponse {
	if result == nil {
		return CloneContextResponse{
			Context: ReconciliationContextResponse{},
		}
	}

	return CloneContextResponse{
		Context:            ReconciliationContextToResponse(result.Context),
		SourcesCloned:      result.SourcesCloned,
		RulesCloned:        result.RulesCloned,
		FieldMapsCloned:    result.FieldMapsCloned,
		FeeSchedulesCloned: result.FeeSchedulesCloned,
	}
}
