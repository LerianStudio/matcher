// Package dto provides data transfer objects for discovery HTTP handlers.
package dto

import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"

// ExtractionTableRequest is the per-table extraction config accepted by the API.
type ExtractionTableRequest struct {
	Columns []string `json:"columns,omitempty" validate:"omitempty,min=1,dive,required"`
}

// StartExtractionRequest is the request body for POST /v1/discovery/connections/:connectionId/extractions.
type StartExtractionRequest struct {
	Tables    map[string]ExtractionTableRequest `json:"tables" validate:"required,min=1"`
	StartDate string                            `json:"startDate,omitempty"`
	EndDate   string                            `json:"endDate,omitempty"`
	Filters   *sharedPorts.ExtractionFilters    `json:"filters,omitempty"`
}
