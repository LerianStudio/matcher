// Package dto provides data transfer objects for discovery HTTP handlers.
package dto

// StartExtractionRequest is the request body for POST /v1/discovery/connections/:connectionId/extractions.
type StartExtractionRequest struct {
	Tables    map[string]any `json:"tables" validate:"required,min=1"`
	StartDate string         `json:"startDate,omitempty"`
	EndDate   string         `json:"endDate,omitempty"`
	Filters   map[string]any `json:"filters,omitempty"`
}

// PollExtractionRequest is the request body for POST /v1/discovery/extractions/:extractionId/poll.
// Currently empty because extraction ID comes from the path.
type PollExtractionRequest struct{}
