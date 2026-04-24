// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

// ProcessCallbackRequest represents a webhook callback from an external system.
// @Description External system callback payload
type ProcessCallbackRequest struct {
	// Type of callback (e.g. "status_update", "assignment")
	CallbackType string `json:"callbackType"`
	// External system identifier (e.g. JIRA, SERVICENOW, WEBHOOK)
	ExternalSystem string `json:"externalSystem" validate:"required"`
	// Issue ID in the external system
	ExternalIssueID string `json:"externalIssueId" validate:"required"`
	// Target status for the exception (e.g. ASSIGNED, RESOLVED)
	Status string `json:"status" validate:"required"`
	// Optional resolution notes
	ResolutionNotes string `json:"resolutionNotes"`
	// Assignee identifier (required when status is ASSIGNED)
	Assignee string `json:"assignee"`
	// Due date for resolution in RFC3339 format
	DueAt *string `json:"dueAt"`
	// External system timestamp in RFC3339 format
	UpdatedAt *string `json:"updatedAt"`
	// Arbitrary payload from the external system
	Payload map[string]any `json:"payload"`
}

// ProcessCallbackResponse represents the response for a processed callback.
// @Description Callback processing result
type ProcessCallbackResponse struct {
	// Processing status
	Status string `json:"status" example:"accepted"`
}
