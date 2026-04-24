// Package client provides the E2E HTTP client and payload models for Matcher tests.
package client

// ErrorResponse represents an API error response.
type ErrorResponse struct {
	Code    string         `json:"code"`
	Title   string         `json:"title,omitempty"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
