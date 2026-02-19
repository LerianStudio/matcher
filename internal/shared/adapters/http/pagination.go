// Package http provides shared HTTP utilities and DTOs.
package http

// CursorResponse provides cursor pagination fields for paginated API responses.
// This is the STANDARD pagination response structure for all list endpoints.
//
// All list response types should EMBED this struct (not nest it) to produce
// a flattened JSON response structure:
//
//	type ListContextsResponse struct {
//	    Items []entities.ReconciliationContext `json:"items"`
//	    CursorResponse // Embedded (flattened fields)
//	}
//
// This produces JSON like:
//
//	{
//	    "items": [...],
//	    "nextCursor": "abc123",
//	    "prevCursor": "",
//	    "limit": 20,
//	    "hasMore": true
//	}
//
// Do NOT nest CursorResponse under a "cursor" key - use embedding for consistency.
type CursorResponse struct {
	NextCursor string `json:"nextCursor,omitempty" example:"eyJpZCI6IjEyMyJ9"`
	PrevCursor string `json:"prevCursor,omitempty" example:"eyJpZCI6IjEyMiJ9"`
	Limit      int    `json:"limit"                example:"20"               minimum:"1" maximum:"200"`
	HasMore    bool   `json:"hasMore"              example:"true"`
}
