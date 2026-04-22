package dto

// BulkResolveRequest represents the payload for bulk resolving exceptions.
// @Description Bulk resolve request payload
type BulkResolveRequest struct {
	// Exception IDs to resolve
	ExceptionIDs []string `json:"exceptionIds" validate:"required,min=1,max=100,dive,uuid"`
	// Resolution type applied
	Resolution string `json:"resolution" validate:"required,max=255" example:"ACCEPTED"`
	// Optional reason for the resolution
	Reason string `json:"reason" validate:"omitempty,max=1000" example:"Variance within tolerance"`
}

// BulkAssignRequest represents the payload for bulk assigning exceptions.
// @Description Bulk assign request payload
type BulkAssignRequest struct {
	// Exception IDs to assign
	ExceptionIDs []string `json:"exception_ids" validate:"required,min=1,max=100,dive,uuid"`
	// User to assign exceptions to
	Assignee string `json:"assignee" validate:"required,max=255" example:"user@example.com"`
}

// BulkDispatchRequest represents the payload for bulk dispatching exceptions.
// @Description Bulk dispatch request payload
type BulkDispatchRequest struct {
	// Exception IDs to dispatch
	ExceptionIDs []string `json:"exception_ids" validate:"required,min=1,max=100,dive,uuid"`
	// Target system to dispatch to
	TargetSystem string `json:"target_system" validate:"required,max=255" example:"JIRA"`
	// Optional queue or team assignment
	Queue string `json:"queue,omitempty" validate:"omitempty,max=255" example:"RECON-TEAM"`
}

// BulkActionResponse represents the result of a bulk operation.
// @Description Bulk action result
type BulkActionResponse struct {
	// IDs of exceptions that were successfully processed
	Succeeded []string `json:"succeeded"`
	// Details of exceptions that failed processing
	Failed []BulkFailure `json:"failed"`
	// Total number of exceptions attempted
	Total int `json:"total" example:"5"`
}

// BulkFailure represents a single failure in a bulk operation.
// @Description Single failure in bulk operation
type BulkFailure struct {
	// Exception ID that failed
	ExceptionID string `json:"exceptionId" example:"550e8400-e29b-41d4-a716-446655440000"`
	// Error description
	Error string `json:"error" example:"exception not found"`
}
