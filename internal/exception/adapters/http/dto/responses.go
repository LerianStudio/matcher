package dto

import (
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// CursorResponse is an alias for the shared cursor pagination type.
type CursorResponse = sharedhttp.CursorResponse

// ExceptionResponse represents an exception in API responses.
// @Description Exception details
type ExceptionResponse struct {
	// Unique identifier for the exception
	ID string `json:"id"                         example:"550e8400-e29b-41d4-a716-446655440000"`
	// Transaction ID this exception is for
	TransactionID string `json:"transactionId"              example:"550e8400-e29b-41d4-a716-446655440001"`
	// Severity level
	Severity string `json:"severity"                   example:"HIGH"                                 enums:"LOW,MEDIUM,HIGH,CRITICAL"`
	// Current status
	Status string `json:"status"                     example:"OPEN"                                 enums:"OPEN,ASSIGNED,RESOLVED"`
	// External system where exception was dispatched
	ExternalSystem *string `json:"externalSystem,omitempty"   example:"JIRA"`
	// External issue ID in the external system
	ExternalIssueID *string `json:"externalIssueId,omitempty"  example:"RECON-1234"`
	// User the exception is assigned to
	AssignedTo *string `json:"assignedTo,omitempty"       example:"user@example.com"`
	// Due date for resolution in RFC3339 format
	DueAt *string `json:"dueAt,omitempty"            example:"2025-01-20T10:30:00Z"`
	// Resolution notes when resolved
	ResolutionNotes *string `json:"resolutionNotes,omitempty"  example:"Resolved via force match"`
	// Type of resolution applied
	ResolutionType *string `json:"resolutionType,omitempty"   example:"FORCE_MATCH"                          enums:"FORCE_MATCH,ADJUST_ENTRY"`
	// Reason for the resolution
	ResolutionReason *string `json:"resolutionReason,omitempty" example:"BUSINESS_DECISION"`
	// Reason the exception was raised
	Reason *string `json:"reason,omitempty"           example:"Amount mismatch detected"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"                  example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"                  example:"2025-01-15T10:30:00Z"`
}

// EvidenceResponse represents evidence in API responses.
// @Description Evidence details
type EvidenceResponse struct {
	// Unique identifier for the evidence
	ID string `json:"id"                example:"550e8400-e29b-41d4-a716-446655440000"`
	// Dispute ID this evidence belongs to
	DisputeID string `json:"disputeId"         example:"550e8400-e29b-41d4-a716-446655440001"`
	// Comment describing the evidence
	Comment string `json:"comment"           example:"Bank statement attached"`
	// User who submitted the evidence
	SubmittedBy string `json:"submittedBy"       example:"user@example.com"`
	// URL to evidence file
	FileURL *string `json:"fileUrl,omitempty" example:"https://storage.example.com/evidence/doc123.pdf"`
	// When the evidence was submitted in RFC3339 format
	SubmittedAt string `json:"submittedAt"       example:"2025-01-15T10:30:00Z"`
}

// DisputeResponse represents a dispute in API responses.
// @Description Dispute details
type DisputeResponse struct {
	// Unique identifier for the dispute
	ID string `json:"id"                     example:"550e8400-e29b-41d4-a716-446655440000"`
	// Exception ID this dispute is for
	ExceptionID string `json:"exceptionId"            example:"550e8400-e29b-41d4-a716-446655440001"`
	// Category of the dispute
	Category string `json:"category"               example:"BANK_FEE_ERROR"                           enums:"BANK_FEE_ERROR,UNRECOGNIZED_CHARGE,DUPLICATE_TRANSACTION,OTHER"`
	// Current state
	State string `json:"state"                  example:"OPEN"                                     enums:"DRAFT,OPEN,PENDING_EVIDENCE,WON,LOST"`
	// Description of the dispute
	Description string `json:"description"            example:"Transaction amount differs from expected"`
	// User who opened the dispute
	OpenedBy string `json:"openedBy"               example:"user@example.com"`
	// Resolution description when closed
	Resolution *string `json:"resolution,omitempty"   example:"Counterparty confirmed error"`
	// Reason for reopening if reopened
	ReopenReason *string `json:"reopenReason,omitempty" example:"New evidence discovered"`
	// Evidence submitted for this dispute (max 50 items)
	Evidence []EvidenceResponse `json:"evidence,omitempty" validate:"omitempty,max=50" maxItems:"50"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"              example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"              example:"2025-01-15T10:30:00Z"`
}

// ListExceptionsResponse represents a paginated list of exceptions.
// @Description Paginated list of exceptions
type ListExceptionsResponse struct {
	// List of exceptions
	Items []ExceptionResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	CursorResponse
}

// ListDisputesResponse represents a paginated list of disputes.
// @Description Paginated list of disputes
type ListDisputesResponse struct {
	// List of disputes
	Items []DisputeResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	CursorResponse
}

// DispatchResponse represents the result of dispatching an exception.
// @Description Dispatch result
type DispatchResponse struct {
	// Exception ID that was dispatched
	ExceptionID string `json:"exceptionId"                 example:"550e8400-e29b-41d4-a716-446655440000"`
	// Target system dispatched to
	Target string `json:"target"                      example:"JIRA"`
	// External reference ID from target system
	ExternalReference string `json:"externalReference,omitempty" example:"RECON-1234"`
	// Whether the dispatch was acknowledged
	Acknowledged bool `json:"acknowledged"                example:"true"`
	// When the dispatch occurred in RFC3339 format
	DispatchedAt string `json:"dispatchedAt"                example:"2025-01-15T10:30:00Z"`
}

// HistoryEntryResponse represents an audit log entry in the exception history.
// @Description Audit log entry
type HistoryEntryResponse struct {
	// Unique identifier for the history entry
	ID string `json:"id"                example:"550e8400-e29b-41d4-a716-446655440000"`
	// Action that was performed
	Action string `json:"action"            example:"FORCE_MATCH"`
	// User who performed the action
	ActorID *string `json:"actorId,omitempty" example:"user@example.com"`
	// Changes made in this action (JSON object representing the diff)
	Changes any `json:"changes,omitempty" swaggertype:"object"`
	// When the action occurred in RFC3339 format
	CreatedAt string `json:"createdAt"         example:"2025-01-15T10:30:00Z"`
}

// HistoryResponse represents the audit history for an exception.
// @Description Audit history
type HistoryResponse struct {
	// List of history entries
	Items []HistoryEntryResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	CursorResponse
}
