// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

// MatchRunResponse represents a match run in API responses.
// @Description Execution of the matching engine
type MatchRunResponse struct {
	// Unique identifier for the run
	ID string `json:"id"                      example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this run belongs to
	ContextID string `json:"contextId"               example:"550e8400-e29b-41d4-a716-446655440000"`
	// Execution mode
	Mode string `json:"mode"                    example:"DRY_RUN"                              enums:"DRY_RUN,COMMIT"`
	// Current status of the run
	Status string `json:"status"                  example:"COMPLETED"                            enums:"PROCESSING,COMPLETED,FAILED"`
	// When the run started in RFC3339 format
	StartedAt string `json:"startedAt"               example:"2025-01-15T10:30:00Z"`
	// When the run completed in RFC3339 format (if completed)
	CompletedAt *string `json:"completedAt,omitempty"   example:"2025-01-15T10:35:00Z"`
	// Run statistics (matched count, unmatched count, etc.)
	Stats map[string]int `json:"stats,omitempty"`
	// Failure reason if run failed
	FailureReason *string `json:"failureReason,omitempty" example:"no matching rules found"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"               example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"               example:"2025-01-15T10:30:00Z"`
}

// MatchGroupResponse represents a match group in API responses.
// @Description Aggregate of matched transactions
type MatchGroupResponse struct {
	// Unique identifier for the group
	ID string `json:"id"                       example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this group belongs to
	ContextID string `json:"contextId"                example:"550e8400-e29b-41d4-a716-446655440000"`
	// Run ID that created this group
	RunID string `json:"runId"                    example:"550e8400-e29b-41d4-a716-446655440000"`
	// Rule ID used for matching (null for manual matches)
	RuleID *string `json:"ruleId,omitempty"          example:"550e8400-e29b-41d4-a716-446655440000"`
	// Confidence score (0-100)
	Confidence int `json:"confidence"               example:"85"                                    minimum:"0" maximum:"100"`
	// Current status of the group
	Status string `json:"status"                   example:"PROPOSED"                                                        enums:"PROPOSED,CONFIRMED,REJECTED"`
	// Matched items in this group
	Items []MatchItemResponse `json:"items"                              maxItems:"500"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"                example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"                example:"2025-01-15T10:30:00Z"`
	// Reason for rejection if rejected
	RejectedReason *string `json:"rejectedReason,omitempty" example:"amounts do not match within tolerance"`
	// When the group was confirmed in RFC3339 format
	ConfirmedAt *string `json:"confirmedAt,omitempty"    example:"2025-01-15T10:35:00Z"`
}

// MatchItemResponse represents a match item in API responses.
// @Description Allocation of a transaction within a match group
type MatchItemResponse struct {
	// Unique identifier for the item
	ID string `json:"id"                example:"550e8400-e29b-41d4-a716-446655440000"`
	// Match group ID this item belongs to
	MatchGroupID string `json:"matchGroupId"      example:"550e8400-e29b-41d4-a716-446655440000"`
	// Transaction ID being matched
	TransactionID string `json:"transactionId"     example:"550e8400-e29b-41d4-a716-446655440000"`
	// Amount allocated in this match
	AllocatedAmount string `json:"allocatedAmount"   example:"1000.00"`
	// Currency of the allocated amount
	AllocatedCurrency string `json:"allocatedCurrency" example:"USD"`
	// Expected amount for full match
	ExpectedAmount string `json:"expectedAmount"    example:"1000.00"`
	// Whether partial allocation is allowed
	AllowPartial bool `json:"allowPartial"      example:"false"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"         example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"         example:"2025-01-15T10:30:00Z"`
}

// AdjustmentResponse represents an adjustment in API responses.
// @Description Balancing journal entry for variance resolution
type AdjustmentResponse struct {
	// Unique identifier for the adjustment
	ID string `json:"id"                      example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this adjustment belongs to
	ContextID string `json:"contextId"               example:"550e8400-e29b-41d4-a716-446655440000"`
	// Match group ID if adjustment is for a group
	MatchGroupID *string `json:"matchGroupId,omitempty"  example:"550e8400-e29b-41d4-a716-446655440000"`
	// Transaction ID if adjustment is for a single transaction
	TransactionID *string `json:"transactionId,omitempty" example:"550e8400-e29b-41d4-a716-446655440000"`
	// Type of adjustment
	Type string `json:"type"                    example:"BANK_FEE"                             enums:"BANK_FEE,FX_DIFFERENCE,ROUNDING,WRITE_OFF,MISCELLANEOUS"`
	// Adjustment amount
	Amount string `json:"amount"                  example:"10.50"`
	// Currency of the adjustment
	Currency string `json:"currency"                example:"USD"`
	// Description of the adjustment
	Description string `json:"description"             example:"Bank wire fee adjustment"`
	// Reason for the adjustment
	Reason string `json:"reason"                  example:"Variance due to bank processing fee"`
	// User who created the adjustment
	CreatedBy string `json:"createdBy"               example:"user@example.com"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"               example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"               example:"2025-01-15T10:30:00Z"`
}
