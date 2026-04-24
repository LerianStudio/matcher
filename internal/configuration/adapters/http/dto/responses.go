// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

// ReconciliationContextResponse represents a reconciliation context in API responses.
// @Description Configuration context for matching rules
type ReconciliationContextResponse struct {
	// Unique identifier for the context
	ID string `json:"id"               example:"550e8400-e29b-41d4-a716-446655440000"`
	// Tenant ID this context belongs to
	TenantID string `json:"tenantId"         example:"550e8400-e29b-41d4-a716-446655440000"`
	// Name of the reconciliation context
	Name string `json:"name"             example:"Bank Reconciliation Q1"`
	// Reconciliation topology type
	Type string `json:"type"             example:"1:1"                                  enums:"1:1,1:N,N:M"`
	// Execution interval
	Interval string `json:"interval"         example:"daily"`
	// Current status of the context
	Status string `json:"status"           example:"ACTIVE"                               enums:"DRAFT,ACTIVE,PAUSED,ARCHIVED"`
	// Absolute fee tolerance amount
	FeeToleranceAbs string `json:"feeToleranceAbs"  example:"0.50"`
	// Percentage fee tolerance
	FeeTolerancePct string `json:"feeTolerancePct"  example:"0.01"`
	// Fee normalization mode
	FeeNormalization string `json:"feeNormalization,omitempty" example:"NET" enums:"NET,GROSS"`
	// Whether to auto-trigger matching after file upload
	AutoMatchOnUpload bool `json:"autoMatchOnUpload" example:"false"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"        example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"        example:"2025-01-15T10:30:00Z"`
}

// ReconciliationSourceResponse represents a reconciliation source in API responses.
// @Description External source for reconciliation
type ReconciliationSourceResponse struct {
	// Unique identifier for the source
	ID string `json:"id"        example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this source belongs to
	ContextID string `json:"contextId" example:"550e8400-e29b-41d4-a716-446655440000"`
	// Name of the source
	Name string `json:"name"      example:"Primary Bank Account"`
	// Type of the source
	Type string `json:"type"      example:"BANK"                                 enums:"LEDGER,BANK,GATEWAY,CUSTOM"`
	// Matching side configured for the source
	Side string `json:"side"      example:"LEFT"                                 enums:"LEFT,RIGHT"`
	// Source configuration
	Config map[string]any `json:"config"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt" example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt" example:"2025-01-15T10:30:00Z"`
}

// SourceWithFieldMapStatusResponse extends ReconciliationSourceResponse with field map status.
// @Description Source with field map availability information
type SourceWithFieldMapStatusResponse struct {
	ReconciliationSourceResponse
	// Indicates if field maps are configured for this source
	HasFieldMaps bool `json:"hasFieldMaps" example:"true"`
}

// FieldMapResponse represents a field map in API responses.
// @Description Field mapping configuration for a source
type FieldMapResponse struct {
	// Unique identifier for the field map
	ID string `json:"id"        example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this field map belongs to
	ContextID string `json:"contextId" example:"550e8400-e29b-41d4-a716-446655440000"`
	// Source ID this field map is for
	SourceID string `json:"sourceId"  example:"550e8400-e29b-41d4-a716-446655440000"`
	// Field mapping configuration
	Mapping map[string]any `json:"mapping"`
	// Version number for optimistic locking
	Version int `json:"version"   example:"1"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt" example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt" example:"2025-01-15T10:30:00Z"`
}

// MatchRuleResponse represents a match rule in API responses.
// @Description Matching rule within a reconciliation context
type MatchRuleResponse struct {
	// Unique identifier for the rule
	ID string `json:"id"        example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this rule belongs to
	ContextID string `json:"contextId" example:"550e8400-e29b-41d4-a716-446655440000"`
	// Execution priority (lower = higher priority)
	Priority int `json:"priority"  example:"1"                                    minimum:"1" maximum:"1000"`
	// Type of matching rule
	Type string `json:"type"      example:"EXACT"                                                           enums:"EXACT,TOLERANCE,DATE_LAG"`
	// Rule configuration
	Config map[string]any `json:"config"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt" example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt" example:"2025-01-15T10:30:00Z"`
}
