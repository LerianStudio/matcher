// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

import (
	"time"

	"github.com/shopspring/decimal"
)

// ForceMatchRequest represents the payload for force matching an exception.
// @Description Force match request payload
type ForceMatchRequest struct {
	// Reason for overriding the normal matching process
	OverrideReason string `json:"overrideReason" validate:"required,max=255"  example:"BUSINESS_DECISION"`
	// Additional notes explaining the force match decision
	Notes string `json:"notes"          validate:"required,max=1000" example:"Approved by finance team after manual review"`
}

// AdjustEntryRequest represents the payload for adjusting an entry to resolve an exception.
// @Description Adjust entry request payload
type AdjustEntryRequest struct {
	// Reason code for the adjustment
	ReasonCode string `json:"reasonCode"  validate:"required,max=255"          example:"FEE_ADJUSTMENT"`
	// Additional notes explaining the adjustment
	Notes string `json:"notes"       validate:"required,max=1000"         example:"Correcting processing fee discrepancy"`
	// Adjustment amount
	Amount decimal.Decimal `json:"amount"      validate:"required,positive_decimal" example:"150.50"`
	// Currency code (ISO 4217)
	Currency string `json:"currency"    validate:"required,len=3"            example:"USD"`
	// When the adjustment takes effect
	EffectiveAt time.Time `json:"effectiveAt" validate:"required"                                                                  format:"date-time"`
}

// OpenDisputeRequest represents the payload for opening a dispute.
// @Description Open dispute request payload
type OpenDisputeRequest struct {
	// Category of the dispute
	Category string `json:"category"    validate:"required,max=255"  example:"AMOUNT_MISMATCH"`
	// Detailed description of the dispute
	Description string `json:"description" validate:"required,max=5000" example:"Transaction amount differs from invoice"`
}

// CloseDisputeRequest represents the payload for closing a dispute.
// @Description Close dispute request payload
type CloseDisputeRequest struct {
	// Whether the dispute was won
	Won bool `json:"won"        example:"true"`
	// Resolution description
	Resolution string `json:"resolution" example:"Counterparty acknowledged the error and issued correction" validate:"required,max=5000"`
}

// SubmitEvidenceRequest represents the payload for submitting evidence to a dispute.
// @Description Submit evidence request payload
type SubmitEvidenceRequest struct {
	// Comment describing the evidence
	Comment string `json:"comment"           validate:"required,max=1000"  example:"Attached bank statement showing correct amount"`
	// Optional URL to evidence file
	FileURL *string `json:"fileUrl,omitempty" validate:"omitempty,max=2048" example:"https://storage.example.com/evidence/doc123.pdf"`
}

// DispatchRequest represents the payload for dispatching an exception to an external system.
// @Description Dispatch request payload
type DispatchRequest struct {
	// Target system to dispatch to
	TargetSystem string `json:"targetSystem"    validate:"required,max=255"  example:"JIRA"       enums:"JIRA,SERVICENOW,WEBHOOK,MANUAL"`
	// Optional queue or team assignment
	Queue string `json:"queue,omitempty" validate:"omitempty,max=255" example:"RECON-TEAM"`
}
