// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

import (
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

// ExceptionToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func ExceptionToResponse(exception *entities.Exception) ExceptionResponse {
	if exception == nil {
		return ExceptionResponse{}
	}

	var dueAt *string

	if exception.DueAt != nil {
		dueAt = pointers.String(exception.DueAt.Format(time.RFC3339))
	}

	return ExceptionResponse{
		ID:               exception.ID.String(),
		TransactionID:    exception.TransactionID.String(),
		Severity:         exception.Severity.String(),
		Status:           exception.Status.String(),
		ExternalSystem:   exception.ExternalSystem,
		ExternalIssueID:  exception.ExternalIssueID,
		AssignedTo:       exception.AssignedTo,
		DueAt:            dueAt,
		ResolutionNotes:  exception.ResolutionNotes,
		ResolutionType:   exception.ResolutionType,
		ResolutionReason: exception.ResolutionReason,
		Reason:           exception.Reason,
		CreatedAt:        exception.CreatedAt.Format(time.RFC3339),
		UpdatedAt:        exception.UpdatedAt.Format(time.RFC3339),
	}
}

// ExceptionsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func ExceptionsToResponse(exceptions []*entities.Exception) []ExceptionResponse {
	result := make([]ExceptionResponse, 0, len(exceptions))

	for _, exception := range exceptions {
		if exception != nil {
			result = append(result, ExceptionToResponse(exception))
		}
	}

	return result
}

// EvidenceToResponse converts an Evidence value object to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func EvidenceToResponse(evidence *dispute.Evidence) EvidenceResponse {
	if evidence == nil {
		return EvidenceResponse{}
	}

	return EvidenceResponse{
		ID:          evidence.ID.String(),
		DisputeID:   evidence.DisputeID.String(),
		Comment:     evidence.Comment,
		SubmittedBy: evidence.SubmittedBy,
		FileURL:     evidence.FileURL,
		SubmittedAt: evidence.SubmittedAt.Format(time.RFC3339),
	}
}

// EvidenceListToResponse converts a slice of Evidence to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func EvidenceListToResponse(evidenceList []dispute.Evidence) []EvidenceResponse {
	result := make([]EvidenceResponse, 0, len(evidenceList))

	for i := range evidenceList {
		result = append(result, EvidenceToResponse(&evidenceList[i]))
	}

	return result
}

// DisputeToResponse converts a Dispute entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func DisputeToResponse(disputeEntity *dispute.Dispute) DisputeResponse {
	if disputeEntity == nil {
		return DisputeResponse{Evidence: []EvidenceResponse{}}
	}

	evidence := EvidenceListToResponse(disputeEntity.Evidence)

	return DisputeResponse{
		ID:           disputeEntity.ID.String(),
		ExceptionID:  disputeEntity.ExceptionID.String(),
		Category:     disputeEntity.Category.String(),
		State:        disputeEntity.State.String(),
		Description:  disputeEntity.Description,
		OpenedBy:     disputeEntity.OpenedBy,
		Resolution:   disputeEntity.Resolution,
		ReopenReason: disputeEntity.ReopenReason,
		Evidence:     evidence,
		CreatedAt:    disputeEntity.CreatedAt.Format(time.RFC3339),
		UpdatedAt:    disputeEntity.UpdatedAt.Format(time.RFC3339),
	}
}

// DisputesToResponse converts a slice of Dispute entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func DisputesToResponse(disputes []*dispute.Dispute) []DisputeResponse {
	result := make([]DisputeResponse, 0, len(disputes))

	for _, d := range disputes {
		if d != nil {
			result = append(result, DisputeToResponse(d))
		}
	}

	return result
}
