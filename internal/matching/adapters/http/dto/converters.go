package dto

import (
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// MatchRunToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func MatchRunToResponse(run *entities.MatchRun) MatchRunResponse {
	if run == nil {
		return MatchRunResponse{Stats: map[string]int{}}
	}

	var completedAt *string

	if run.CompletedAt != nil {
		s := run.CompletedAt.Format(time.RFC3339)
		completedAt = &s
	}

	stats := run.Stats

	if stats == nil {
		stats = map[string]int{}
	}

	return MatchRunResponse{
		ID:            run.ID.String(),
		ContextID:     run.ContextID.String(),
		Mode:          run.Mode.String(),
		Status:        run.Status.String(),
		StartedAt:     run.StartedAt.Format(time.RFC3339),
		CompletedAt:   completedAt,
		Stats:         stats,
		FailureReason: run.FailureReason,
		CreatedAt:     run.CreatedAt.Format(time.RFC3339),
		UpdatedAt:     run.UpdatedAt.Format(time.RFC3339),
	}
}

// MatchRunsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func MatchRunsToResponse(runs []*entities.MatchRun) []MatchRunResponse {
	result := make([]MatchRunResponse, 0, len(runs))

	for _, run := range runs {
		if run != nil {
			result = append(result, MatchRunToResponse(run))
		}
	}

	return result
}

// MatchGroupToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func MatchGroupToResponse(group *entities.MatchGroup) MatchGroupResponse {
	if group == nil {
		return MatchGroupResponse{Items: []MatchItemResponse{}}
	}

	var confirmedAt *string

	if group.ConfirmedAt != nil {
		s := group.ConfirmedAt.Format(time.RFC3339)
		confirmedAt = &s
	}

	var ruleID *string

	if group.RuleID != uuid.Nil {
		s := group.RuleID.String()
		ruleID = &s
	}

	items := MatchItemsToResponse(group.Items)

	return MatchGroupResponse{
		ID:             group.ID.String(),
		ContextID:      group.ContextID.String(),
		RunID:          group.RunID.String(),
		RuleID:         ruleID,
		Confidence:     group.Confidence.Value(),
		Status:         group.Status.String(),
		Items:          items,
		CreatedAt:      group.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      group.UpdatedAt.Format(time.RFC3339),
		RejectedReason: group.RejectedReason,
		ConfirmedAt:    confirmedAt,
	}
}

// MatchGroupsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func MatchGroupsToResponse(groups []*entities.MatchGroup) []MatchGroupResponse {
	result := make([]MatchGroupResponse, 0, len(groups))

	for _, group := range groups {
		if group != nil {
			result = append(result, MatchGroupToResponse(group))
		}
	}

	return result
}

// MatchItemToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func MatchItemToResponse(item *entities.MatchItem) MatchItemResponse {
	if item == nil {
		return MatchItemResponse{}
	}

	return MatchItemResponse{
		ID:                item.ID.String(),
		MatchGroupID:      item.MatchGroupID.String(),
		TransactionID:     item.TransactionID.String(),
		AllocatedAmount:   item.AllocatedAmount.String(),
		AllocatedCurrency: item.AllocatedCurrency,
		ExpectedAmount:    item.ExpectedAmount.String(),
		AllowPartial:      item.AllowPartial,
		CreatedAt:         item.CreatedAt.Format(time.RFC3339),
		UpdatedAt:         item.UpdatedAt.Format(time.RFC3339),
	}
}

// MatchItemsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func MatchItemsToResponse(items []*entities.MatchItem) []MatchItemResponse {
	result := make([]MatchItemResponse, 0, len(items))

	for _, item := range items {
		if item != nil {
			result = append(result, MatchItemToResponse(item))
		}
	}

	return result
}

// AdjustmentToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func AdjustmentToResponse(adj *entities.Adjustment) AdjustmentResponse {
	if adj == nil {
		return AdjustmentResponse{}
	}

	var matchGroupID *string

	if adj.MatchGroupID != nil {
		s := adj.MatchGroupID.String()
		matchGroupID = &s
	}

	var transactionID *string

	if adj.TransactionID != nil {
		s := adj.TransactionID.String()
		transactionID = &s
	}

	return AdjustmentResponse{
		ID:            adj.ID.String(),
		ContextID:     adj.ContextID.String(),
		MatchGroupID:  matchGroupID,
		TransactionID: transactionID,
		Type:          adj.Type.String(),
		Amount:        adj.Amount.String(),
		Currency:      adj.Currency,
		Description:   adj.Description,
		Reason:        adj.Reason,
		CreatedBy:     adj.CreatedBy,
		CreatedAt:     adj.CreatedAt.Format(time.RFC3339),
		UpdatedAt:     adj.UpdatedAt.Format(time.RFC3339),
	}
}

// AdjustmentsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func AdjustmentsToResponse(adjustments []*entities.Adjustment) []AdjustmentResponse {
	result := make([]AdjustmentResponse, 0, len(adjustments))

	for _, adj := range adjustments {
		if adj != nil {
			result = append(result, AdjustmentToResponse(adj))
		}
	}

	return result
}
