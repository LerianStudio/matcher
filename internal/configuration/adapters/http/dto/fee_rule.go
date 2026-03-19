package dto

import (
	"time"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// CreateFeeRuleRequest is the request body for POST /v1/config/contexts/:contextId/fee-rules.
type CreateFeeRuleRequest struct {
	Side          string                  `json:"side"          validate:"required,oneof=LEFT RIGHT ANY" example:"RIGHT" enums:"LEFT,RIGHT,ANY"`
	FeeScheduleID string                  `json:"feeScheduleId" validate:"required,uuid"                 example:"550e8400-e29b-41d4-a716-446655440000"`
	Name          string                  `json:"name"          validate:"required,max=100"              example:"BB Right-Side Rule"                   minLength:"1" maxLength:"100"`
	Priority      int                     `json:"priority"      validate:"gte=0"                         example:"0"` // Unique within context; LEFT, RIGHT, and ANY share the same priority space
	Predicates    []FieldPredicateRequest `json:"predicates"    validate:"max=50,dive"`
}

// FieldPredicateRequest represents a single predicate in a fee rule request.
type FieldPredicateRequest struct {
	Field    string   `json:"field"    validate:"required" example:"institution"`
	Operator string   `json:"operator" validate:"required,oneof=EQUALS IN EXISTS" example:"EQUALS" enums:"EQUALS,IN,EXISTS"`
	Value    string   `json:"value,omitempty"              example:"Banco do Brasil"`
	Values   []string `json:"values,omitempty"`
}

// UpdateFeeRuleRequest is the request body for PATCH /v1/config/fee-rules/:feeRuleId.
type UpdateFeeRuleRequest struct {
	Side          *string                  `json:"side,omitempty"          validate:"omitempty,oneof=LEFT RIGHT ANY" example:"LEFT"   enums:"LEFT,RIGHT,ANY"`
	FeeScheduleID *string                  `json:"feeScheduleId,omitempty" validate:"omitempty,uuid"`
	Name          *string                  `json:"name,omitempty"          validate:"omitempty,max=100"              example:"Updated Rule"`
	Priority      *int                     `json:"priority,omitempty"      validate:"omitempty,gte=0"`
	Predicates    *[]FieldPredicateRequest `json:"predicates,omitempty"    validate:"omitempty,max=50,dive"`
}

// FeeRuleResponse is the response body for fee rule endpoints.
type FeeRuleResponse struct {
	ID            string                   `json:"id"            example:"550e8400-e29b-41d4-a716-446655440000"`
	ContextID     string                   `json:"contextId"     example:"550e8400-e29b-41d4-a716-446655440000"`
	Side          string                   `json:"side"          example:"RIGHT"  enums:"LEFT,RIGHT,ANY"`
	FeeScheduleID string                   `json:"feeScheduleId" example:"550e8400-e29b-41d4-a716-446655440000"`
	Name          string                   `json:"name"          example:"BB Right-Side Rule"`
	Priority      int                      `json:"priority"      example:"0"`
	Predicates    []FieldPredicateResponse `json:"predicates"`
	CreatedAt     string                   `json:"createdAt"     example:"2025-01-15T10:30:00Z"`
	UpdatedAt     string                   `json:"updatedAt"     example:"2025-01-15T10:30:00Z"`
}

// FieldPredicateResponse represents a single predicate in a fee rule response.
type FieldPredicateResponse struct {
	Field    string   `json:"field"              example:"institution"`
	Operator string   `json:"operator"           example:"EQUALS" enums:"EQUALS,IN,EXISTS"`
	Value    string   `json:"value,omitempty"    example:"Banco do Brasil"`
	Values   []string `json:"values,omitempty"`
}

// FeeRuleToResponse converts a domain FeeRule to a response DTO.
func FeeRuleToResponse(rule *fee.FeeRule) FeeRuleResponse {
	if rule == nil {
		return FeeRuleResponse{Predicates: []FieldPredicateResponse{}}
	}

	predicates := make([]FieldPredicateResponse, 0, len(rule.Predicates))
	for _, p := range rule.Predicates {
		predicates = append(predicates, FieldPredicateResponse{
			Field:    p.Field,
			Operator: string(p.Operator),
			Value:    p.Value,
			Values:   p.Values,
		})
	}

	return FeeRuleResponse{
		ID:            rule.ID.String(),
		ContextID:     rule.ContextID.String(),
		Side:          string(rule.Side),
		FeeScheduleID: rule.FeeScheduleID.String(),
		Name:          rule.Name,
		Priority:      rule.Priority,
		Predicates:    predicates,
		CreatedAt:     rule.CreatedAt.Format(time.RFC3339),
		UpdatedAt:     rule.UpdatedAt.Format(time.RFC3339),
	}
}

// FeeRulesToResponse converts a slice of domain FeeRules to response DTOs.
func FeeRulesToResponse(rules []*fee.FeeRule) []FeeRuleResponse {
	result := make([]FeeRuleResponse, 0, len(rules))
	for _, r := range rules {
		if r != nil {
			result = append(result, FeeRuleToResponse(r))
		}
	}

	return result
}

// ToPredicates converts request predicates to domain FieldPredicate values.
func ToPredicates(reqs []FieldPredicateRequest) []fee.FieldPredicate {
	if len(reqs) == 0 {
		return nil
	}

	result := make([]fee.FieldPredicate, 0, len(reqs))
	for _, r := range reqs {
		result = append(result, fee.FieldPredicate{
			Field:    r.Field,
			Operator: fee.PredicateOperator(r.Operator),
			Value:    r.Value,
			Values:   r.Values,
		})
	}

	return result
}
