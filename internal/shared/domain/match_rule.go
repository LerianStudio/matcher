// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// RuleType defines the type of matching rule.
// @Description Type of matching rule algorithm
// @Enum EXACT,TOLERANCE,DATE_LAG
// swagger:enum RuleType
type RuleType string

// Supported rule types for matching operations.
const (
	RuleTypeExact     RuleType = "EXACT"
	RuleTypeTolerance RuleType = "TOLERANCE"
	RuleTypeDateLag   RuleType = "DATE_LAG"
)

// ErrInvalidRuleType indicates an invalid rule type value.
var ErrInvalidRuleType = errors.New("invalid rule type")

// Valid reports whether the rule type is supported.
func (rt RuleType) Valid() bool {
	switch rt {
	case RuleTypeExact, RuleTypeTolerance, RuleTypeDateLag:
		return true
	}

	return false
}

// IsValid reports whether the rule type is supported.
// This is an alias for Valid() to maintain API consistency.
func (rt RuleType) IsValid() bool {
	return rt.Valid()
}

func (rt RuleType) String() string {
	return string(rt)
}

// ParseRuleType parses a string into a RuleType (case-insensitive).
func ParseRuleType(s string) (RuleType, error) {
	rt := RuleType(strings.ToUpper(strings.TrimSpace(s)))
	if !rt.Valid() {
		return "", fmt.Errorf("%w: %q", ErrInvalidRuleType, s)
	}

	return rt, nil
}

// ContextType defines the reconciliation topology for a context.
// @Description Reconciliation topology (cardinality between sources)
// @Enum 1:1,1:N,N:M
// swagger:enum ContextType
type ContextType string

// Supported context types for reconciliation topologies.
const (
	ContextTypeOneToOne   ContextType = "1:1"
	ContextTypeOneToMany  ContextType = "1:N"
	ContextTypeManyToMany ContextType = "N:M"
)

// ErrInvalidContextType indicates an invalid context type value.
var ErrInvalidContextType = errors.New("invalid context type")

// Valid reports whether the context type is supported.
func (ct ContextType) Valid() bool {
	switch ct {
	case ContextTypeOneToOne, ContextTypeOneToMany, ContextTypeManyToMany:
		return true
	}

	return false
}

// IsValid reports whether the context type is supported.
// This is an alias for Valid() to maintain API consistency.
func (ct ContextType) IsValid() bool {
	return ct.Valid()
}

func (ct ContextType) String() string {
	return string(ct)
}

// ParseContextType parses a string into a ContextType.
func ParseContextType(s string) (ContextType, error) {
	ct := ContextType(strings.TrimSpace(s))
	if !ct.Valid() {
		return "", fmt.Errorf("%w: %q", ErrInvalidContextType, s)
	}

	return ct, nil
}

// Match rule errors.
var (
	ErrMatchRuleNil        = errors.New("match rule is nil")
	ErrRuleContextRequired = errors.New("context_id is required")
	ErrRulePriorityInvalid = errors.New("priority must be between 1 and 1000")
	ErrRuleTypeInvalid     = errors.New("invalid rule type")
	ErrRuleConfigRequired  = errors.New("config is required")
)

// MatchRule represents a reconciliation rule within a context.
// This is a shared kernel type used by both Configuration and Matching contexts.
type MatchRule struct {
	ID        uuid.UUID
	ContextID uuid.UUID
	Priority  int
	Type      RuleType
	Config    map[string]any
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MaxPriority defines the upper bound for rule priorities.
const MaxPriority = 1000

// CreateMatchRuleInput defines the input required to create a match rule.
type CreateMatchRuleInput struct {
	Priority int            `json:"priority" validate:"required,min=1,max=1000" example:"1"     minimum:"1" maximum:"1000"`
	Type     RuleType       `json:"type"     validate:"required"                example:"EXACT"                            enums:"EXACT,TOLERANCE,DATE_LAG"`
	Config   map[string]any `json:"config"                                                                                                                  swaggertype:"object"`
}

// UpdateMatchRuleInput defines the fields that can be updated on a match rule.
type UpdateMatchRuleInput struct {
	Priority *int           `json:"priority,omitempty" validate:"omitempty,min=1,max=1000" example:"2"         minimum:"1" maximum:"1000"`
	Type     *RuleType      `json:"type,omitempty"                                         example:"TOLERANCE"                            enums:"EXACT,TOLERANCE,DATE_LAG"`
	Config   map[string]any `json:"config,omitempty"                                                                                                                       swaggertype:"object"`
}

// NewMatchRule validates input and returns a new match rule entity.
func NewMatchRule(
	ctx context.Context,
	contextID uuid.UUID,
	input CreateMatchRuleInput,
) (*MatchRule, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "shared.match_rule.new")

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, fmt.Errorf("match rule context id: %w", ErrRuleContextRequired)
	}

	if err := asserter.That(ctx, input.Priority >= 1 && input.Priority <= MaxPriority, "priority must be between 1 and 1000", "priority", input.Priority); err != nil {
		return nil, ErrRulePriorityInvalid
	}

	if err := asserter.That(ctx, input.Type.Valid(), "invalid rule type", "type", input.Type.String()); err != nil {
		return nil, ErrRuleTypeInvalid
	}

	if len(input.Config) == 0 {
		return nil, ErrRuleConfigRequired
	}

	now := time.Now().UTC()

	return &MatchRule{
		ID:        uuid.New(),
		ContextID: contextID,
		Priority:  input.Priority,
		Type:      input.Type,
		Config:    input.Config,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// Update applies match rule updates.
func (mr *MatchRule) Update(ctx context.Context, input UpdateMatchRuleInput) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "shared.match_rule.update")

	if err := asserter.NotNil(ctx, mr, "match rule is required"); err != nil {
		return ErrMatchRuleNil
	}

	if input.Priority != nil {
		if *input.Priority < 1 || *input.Priority > MaxPriority {
			return ErrRulePriorityInvalid
		}

		mr.Priority = *input.Priority
	}

	if input.Type != nil {
		if !input.Type.Valid() {
			return ErrRuleTypeInvalid
		}

		mr.Type = *input.Type
	}

	if input.Config != nil {
		if len(input.Config) == 0 {
			return ErrRuleConfigRequired
		}

		mr.Config = input.Config
	}

	mr.UpdatedAt = time.Now().UTC()

	return nil
}

// ConfigJSON marshals the match rule configuration to JSON.
func (mr *MatchRule) ConfigJSON() ([]byte, error) {
	if mr == nil {
		return json.Marshal(nil)
	}

	return json.Marshal(mr.Config)
}

// MatchRules is a sortable slice of match rules ordered by priority.
type MatchRules []*MatchRule

func (mr MatchRules) Len() int { return len(mr) }

func (mr MatchRules) Less(i, j int) bool {
	if mr[i] == nil && mr[j] == nil {
		return false
	}

	if mr[i] == nil {
		return false
	}

	if mr[j] == nil {
		return true
	}

	return mr[i].Priority < mr[j].Priority
}

func (mr MatchRules) Swap(i, j int) { mr[i], mr[j] = mr[j], mr[i] }
