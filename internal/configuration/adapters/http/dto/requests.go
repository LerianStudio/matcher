package dto

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Sentinel errors for DTO validation.
var (
	ErrJSONFieldTooLarge  = errors.New("JSON field exceeds maximum size")
	ErrJSONNestingTooDeep = errors.New("JSON field exceeds maximum nesting depth")
	ErrJSONTooManyKeys    = errors.New("JSON field exceeds maximum key count")
	ErrNameWhitespaceOnly = errors.New("name must contain non-whitespace characters")
)

const (
	maxJSONFieldBytes = 64 * 1024
	maxJSONFieldDepth = 5
	maxJSONFieldKeys  = 100
)

// Reconciliation Context DTOs.

// CreateContextRequest is the API request body for creating a reconciliation context.
// @Description Request payload for creating a reconciliation context
type CreateContextRequest struct {
	Name              string                       `json:"name"                       validate:"required,max=100" example:"Bank Reconciliation Q1"               minLength:"1" maxLength:"100"`
	Type              string                       `json:"type"                       validate:"required,oneof=1:1 1:N N:M" example:"1:1"                                                  enums:"1:1,1:N,N:M"`
	Interval          string                       `json:"interval"                   validate:"required,max=100" example:"daily"                                minLength:"1" maxLength:"100"`
	RateID            *string                      `json:"rateId,omitempty"           validate:"omitempty,uuid"   example:"550e8400-e29b-41d4-a716-446655440000" format:"uuid"`
	FeeToleranceAbs   *string                      `json:"feeToleranceAbs,omitempty"                              example:"0.01"`
	FeeTolerancePct   *string                      `json:"feeTolerancePct,omitempty"                              example:"0.5"`
	FeeNormalization  *string                      `json:"feeNormalization,omitempty"                             example:"NET"                                                                enums:"NET,GROSS"`
	AutoMatchOnUpload *bool                        `json:"autoMatchOnUpload,omitempty"                            example:"false"`
	Sources           []CreateContextSourceRequest `json:"sources,omitempty"          validate:"omitempty,max=10,dive" maxItems:"10"`
	Rules             []CreateMatchRuleRequest     `json:"rules,omitempty"            validate:"omitempty,max=50,dive" maxItems:"50"`
}

// CreateContextSourceRequest represents a source created inline with a context.
// @Description Request payload for creating a source inline with a context
type CreateContextSourceRequest struct {
	Name    string         `json:"name"              validate:"required,max=50"                         example:"Primary Bank Account" minLength:"1" maxLength:"50"`
	Type    string         `json:"type"              validate:"required,oneof=LEDGER BANK GATEWAY CUSTOM FETCHER" example:"BANK"        enums:"LEDGER,BANK,GATEWAY,CUSTOM,FETCHER"`
	Side    string         `json:"side"              validate:"required,oneof=LEFT RIGHT"                example:"LEFT"        enums:"LEFT,RIGHT"`
	Config  map[string]any `json:"config"`
	Mapping map[string]any `json:"mapping,omitempty" validate:"omitempty" swaggertype:"object"`
}

// ToDomainInput converts the API request to a domain input struct.
// Callers must validate the request (via ParseBodyAndValidate) before calling this method.
func (req *CreateContextRequest) ToDomainInput() (entities.CreateReconciliationContextInput, error) {
	input := entities.CreateReconciliationContextInput{
		Name:              req.Name,
		Type:              value_objects.ContextType(req.Type),
		Interval:          req.Interval,
		FeeToleranceAbs:   req.FeeToleranceAbs,
		FeeTolerancePct:   req.FeeTolerancePct,
		FeeNormalization:  req.FeeNormalization,
		AutoMatchOnUpload: req.AutoMatchOnUpload,
	}

	if req.RateID != nil {
		parsed, err := uuid.Parse(*req.RateID)
		if err != nil {
			return entities.CreateReconciliationContextInput{}, fmt.Errorf("invalid rateId: %w", err)
		}

		input.RateID = &parsed
	}

	if len(req.Sources) > 0 {
		input.Sources = make([]entities.CreateContextSourceInput, 0, len(req.Sources))
		for _, src := range req.Sources {
			srcInput, err := src.ToDomainInput()
			if err != nil {
				return entities.CreateReconciliationContextInput{}, fmt.Errorf("invalid source: %w", err)
			}

			input.Sources = append(input.Sources, srcInput)
		}
	}

	if len(req.Rules) > 0 {
		input.Rules = make([]entities.CreateMatchRuleInput, 0, len(req.Rules))
		for _, rule := range req.Rules {
			ruleInput, err := rule.ToDomainInput()
			if err != nil {
				return entities.CreateReconciliationContextInput{}, fmt.Errorf("invalid rule: %w", err)
			}

			input.Rules = append(input.Rules, ruleInput)
		}
	}

	return input, nil
}

// ToDomainInput converts the API request to a domain input struct.
func (req *CreateContextSourceRequest) ToDomainInput() (entities.CreateContextSourceInput, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return entities.CreateContextSourceInput{}, ErrNameWhitespaceOnly
	}

	if err := validateJSONField("source.config", req.Config); err != nil {
		return entities.CreateContextSourceInput{}, err
	}

	if err := validateJSONField("source.mapping", req.Mapping); err != nil {
		return entities.CreateContextSourceInput{}, err
	}

	return entities.CreateContextSourceInput{
		Name:    name,
		Type:    value_objects.SourceType(req.Type),
		Side:    sharedfee.MatchingSide(req.Side),
		Config:  req.Config,
		Mapping: req.Mapping,
	}, nil
}

// UpdateContextRequest is the API request body for updating a reconciliation context.
// @Description Request payload for updating a reconciliation context
type UpdateContextRequest struct {
	Name              *string `json:"name,omitempty"              validate:"omitempty,max=100" example:"Bank Reconciliation Q2"               maxLength:"100"`
	Type              *string `json:"type,omitempty"              validate:"omitempty,oneof=1:1 1:N N:M"              example:"1:N"                                   enums:"1:1,1:N,N:M"`
	Interval          *string `json:"interval,omitempty"          validate:"omitempty,max=100"                          example:"weekly"                                 maxLength:"100"`
	Status            *string `json:"status,omitempty"            validate:"omitempty,oneof=ACTIVE PAUSED ARCHIVED" example:"ACTIVE"                                 enums:"ACTIVE,PAUSED,ARCHIVED"`
	RateID            *string `json:"rateId,omitempty"             validate:"omitempty,uuid"   example:"550e8400-e29b-41d4-a716-446655440000" format:"uuid"`
	FeeToleranceAbs   *string `json:"feeToleranceAbs,omitempty"                                example:"0.01"`
	FeeTolerancePct   *string `json:"feeTolerancePct,omitempty"                                example:"0.5"`
	FeeNormalization  *string `json:"feeNormalization,omitempty"                                example:"NET"                                                  enums:"NET,GROSS"`
	AutoMatchOnUpload *bool   `json:"autoMatchOnUpload,omitempty"                              example:"true"`
}

// ToDomainInput converts the API request to a domain input struct.
// Callers must validate the request (via ParseBodyAndValidate) before calling this method.
func (req *UpdateContextRequest) ToDomainInput() (entities.UpdateReconciliationContextInput, error) {
	input := entities.UpdateReconciliationContextInput{
		Name:              req.Name,
		Interval:          req.Interval,
		FeeToleranceAbs:   req.FeeToleranceAbs,
		FeeTolerancePct:   req.FeeTolerancePct,
		FeeNormalization:  req.FeeNormalization,
		AutoMatchOnUpload: req.AutoMatchOnUpload,
	}

	if req.Type != nil {
		ct := value_objects.ContextType(*req.Type)
		input.Type = &ct
	}

	if req.Status != nil {
		cs := value_objects.ContextStatus(*req.Status)
		input.Status = &cs
	}

	if req.RateID != nil {
		parsed, err := uuid.Parse(*req.RateID)
		if err != nil {
			return entities.UpdateReconciliationContextInput{}, fmt.Errorf("invalid rateId: %w", err)
		}

		input.RateID = &parsed
	}

	return input, nil
}

// Reconciliation Source DTOs.

// CreateSourceRequest is the API request body for creating a reconciliation source.
// @Description Request payload for creating a reconciliation source
type CreateSourceRequest struct {
	Name   string         `json:"name"   validate:"required,max=50"                         example:"Primary Bank Account" minLength:"1" maxLength:"50"`
	Type   string         `json:"type"   validate:"required,oneof=LEDGER BANK GATEWAY CUSTOM FETCHER" example:"BANK"        enums:"LEDGER,BANK,GATEWAY,CUSTOM,FETCHER"`
	Side   string         `json:"side"   validate:"required,oneof=LEFT RIGHT"                example:"LEFT"        enums:"LEFT,RIGHT"`
	Config map[string]any `json:"config"`
}

// ToDomainInput converts the API request to a domain input struct.
// Callers must validate the request (via ParseBodyAndValidate) before calling this method.
func (req *CreateSourceRequest) ToDomainInput() (entities.CreateReconciliationSourceInput, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return entities.CreateReconciliationSourceInput{}, ErrNameWhitespaceOnly
	}

	if err := validateJSONField("source.config", req.Config); err != nil {
		return entities.CreateReconciliationSourceInput{}, err
	}

	return entities.CreateReconciliationSourceInput{
		Name:   name,
		Type:   value_objects.SourceType(req.Type),
		Side:   sharedfee.MatchingSide(req.Side),
		Config: req.Config,
	}, nil
}

// UpdateSourceRequest is the API request body for updating a reconciliation source.
// @Description Request payload for updating a reconciliation source
type UpdateSourceRequest struct {
	Name   *string        `json:"name,omitempty"   validate:"omitempty,max=50"                         example:"Secondary Bank Account" maxLength:"50"`
	Type   *string        `json:"type,omitempty"   validate:"omitempty,oneof=LEDGER BANK GATEWAY CUSTOM FETCHER" example:"LEDGER"        enums:"LEDGER,BANK,GATEWAY,CUSTOM,FETCHER"`
	Side   *string        `json:"side,omitempty"   validate:"omitempty,oneof=LEFT RIGHT"                example:"RIGHT"         enums:"LEFT,RIGHT"`
	Config map[string]any `json:"config,omitempty"`
}

// ToDomainInput converts the API request to a domain input struct.
// Callers must validate the request (via ParseBodyAndValidate) before calling this method.
func (req *UpdateSourceRequest) ToDomainInput() (entities.UpdateReconciliationSourceInput, error) {
	if err := validateJSONField("source.config", req.Config); err != nil {
		return entities.UpdateReconciliationSourceInput{}, err
	}

	input := entities.UpdateReconciliationSourceInput{
		Config: req.Config,
	}

	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		if trimmed == "" {
			return entities.UpdateReconciliationSourceInput{}, ErrNameWhitespaceOnly
		}

		input.Name = &trimmed
	}

	if req.Type != nil {
		st := value_objects.SourceType(*req.Type)
		input.Type = &st
	}

	if req.Side != nil {
		side := sharedfee.MatchingSide(*req.Side)
		input.Side = &side
	}

	return input, nil
}

// Field Map DTOs.

// CreateFieldMapRequest is the API request body for creating a field map.
// @Description Request payload for creating a field mapping configuration
type CreateFieldMapRequest struct {
	Mapping map[string]any `json:"mapping" validate:"required" swaggertype:"object"`
}

// ToDomainInput converts the API request to a domain input struct.
func (r *CreateFieldMapRequest) ToDomainInput() entities.CreateFieldMapInput {
	return entities.CreateFieldMapInput{
		Mapping: r.Mapping,
	}
}

// UpdateFieldMapRequest is the API request body for updating a field map.
// @Description Request payload for updating a field mapping configuration
type UpdateFieldMapRequest struct {
	Mapping map[string]any `json:"mapping" swaggertype:"object"`
}

// ToDomainInput converts the API request to a domain input struct.
func (r *UpdateFieldMapRequest) ToDomainInput() entities.UpdateFieldMapInput {
	return entities.UpdateFieldMapInput{
		Mapping: r.Mapping,
	}
}

// Match Rule DTOs.

// CreateMatchRuleRequest is the API request body for creating a match rule.
// @Description Request payload for creating a matching rule
type CreateMatchRuleRequest struct {
	Priority int            `json:"priority" validate:"required,min=1,max=1000" example:"1"     minimum:"1" maximum:"1000"`
	Type     string         `json:"type"     validate:"required,oneof=EXACT TOLERANCE DATE_LAG" example:"EXACT"            enums:"EXACT,TOLERANCE,DATE_LAG"`
	Config   map[string]any `json:"config"                                                                                                                  swaggertype:"object"`
}

// ToDomainInput converts the API request to a domain input struct.
func (req *CreateMatchRuleRequest) ToDomainInput() (entities.CreateMatchRuleInput, error) {
	if err := validateJSONField("rule.config", req.Config); err != nil {
		return entities.CreateMatchRuleInput{}, err
	}

	return entities.CreateMatchRuleInput{
		Priority: req.Priority,
		Type:     shared.RuleType(req.Type),
		Config:   req.Config,
	}, nil
}

// UpdateMatchRuleRequest is the API request body for updating a match rule.
// @Description Request payload for updating a matching rule
type UpdateMatchRuleRequest struct {
	Priority *int           `json:"priority,omitempty" validate:"omitempty,min=1,max=1000" example:"2"         minimum:"1" maximum:"1000"`
	Type     *string        `json:"type,omitempty"     validate:"omitempty,oneof=EXACT TOLERANCE DATE_LAG" example:"TOLERANCE"  enums:"EXACT,TOLERANCE,DATE_LAG"`
	Config   map[string]any `json:"config,omitempty"                                                                                                                       swaggertype:"object"`
}

// ToDomainInput converts the API request to a domain input struct.
func (req *UpdateMatchRuleRequest) ToDomainInput() entities.UpdateMatchRuleInput {
	input := entities.UpdateMatchRuleInput{
		Priority: req.Priority,
		Config:   req.Config,
	}

	if req.Type != nil {
		rt := shared.RuleType(*req.Type)
		input.Type = &rt
	}

	return input
}

func validateJSONField(fieldPath string, value map[string]any) error {
	if len(value) == 0 {
		return nil
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%s contains non-serializable data: %w", fieldPath, err)
	}

	if len(encoded) > maxJSONFieldBytes {
		return fmt.Errorf("%s exceeds maximum size of %d bytes: %w", fieldPath, maxJSONFieldBytes, ErrJSONFieldTooLarge)
	}

	keyCount := 0
	if err := validateJSONNode(fieldPath, value, 1, &keyCount); err != nil {
		return err
	}

	return nil
}

func validateJSONNode(fieldPath string, node any, depth int, keyCount *int) error {
	if depth > maxJSONFieldDepth {
		return fmt.Errorf("%s exceeds maximum nesting depth of %d: %w", fieldPath, maxJSONFieldDepth, ErrJSONNestingTooDeep)
	}

	switch typed := node.(type) {
	case map[string]any:
		*keyCount += len(typed)
		if *keyCount > maxJSONFieldKeys {
			return fmt.Errorf("%s exceeds maximum key count of %d: %w", fieldPath, maxJSONFieldKeys, ErrJSONTooManyKeys)
		}

		for key, child := range typed {
			if err := validateJSONNode(fieldPath+"."+key, child, depth+1, keyCount); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range typed {
			*keyCount++
			if *keyCount > maxJSONFieldKeys {
				return fmt.Errorf("%s exceeds maximum number of keys (%d): %w", fieldPath, maxJSONFieldKeys, ErrJSONTooManyKeys)
			}

			if err := validateJSONNode(fmt.Sprintf("%s[%d]", fieldPath, i), item, depth+1, keyCount); err != nil {
				return err
			}
		}
	}

	return nil
}

// Schedule DTOs.

// CreateScheduleRequest is the API request body for creating a reconciliation schedule.
// @Description Request payload for creating a cron-based reconciliation schedule
type CreateScheduleRequest struct {
	CronExpression string `json:"cronExpression" validate:"required,max=100" example:"0 0 * * *" minLength:"1" maxLength:"100"`
	Enabled        *bool  `json:"enabled,omitempty"                          example:"true"`
}

// ToDomainInput converts the API request to a domain input struct.
func (r *CreateScheduleRequest) ToDomainInput() entities.CreateScheduleInput {
	return entities.CreateScheduleInput{
		CronExpression: r.CronExpression,
		Enabled:        r.Enabled,
	}
}

// UpdateScheduleRequest is the API request body for updating a reconciliation schedule.
// @Description Request payload for updating a reconciliation schedule
type UpdateScheduleRequest struct {
	CronExpression *string `json:"cronExpression,omitempty" validate:"omitempty,max=100" example:"0 6 * * *" maxLength:"100"`
	Enabled        *bool   `json:"enabled,omitempty"                                     example:"false"`
}

// ToDomainInput converts the API request to a domain input struct.
func (r *UpdateScheduleRequest) ToDomainInput() entities.UpdateScheduleInput {
	return entities.UpdateScheduleInput{
		CronExpression: r.CronExpression,
		Enabled:        r.Enabled,
	}
}
