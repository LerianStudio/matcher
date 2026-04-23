// Package entities holds configuration domain entities.
//
// ADR: Domain DTOs with Swagger Tags
//
// SUPERSEDED (2026-02-11): Dedicated request DTOs introduced in adapters/http/dto/requests.go.
//
// Trigger: SDK/code generators produce unusable type names from domain packages
// (e.g., "github_com_LerianStudio_matcher_internal_configuration_domain_entities.CreateReconciliationContextInput").
// This leaks internal package structure into the public API contract.
//
// Resolution: HTTP handlers now use DTO request structs (dto.CreateContextRequest, etc.)
// with ToDomainInput() converters. The domain input structs below remain as the
// service-layer contract -- they are no longer referenced directly in swagger annotations.
//
// The json/validate tags on domain input structs are retained for compatibility
// with any non-HTTP callers and for the domain constructor validation logic.
//
// Original decision date: 2026-02-10
// Superseded date: 2026-02-11.
package entities

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// maxContextNameLength defines the maximum allowed length for context names.
const maxContextNameLength = 100

// Reconciliation context errors.
var (
	// ErrNilReconciliationContext is returned when the reconciliation context is nil.
	ErrNilReconciliationContext = errors.New("reconciliation context is nil")
	// ErrContextNameRequired is returned when the context name is not provided.
	ErrContextNameRequired = errors.New("context name is required")
	// ErrContextNameTooLong is returned when the context name exceeds 100 characters.
	ErrContextNameTooLong = errors.New("context name exceeds 100 characters")
	// ErrContextTypeInvalid is returned when the context type is invalid.
	ErrContextTypeInvalid = errors.New("invalid context type")
	// ErrContextStatusInvalid is returned when the context status is invalid.
	ErrContextStatusInvalid = errors.New("invalid context status")
	// ErrContextIntervalRequired is returned when the context interval is not provided.
	ErrContextIntervalRequired = errors.New("context interval is required")
	// ErrContextTenantRequired is returned when the tenant_id is not provided.
	ErrContextTenantRequired = errors.New("tenant_id is required")
	// ErrFeeToleranceAbsInvalid is returned when the fee tolerance absolute value is invalid.
	ErrFeeToleranceAbsInvalid = errors.New("invalid fee tolerance absolute value")
	// ErrFeeTolerancePctInvalid is returned when the fee tolerance percentage value is invalid.
	ErrFeeTolerancePctInvalid = errors.New("invalid fee tolerance percentage value")
	// ErrFeeNormalizationInvalid is returned when the fee normalization mode is invalid.
	ErrFeeNormalizationInvalid = errors.New("invalid fee normalization mode")
	// ErrInvalidStateTransition is returned when a state transition is not allowed.
	ErrInvalidStateTransition = errors.New("invalid state transition")
	// ErrArchivedContextCannotBeModified is returned when attempting to modify an archived context.
	ErrArchivedContextCannotBeModified = errors.New("archived context cannot be modified")
)

// ReconciliationContext represents a configuration context for matching rules.
type ReconciliationContext struct {
	ID                uuid.UUID
	TenantID          uuid.UUID
	Name              string
	Type              shared.ContextType
	Interval          string
	Status            value_objects.ContextStatus
	FeeToleranceAbs   decimal.Decimal
	FeeTolerancePct   decimal.Decimal
	FeeNormalization  *string
	AutoMatchOnUpload bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// CreateReconciliationContextInput defines the input required to create a context.
type CreateReconciliationContextInput struct {
	Name              string                     `json:"name"                      validate:"required,max=100" example:"Bank Reconciliation Q1"               minLength:"1" maxLength:"100"`
	Type              shared.ContextType         `json:"type"                      validate:"required"         example:"1:1"                                                                enums:"1:1,1:N,N:M"`
	Interval          string                     `json:"interval"                  validate:"required,max=100" example:"daily"                                minLength:"1" maxLength:"100"`
	FeeToleranceAbs   *string                    `json:"feeToleranceAbs,omitempty"                             example:"0.01"`
	FeeTolerancePct   *string                    `json:"feeTolerancePct,omitempty"                             example:"0.5"`
	FeeNormalization  *string                    `json:"feeNormalization,omitempty"                            example:"NET"                                                                enums:"NET,GROSS"`
	AutoMatchOnUpload *bool                      `json:"autoMatchOnUpload,omitempty"                           example:"false"`
	Sources           []CreateContextSourceInput `json:"sources,omitempty"`
	Rules             []CreateMatchRuleInput     `json:"rules,omitempty"`
}

// UpdateReconciliationContextInput defines fields that can be updated on a context.
type UpdateReconciliationContextInput struct {
	Name              *string                      `json:"name,omitempty"             validate:"omitempty,max=100" example:"Bank Reconciliation Q2"               maxLength:"100"`
	Type              *shared.ContextType          `json:"type,omitempty"                                          example:"1:N"                                                  enums:"1:1,1:N,N:M"`
	Interval          *string                      `json:"interval,omitempty"         validate:"omitempty,max=100" example:"weekly"                               maxLength:"100"`
	Status            *value_objects.ContextStatus `json:"status,omitempty"                                        example:"ACTIVE"                                               enums:"DRAFT,ACTIVE,PAUSED,ARCHIVED"`
	FeeToleranceAbs   *string                      `json:"feeToleranceAbs,omitempty"                               example:"0.01"`
	FeeTolerancePct   *string                      `json:"feeTolerancePct,omitempty"                               example:"0.5"`
	FeeNormalization  *string                      `json:"feeNormalization,omitempty"                              example:"NET"                                                  enums:"NET,GROSS"`
	AutoMatchOnUpload *bool                        `json:"autoMatchOnUpload,omitempty"                             example:"true"`
}

// NewReconciliationContext validates input and returns a new context entity.
func NewReconciliationContext(
	ctx context.Context,
	tenantID uuid.UUID,
	input CreateReconciliationContextInput,
) (*ReconciliationContext, error) {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_context.new",
	)

	if err := asserter.That(ctx, tenantID != uuid.Nil, "tenant id is required"); err != nil {
		return nil, fmt.Errorf("context tenant id: %w", ErrContextTenantRequired)
	}

	name := strings.TrimSpace(input.Name)
	if err := asserter.That(ctx, name != "", "context name is required"); err != nil {
		return nil, ErrContextNameRequired
	}

	if err := asserter.That(ctx, len(name) <= maxContextNameLength, "context name too long", "name", name); err != nil {
		return nil, ErrContextNameTooLong
	}

	if err := asserter.That(ctx, input.Type.Valid(), "invalid context type", "type", input.Type.String()); err != nil {
		return nil, ErrContextTypeInvalid
	}

	interval := strings.TrimSpace(input.Interval)
	if err := asserter.That(ctx, interval != "", "context interval is required"); err != nil {
		return nil, ErrContextIntervalRequired
	}

	feeToleranceAbs, feeTolerancePct, err := parseFeeTolerances(input.FeeToleranceAbs, input.FeeTolerancePct)
	if err != nil {
		return nil, err
	}

	if input.FeeNormalization != nil && *input.FeeNormalization != "" {
		mode := fee.NormalizationMode(*input.FeeNormalization)
		if !mode.IsValid() {
			return nil, fmt.Errorf("fee normalization: %w", ErrFeeNormalizationInvalid)
		}
	}

	now := time.Now().UTC()

	var autoMatchOnUpload bool
	if input.AutoMatchOnUpload != nil {
		autoMatchOnUpload = *input.AutoMatchOnUpload
	}

	return &ReconciliationContext{
		ID:                uuid.New(),
		TenantID:          tenantID,
		Name:              name,
		Type:              input.Type,
		Interval:          interval,
		Status:            value_objects.ContextStatusDraft,
		FeeToleranceAbs:   feeToleranceAbs,
		FeeTolerancePct:   feeTolerancePct,
		FeeNormalization:  input.FeeNormalization,
		AutoMatchOnUpload: autoMatchOnUpload,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

func parseFeeTolerances(absRaw, pctRaw *string) (decimal.Decimal, decimal.Decimal, error) {
	feeToleranceAbs, err := parseNonNegativeDecimal(absRaw, ErrFeeToleranceAbsInvalid)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}

	feeTolerancePct, err := parseNonNegativeDecimal(pctRaw, ErrFeeTolerancePctInvalid)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}

	return feeToleranceAbs, feeTolerancePct, nil
}

func parseNonNegativeDecimal(raw *string, sentinel error) (decimal.Decimal, error) {
	if raw == nil {
		return decimal.Zero, nil
	}

	parsed, err := decimal.NewFromString(*raw)
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: parse error: %w", sentinel, err)
	}

	if parsed.IsNegative() {
		return decimal.Zero, sentinel
	}

	return parsed, nil
}

func (rc *ReconciliationContext) updateName(_ context.Context, name *string) error {
	if name == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*name)

	if trimmed == "" {
		return ErrContextNameRequired
	}

	if len(trimmed) > maxContextNameLength {
		return ErrContextNameTooLong
	}

	rc.Name = trimmed

	return nil
}

func (rc *ReconciliationContext) updateType(
	_ context.Context,
	ctxType *shared.ContextType,
) error {
	if ctxType == nil {
		return nil
	}

	if !ctxType.Valid() {
		return ErrContextTypeInvalid
	}

	rc.Type = *ctxType

	return nil
}

func (rc *ReconciliationContext) updateInterval(_ context.Context, interval *string) error {
	if interval == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*interval)

	if trimmed == "" {
		return ErrContextIntervalRequired
	}

	rc.Interval = trimmed

	return nil
}

func (rc *ReconciliationContext) updateStatus(
	ctx context.Context,
	status *value_objects.ContextStatus,
) error {
	if status == nil {
		return nil
	}

	if *status == rc.Status {
		return nil
	}

	switch *status {
	case value_objects.ContextStatusActive:
		return rc.Activate(ctx)
	case value_objects.ContextStatusPaused:
		return rc.Pause(ctx)
	case value_objects.ContextStatusArchived:
		return rc.Archive(ctx)
	default:
		return ErrContextStatusInvalid
	}
}

func (rc *ReconciliationContext) updateFeeTolerances(input UpdateReconciliationContextInput) error {
	if input.FeeToleranceAbs != nil {
		parsed, err := decimal.NewFromString(*input.FeeToleranceAbs)
		if err != nil {
			return fmt.Errorf("%w: parse error: %w", ErrFeeToleranceAbsInvalid, err)
		}

		if parsed.IsNegative() {
			return ErrFeeToleranceAbsInvalid
		}

		rc.FeeToleranceAbs = parsed
	}

	if input.FeeTolerancePct != nil {
		parsed, err := decimal.NewFromString(*input.FeeTolerancePct)
		if err != nil {
			return fmt.Errorf("%w: parse error: %w", ErrFeeTolerancePctInvalid, err)
		}

		if parsed.IsNegative() {
			return ErrFeeTolerancePctInvalid
		}

		rc.FeeTolerancePct = parsed
	}

	if input.FeeNormalization != nil {
		if *input.FeeNormalization != "" {
			mode := fee.NormalizationMode(*input.FeeNormalization)
			if !mode.IsValid() {
				return fmt.Errorf("fee normalization: %w", ErrFeeNormalizationInvalid)
			}
		}

		rc.FeeNormalization = input.FeeNormalization
	}

	return nil
}

// Update applies changes to a reconciliation context.
func (rc *ReconciliationContext) Update(
	ctx context.Context,
	input UpdateReconciliationContextInput,
) error {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_context.update",
	)
	if err := asserter.NotNil(ctx, rc, "reconciliation context is required"); err != nil {
		return ErrNilReconciliationContext
	}

	if rc.Status == value_objects.ContextStatusArchived {
		return ErrArchivedContextCannotBeModified
	}

	if err := rc.updateName(ctx, input.Name); err != nil {
		return err
	}

	if err := rc.updateType(ctx, input.Type); err != nil {
		return err
	}

	if err := rc.updateInterval(ctx, input.Interval); err != nil {
		return err
	}

	if err := rc.updateStatus(ctx, input.Status); err != nil {
		return err
	}

	if err := rc.updateFeeTolerances(input); err != nil {
		return err
	}

	if input.AutoMatchOnUpload != nil {
		rc.AutoMatchOnUpload = *input.AutoMatchOnUpload
	}

	rc.UpdatedAt = time.Now().UTC()

	return nil
}

// Pause marks the context as paused.
// Only allowed from ACTIVE status.
func (rc *ReconciliationContext) Pause(ctx context.Context) error {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_context.pause",
	)
	if err := asserter.NotNil(ctx, rc, "reconciliation context is required"); err != nil {
		return ErrNilReconciliationContext
	}

	if rc.Status != value_objects.ContextStatusActive {
		return fmt.Errorf("cannot pause context in %s status: %w", rc.Status, ErrInvalidStateTransition)
	}

	rc.Status = value_objects.ContextStatusPaused
	rc.UpdatedAt = time.Now().UTC()

	return nil
}

// Activate marks the context as active.
// Only allowed from DRAFT or PAUSED status.
func (rc *ReconciliationContext) Activate(ctx context.Context) error {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_context.activate",
	)
	if err := asserter.NotNil(ctx, rc, "reconciliation context is required"); err != nil {
		return ErrNilReconciliationContext
	}

	if rc.Status != value_objects.ContextStatusDraft && rc.Status != value_objects.ContextStatusPaused {
		return fmt.Errorf("cannot activate context in %s status: %w", rc.Status, ErrInvalidStateTransition)
	}

	rc.Status = value_objects.ContextStatusActive
	rc.UpdatedAt = time.Now().UTC()

	return nil
}

// Archive marks the context as archived.
// Only allowed from ACTIVE or PAUSED status.
func (rc *ReconciliationContext) Archive(ctx context.Context) error {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_context.archive",
	)
	if err := asserter.NotNil(ctx, rc, "reconciliation context is required"); err != nil {
		return ErrNilReconciliationContext
	}

	if rc.Status != value_objects.ContextStatusActive && rc.Status != value_objects.ContextStatusPaused {
		return fmt.Errorf("cannot archive context in %s status: %w", rc.Status, ErrInvalidStateTransition)
	}

	rc.Status = value_objects.ContextStatusArchived
	rc.UpdatedAt = time.Now().UTC()

	return nil
}

// IsDraft reports whether the context is in draft status.
func (rc *ReconciliationContext) IsDraft() bool {
	if rc == nil {
		return false
	}

	return rc.Status == value_objects.ContextStatusDraft
}

// IsActive reports whether the context is active.
func (rc *ReconciliationContext) IsActive() bool {
	if rc == nil {
		return false
	}

	return rc.Status == value_objects.ContextStatusActive
}

// IsArchived reports whether the context is archived.
func (rc *ReconciliationContext) IsArchived() bool {
	if rc == nil {
		return false
	}

	return rc.Status == value_objects.ContextStatusArchived
}
