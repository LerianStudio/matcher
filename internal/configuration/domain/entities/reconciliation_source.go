// Package entities holds configuration domain entities.
package entities

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// maxSourceNameLength defines the maximum allowed length for source names.
const maxSourceNameLength = 50

// Reconciliation source errors.
var (
	// ErrNilReconciliationSource is returned when the reconciliation source is nil.
	ErrNilReconciliationSource = errors.New("reconciliation source is nil")
	// ErrSourceNameRequired is returned when the source name is not provided.
	ErrSourceNameRequired = errors.New("source name is required")
	// ErrSourceNameTooLong is returned when the source name exceeds 50 characters.
	ErrSourceNameTooLong = errors.New("source name exceeds 50 characters")
	// ErrSourceTypeInvalid is returned when the source type is invalid.
	ErrSourceTypeInvalid = errors.New("invalid source type")
	// ErrSourceContextRequired is returned when the context_id is not provided.
	ErrSourceContextRequired = errors.New("context_id is required")
)

// ReconciliationSource represents an external source to reconcile against.
type ReconciliationSource struct {
	ID        uuid.UUID
	ContextID uuid.UUID
	Name      string
	Type      value_objects.SourceType
	Config    map[string]any
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateReconciliationSourceInput defines the input required to create a source.
type CreateReconciliationSourceInput struct {
	Name   string                   `json:"name"   validate:"required,max=50" example:"Primary Bank Account" minLength:"1" maxLength:"50"`
	Type   value_objects.SourceType `json:"type"   validate:"required"        example:"BANK"                                              enums:"LEDGER,BANK,GATEWAY,CUSTOM,FETCHER"`
	Config map[string]any           `json:"config"`
}

// CreateContextSourceInput defines the input required to create a source inline with a context.
type CreateContextSourceInput struct {
	Name    string                   `json:"name"              validate:"required,max=50" example:"Primary Bank Account" minLength:"1" maxLength:"50"`
	Type    value_objects.SourceType `json:"type"              validate:"required"        example:"BANK"                                              enums:"LEDGER,BANK,GATEWAY,CUSTOM,FETCHER"`
	Config  map[string]any           `json:"config"`
	Mapping map[string]any           `json:"mapping,omitempty"`
}

// UpdateReconciliationSourceInput defines the fields that can be updated on a source.
type UpdateReconciliationSourceInput struct {
	Name   *string                   `json:"name,omitempty" validate:"omitempty,max=50" example:"Secondary Bank Account" maxLength:"50"`
	Type   *value_objects.SourceType `json:"type,omitempty"                             example:"LEDGER"                                enums:"LEDGER,BANK,GATEWAY,CUSTOM,FETCHER"`
	Config map[string]any            `json:"config,omitempty"`
}

// NewReconciliationSource validates input and returns a new source entity.
func NewReconciliationSource(
	ctx context.Context,
	contextID uuid.UUID,
	input CreateReconciliationSourceInput,
) (*ReconciliationSource, error) {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_source.new",
	)
	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, ErrSourceContextRequired
	}

	name := strings.TrimSpace(input.Name)

	if err := asserter.That(ctx, name != "", "source name is required"); err != nil {
		return nil, ErrSourceNameRequired
	}

	if err := asserter.That(ctx, len(name) <= maxSourceNameLength, "source name too long", "name", name); err != nil {
		return nil, ErrSourceNameTooLong
	}

	if err := asserter.That(ctx, input.Type.Valid(), "invalid source type", "type", input.Type.String()); err != nil {
		return nil, ErrSourceTypeInvalid
	}

	config := input.Config
	if config == nil {
		config = make(map[string]any)
	}

	now := time.Now().UTC()

	return &ReconciliationSource{
		ID:        uuid.New(),
		ContextID: contextID,
		Name:      name,
		Type:      input.Type,
		Config:    config,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// Update applies changes to a reconciliation source.
func (rs *ReconciliationSource) Update(
	ctx context.Context,
	input UpdateReconciliationSourceInput,
) error {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_source.update",
	)
	if err := asserter.NotNil(ctx, rs, "reconciliation source is required"); err != nil {
		return ErrNilReconciliationSource
	}

	if input.Name != nil {
		trimmed := strings.TrimSpace(*input.Name)

		if err := asserter.That(ctx, trimmed != "", "source name is required"); err != nil {
			return ErrSourceNameRequired
		}

		if err := asserter.That(ctx, len(trimmed) <= maxSourceNameLength, "source name too long", "name", trimmed); err != nil {
			return ErrSourceNameTooLong
		}

		rs.Name = trimmed
	}

	if input.Type != nil {
		if !input.Type.Valid() {
			return ErrSourceTypeInvalid
		}

		rs.Type = *input.Type
	}

	if input.Config != nil {
		rs.Config = input.Config
	}

	rs.UpdatedAt = time.Now().UTC()

	return nil
}

// ConfigJSON marshals the source configuration to JSON.
func (rs *ReconciliationSource) ConfigJSON() ([]byte, error) {
	if rs == nil {
		return json.Marshal(nil)
	}

	return json.Marshal(rs.Config)
}
