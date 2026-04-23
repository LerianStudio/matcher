// Package context provides PostgreSQL repository implementation for reconciliation contexts.
package context

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// ContextPostgreSQLModel represents the database model for reconciliation contexts.
type ContextPostgreSQLModel struct {
	ID                uuid.UUID
	TenantID          uuid.UUID
	Name              string
	Type              string
	Interval          string
	Status            string
	FeeToleranceAbs   string
	FeeTolerancePct   string
	FeeNormalization  *string
	AutoMatchOnUpload bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// NewContextPostgreSQLModel creates a new PostgreSQL model from a context entity.
func NewContextPostgreSQLModel(
	entity *entities.ReconciliationContext,
) (*ContextPostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	if entity.TenantID == uuid.Nil {
		return nil, ErrTenantIDRequired
	}

	id := entity.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	createdAt := entity.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	updatedAt := entity.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	return &ContextPostgreSQLModel{
		ID:                id,
		TenantID:          entity.TenantID,
		Name:              entity.Name,
		Type:              entity.Type.String(),
		Interval:          entity.Interval,
		Status:            entity.Status.String(),
		FeeToleranceAbs:   entity.FeeToleranceAbs.String(),
		FeeTolerancePct:   entity.FeeTolerancePct.String(),
		FeeNormalization:  entity.FeeNormalization,
		AutoMatchOnUpload: entity.AutoMatchOnUpload,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model to a domain entity.
func (model *ContextPostgreSQLModel) ToEntity() (*entities.ReconciliationContext, error) {
	if model == nil {
		return nil, ErrContextModelRequired
	}

	contextType, err := shared.ParseContextType(model.Type)
	if err != nil {
		return nil, fmt.Errorf("parsing Type '%s': %w", model.Type, err)
	}

	contextStatus, err := value_objects.ParseContextStatus(model.Status)
	if err != nil {
		return nil, fmt.Errorf("parsing Status '%s': %w", model.Status, err)
	}

	feeToleranceAbs, err := parseDecimalField(model.FeeToleranceAbs, "FeeToleranceAbs")
	if err != nil {
		return nil, err
	}

	feeTolerancePct, err := parseDecimalField(model.FeeTolerancePct, "FeeTolerancePct")
	if err != nil {
		return nil, err
	}

	if model.FeeNormalization != nil && *model.FeeNormalization != "" {
		mode := sharedfee.NormalizationMode(*model.FeeNormalization)
		if !mode.IsValid() {
			return nil, fmt.Errorf("parsing FeeNormalization %q: %w", *model.FeeNormalization, entities.ErrFeeNormalizationInvalid)
		}
	}

	return &entities.ReconciliationContext{
		ID:                model.ID,
		TenantID:          model.TenantID,
		Name:              model.Name,
		Type:              contextType,
		Interval:          model.Interval,
		Status:            contextStatus,
		FeeToleranceAbs:   feeToleranceAbs,
		FeeTolerancePct:   feeTolerancePct,
		FeeNormalization:  model.FeeNormalization,
		AutoMatchOnUpload: model.AutoMatchOnUpload,
		CreatedAt:         model.CreatedAt,
		UpdatedAt:         model.UpdatedAt,
	}, nil
}

// parseDecimalField parses a decimal value from a string field.
// Returns decimal.Zero for empty strings; wraps parse errors with the field name.
func parseDecimalField(value, fieldName string) (decimal.Decimal, error) {
	if value == "" {
		return decimal.Zero, nil
	}

	parsed, err := decimal.NewFromString(value)
	if err != nil {
		return decimal.Zero, fmt.Errorf("parsing %s '%s': %w", fieldName, value, err)
	}

	return parsed, nil
}
