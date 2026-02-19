// Package rate provides PostgreSQL persistence adapters for fee rate entities.
package rate

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// PostgreSQLModel represents the rates table mapping.
type PostgreSQLModel struct {
	ID            string
	Currency      string
	StructureType string
	StructureData []byte
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// FlatFeeData represents the JSON structure for flat fee persistence.
type FlatFeeData struct {
	Amount string `json:"amount"`
}

// PercentageFeeData represents the JSON structure for percentage fee persistence.
type PercentageFeeData struct {
	Rate string `json:"rate"`
}

// TierData represents a single tier in JSON persistence.
type TierData struct {
	UpTo *string `json:"up_to,omitempty"`
	Rate string  `json:"rate"`
}

// TieredFeeData represents the JSON structure for tiered fee persistence.
type TieredFeeData struct {
	Tiers []TierData `json:"tiers"`
}

// ToEntity converts a PostgreSQLModel to a fee.Rate entity.
func (model *PostgreSQLModel) ToEntity() (*fee.Rate, error) {
	if model == nil {
		return nil, ErrRateModelNeeded
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	structure, err := model.parseStructure()
	if err != nil {
		return nil, fmt.Errorf("parse structure: %w", err)
	}

	return &fee.Rate{
		ID:        id,
		Currency:  model.Currency,
		Structure: structure,
		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}, nil
}

func (model *PostgreSQLModel) parseStructure() (fee.FeeStructure, error) {
	switch fee.FeeStructureType(model.StructureType) {
	case fee.FeeStructureFlat:
		return model.parseFlatFee()
	case fee.FeeStructurePercentage:
		return model.parsePercentageFee()
	case fee.FeeStructureTiered:
		return model.parseTieredFee()
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownStructureType, model.StructureType)
	}
}

func (model *PostgreSQLModel) parseFlatFee() (fee.FlatFee, error) {
	var data FlatFeeData
	if err := json.Unmarshal(model.StructureData, &data); err != nil {
		return fee.FlatFee{}, fmt.Errorf("unmarshal flat fee: %w", err)
	}

	amount, err := decimal.NewFromString(data.Amount)
	if err != nil {
		return fee.FlatFee{}, fmt.Errorf("parse flat fee amount: %w", err)
	}

	return fee.FlatFee{Amount: amount}, nil
}

func (model *PostgreSQLModel) parsePercentageFee() (fee.PercentageFee, error) {
	var data PercentageFeeData
	if err := json.Unmarshal(model.StructureData, &data); err != nil {
		return fee.PercentageFee{}, fmt.Errorf("unmarshal percentage fee: %w", err)
	}

	rate, err := decimal.NewFromString(data.Rate)
	if err != nil {
		return fee.PercentageFee{}, fmt.Errorf("parse percentage rate: %w", err)
	}

	return fee.PercentageFee{Rate: rate}, nil
}

func (model *PostgreSQLModel) parseTieredFee() (fee.TieredFee, error) {
	var data TieredFeeData
	if err := json.Unmarshal(model.StructureData, &data); err != nil {
		return fee.TieredFee{}, fmt.Errorf("unmarshal tiered fee: %w", err)
	}

	tiers := make([]fee.Tier, 0, len(data.Tiers))

	for idx, tierData := range data.Tiers {
		rate, err := decimal.NewFromString(tierData.Rate)
		if err != nil {
			return fee.TieredFee{}, fmt.Errorf("parse tier %d rate: %w", idx, err)
		}

		tier := fee.Tier{Rate: rate}

		if tierData.UpTo != nil {
			upTo, err := decimal.NewFromString(*tierData.UpTo)
			if err != nil {
				return fee.TieredFee{}, fmt.Errorf("parse tier %d up_to: %w", idx, err)
			}

			tier.UpTo = &upTo
		}

		tiers = append(tiers, tier)
	}

	return fee.TieredFee{Tiers: tiers}, nil
}
