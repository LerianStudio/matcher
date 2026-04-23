// Package fee_schedule provides PostgreSQL persistence adapters for fee schedule entities.
package fee_schedule

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// PostgreSQLModel represents the fee_schedules table mapping.
type PostgreSQLModel struct {
	ID               uuid.UUID
	TenantID         uuid.UUID
	Name             string
	Currency         string
	ApplicationOrder string
	RoundingScale    int
	RoundingMode     string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// ItemPostgreSQLModel represents the fee_schedule_items table mapping.
type ItemPostgreSQLModel struct {
	ID            uuid.UUID
	FeeScheduleID uuid.UUID
	Name          string
	Priority      int
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

// ToEntity converts PostgreSQL models to a fee.FeeSchedule entity.
func ToEntity(model *PostgreSQLModel, items []ItemPostgreSQLModel) (*fee.FeeSchedule, error) {
	if model == nil {
		return nil, ErrFeeScheduleModelNeeded
	}

	feeItems := make([]fee.FeeScheduleItem, 0, len(items))

	for idx, item := range items {
		feeItem, err := itemToEntity(item)
		if err != nil {
			return nil, fmt.Errorf("parse item[%d]: %w", idx, err)
		}

		feeItems = append(feeItems, *feeItem)
	}

	return &fee.FeeSchedule{
		ID:               model.ID,
		TenantID:         model.TenantID,
		Name:             model.Name,
		Currency:         model.Currency,
		ApplicationOrder: fee.ApplicationOrder(model.ApplicationOrder),
		RoundingScale:    model.RoundingScale,
		RoundingMode:     fee.RoundingMode(model.RoundingMode),
		Items:            feeItems,
		CreatedAt:        model.CreatedAt,
		UpdatedAt:        model.UpdatedAt,
	}, nil
}

func itemToEntity(item ItemPostgreSQLModel) (*fee.FeeScheduleItem, error) {
	structure, err := parseStructure(item.StructureType, item.StructureData)
	if err != nil {
		return nil, fmt.Errorf("parse structure: %w", err)
	}

	return &fee.FeeScheduleItem{
		ID:        item.ID,
		Name:      item.Name,
		Priority:  item.Priority,
		Structure: structure,
		CreatedAt: item.CreatedAt,
		UpdatedAt: item.UpdatedAt,
	}, nil
}

func parseStructure(structureType string, data []byte) (fee.FeeStructure, error) {
	switch fee.FeeStructureType(structureType) {
	case fee.FeeStructureFlat:
		return parseFlatFee(data)
	case fee.FeeStructurePercentage:
		return parsePercentageFee(data)
	case fee.FeeStructureTiered:
		return parseTieredFee(data)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownStructureType, structureType)
	}
}

func parseFlatFee(data []byte) (fee.FlatFee, error) {
	var feeData FlatFeeData
	if err := json.Unmarshal(data, &feeData); err != nil {
		return fee.FlatFee{}, fmt.Errorf("unmarshal flat fee: %w", err)
	}

	amount, err := decimal.NewFromString(feeData.Amount)
	if err != nil {
		return fee.FlatFee{}, fmt.Errorf("parse flat fee amount: %w", err)
	}

	return fee.FlatFee{Amount: amount}, nil
}

func parsePercentageFee(data []byte) (fee.PercentageFee, error) {
	var feeData PercentageFeeData
	if err := json.Unmarshal(data, &feeData); err != nil {
		return fee.PercentageFee{}, fmt.Errorf("unmarshal percentage fee: %w", err)
	}

	rate, err := decimal.NewFromString(feeData.Rate)
	if err != nil {
		return fee.PercentageFee{}, fmt.Errorf("parse percentage rate: %w", err)
	}

	return fee.PercentageFee{Rate: rate}, nil
}

func parseTieredFee(data []byte) (fee.TieredFee, error) {
	var feeData TieredFeeData
	if err := json.Unmarshal(data, &feeData); err != nil {
		return fee.TieredFee{}, fmt.Errorf("unmarshal tiered fee: %w", err)
	}

	tiers := make([]fee.Tier, 0, len(feeData.Tiers))

	for idx, tierData := range feeData.Tiers {
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

// FromEntity converts a fee.FeeSchedule entity to PostgreSQL models.
func FromEntity(schedule *fee.FeeSchedule) (*PostgreSQLModel, []ItemPostgreSQLModel, error) {
	if schedule == nil {
		return nil, nil, nil
	}

	model := &PostgreSQLModel{
		ID:               schedule.ID,
		TenantID:         schedule.TenantID,
		Name:             schedule.Name,
		Currency:         schedule.Currency,
		ApplicationOrder: string(schedule.ApplicationOrder),
		RoundingScale:    schedule.RoundingScale,
		RoundingMode:     string(schedule.RoundingMode),
		CreatedAt:        schedule.CreatedAt,
		UpdatedAt:        schedule.UpdatedAt,
	}

	items := make([]ItemPostgreSQLModel, 0, len(schedule.Items))

	for idx, item := range schedule.Items {
		structureType, structureData, err := marshalStructure(item.Structure)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal item[%d] structure: %w", idx, err)
		}

		items = append(items, ItemPostgreSQLModel{
			ID:            item.ID,
			FeeScheduleID: schedule.ID,
			Name:          item.Name,
			Priority:      item.Priority,
			StructureType: string(structureType),
			StructureData: structureData,
			CreatedAt:     item.CreatedAt,
			UpdatedAt:     item.UpdatedAt,
		})
	}

	return model, items, nil
}

func marshalStructure(structure fee.FeeStructure) (fee.FeeStructureType, []byte, error) {
	if structure == nil {
		return "", []byte("{}"), nil
	}

	structureType := structure.Type()

	var (
		data []byte
		err  error
	)

	switch feeVal := structure.(type) {
	case fee.FlatFee:
		data, err = json.Marshal(FlatFeeData{Amount: feeVal.Amount.String()})
		if err != nil {
			return "", nil, fmt.Errorf("marshal flat fee structure: %w", err)
		}
	case fee.PercentageFee:
		data, err = json.Marshal(PercentageFeeData{Rate: feeVal.Rate.String()})
		if err != nil {
			return "", nil, fmt.Errorf("marshal percentage fee structure: %w", err)
		}
	case fee.TieredFee:
		tiers := make([]TierData, 0, len(feeVal.Tiers))
		for _, tier := range feeVal.Tiers {
			td := TierData{Rate: tier.Rate.String()}
			if tier.UpTo != nil {
				upToStr := tier.UpTo.String()
				td.UpTo = &upToStr
			}

			tiers = append(tiers, td)
		}

		data, err = json.Marshal(TieredFeeData{Tiers: tiers})
		if err != nil {
			return "", nil, fmt.Errorf("marshal tiered fee structure: %w", err)
		}
	default:
		return "", nil, fmt.Errorf("%w: %T", ErrUnknownStructureType, structure)
	}

	return structureType, data, nil
}
