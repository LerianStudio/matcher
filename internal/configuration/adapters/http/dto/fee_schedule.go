package dto

import (
	"time"

	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// FeeScheduleResponse represents the API response for a fee schedule.
type FeeScheduleResponse struct {
	ID               string                    `json:"id"               example:"550e8400-e29b-41d4-a716-446655440000"`
	TenantID         string                    `json:"tenantId"         example:"550e8400-e29b-41d4-a716-446655440000"`
	Name             string                    `json:"name"             example:"Card Processing - Visa"`
	Currency         string                    `json:"currency"         example:"USD"`
	ApplicationOrder string                    `json:"applicationOrder" example:"PARALLEL"  enums:"PARALLEL,CASCADING"`
	RoundingScale    int                       `json:"roundingScale"    example:"2"`
	RoundingMode     string                    `json:"roundingMode"     example:"HALF_UP"   enums:"HALF_UP,BANKERS,FLOOR,CEIL,TRUNCATE"`
	Items            []FeeScheduleItemResponse `json:"items" validate:"omitempty,max=100" maxItems:"100"`
	CreatedAt        string                    `json:"createdAt"        example:"2025-01-15T10:30:00Z"`
	UpdatedAt        string                    `json:"updatedAt"        example:"2025-01-15T10:30:00Z"`
}

// FeeScheduleItemResponse represents a single fee item in the response.
type FeeScheduleItemResponse struct {
	ID            string         `json:"id"            example:"550e8400-e29b-41d4-a716-446655440000"`
	Name          string         `json:"name"          example:"interchange"`
	Priority      int            `json:"priority"      example:"1"`
	StructureType string         `json:"structureType" example:"PERCENTAGE" enums:"FLAT,PERCENTAGE,TIERED"`
	Structure     map[string]any `json:"structure"`
	CreatedAt     string         `json:"createdAt"     example:"2025-01-15T10:30:00Z"`
	UpdatedAt     string         `json:"updatedAt"     example:"2025-01-15T10:30:00Z"`
}

// CreateFeeScheduleRequest is the request body for creating a fee schedule.
type CreateFeeScheduleRequest struct {
	Name             string                         `json:"name"             validate:"required,max=100" example:"Card Processing - Visa" minLength:"1" maxLength:"100"`
	Currency         string                         `json:"currency"         validate:"required,max=3"   example:"USD"`
	ApplicationOrder string                         `json:"applicationOrder" validate:"required"         example:"PARALLEL"               enums:"PARALLEL,CASCADING"`
	RoundingScale    int                            `json:"roundingScale"                                example:"2"`
	RoundingMode     string                         `json:"roundingMode"                                 example:"HALF_UP"                enums:"HALF_UP,BANKERS,FLOOR,CEIL,TRUNCATE"`
	Items            []CreateFeeScheduleItemRequest `json:"items"            validate:"required,min=1,max=100" minItems:"1" maxItems:"100"`
}

// CreateFeeScheduleItemRequest is a single item in the create request.
type CreateFeeScheduleItemRequest struct {
	Name          string         `json:"name"          validate:"required,max=100" example:"interchange"`
	Priority      int            `json:"priority"                                  example:"1"`
	StructureType string         `json:"structureType" validate:"required"         example:"PERCENTAGE" enums:"FLAT,PERCENTAGE,TIERED"`
	Structure     map[string]any `json:"structure"     validate:"required"`
}

// UpdateFeeScheduleRequest is the request body for updating a fee schedule.
type UpdateFeeScheduleRequest struct {
	Name             *string `json:"name,omitempty"             validate:"omitempty,max=100" example:"Updated Schedule"`
	ApplicationOrder *string `json:"applicationOrder,omitempty"                              example:"CASCADING"        enums:"PARALLEL,CASCADING"`
	RoundingScale    *int    `json:"roundingScale,omitempty"                                 example:"4"`
	RoundingMode     *string `json:"roundingMode,omitempty"                                  example:"BANKERS"          enums:"HALF_UP,BANKERS,FLOOR,CEIL,TRUNCATE"`
}

// SimulateFeeRequest is the request body for simulating fee calculation.
type SimulateFeeRequest struct {
	GrossAmount string `json:"grossAmount" validate:"required" example:"100.00"`
	Currency    string `json:"currency"    validate:"required" example:"USD"`
}

// SimulateFeeResponse is the response for fee simulation.
type SimulateFeeResponse struct {
	GrossAmount string            `json:"grossAmount" example:"100.00"`
	NetAmount   string            `json:"netAmount"   example:"97.70"`
	TotalFee    string            `json:"totalFee"    example:"2.30"`
	Currency    string            `json:"currency"    example:"USD"`
	Items       []SimulateFeeItem `json:"items" validate:"omitempty,max=100" maxItems:"100"`
}

// SimulateFeeItem represents a single fee item in the simulation response.
type SimulateFeeItem struct {
	Name     string `json:"name"     example:"interchange"`
	Fee      string `json:"fee"      example:"1.50"`
	BaseUsed string `json:"baseUsed" example:"100.00"`
}

// FeeScheduleToResponse converts a fee.FeeSchedule domain entity to a response DTO.
func FeeScheduleToResponse(schedule *fee.FeeSchedule) FeeScheduleResponse {
	if schedule == nil {
		return FeeScheduleResponse{Items: []FeeScheduleItemResponse{}}
	}

	items := make([]FeeScheduleItemResponse, 0, len(schedule.Items))
	for _, item := range schedule.Items {
		items = append(items, feeScheduleItemToResponse(item))
	}

	return FeeScheduleResponse{
		ID:               schedule.ID.String(),
		TenantID:         schedule.TenantID.String(),
		Name:             schedule.Name,
		Currency:         schedule.Currency,
		ApplicationOrder: string(schedule.ApplicationOrder),
		RoundingScale:    schedule.RoundingScale,
		RoundingMode:     string(schedule.RoundingMode),
		Items:            items,
		CreatedAt:        schedule.CreatedAt.Format(time.RFC3339),
		UpdatedAt:        schedule.UpdatedAt.Format(time.RFC3339),
	}
}

// FeeSchedulesToResponse converts a slice of fee schedules to response DTOs.
func FeeSchedulesToResponse(schedules []*fee.FeeSchedule) []FeeScheduleResponse {
	result := make([]FeeScheduleResponse, 0, len(schedules))
	for _, s := range schedules {
		if s != nil {
			result = append(result, FeeScheduleToResponse(s))
		}
	}

	return result
}

func feeScheduleItemToResponse(item fee.FeeScheduleItem) FeeScheduleItemResponse {
	structureType := ""

	var structure map[string]any

	if item.Structure != nil {
		structureType = string(item.Structure.Type())
		structure = feeStructureToMap(item.Structure)
	}

	return FeeScheduleItemResponse{
		ID:            item.ID.String(),
		Name:          item.Name,
		Priority:      item.Priority,
		StructureType: structureType,
		Structure:     structure,
		CreatedAt:     item.CreatedAt.Format(time.RFC3339),
		UpdatedAt:     item.UpdatedAt.Format(time.RFC3339),
	}
}

func feeStructureToMap(structure fee.FeeStructure) map[string]any {
	if structure == nil {
		return map[string]any{}
	}

	switch feeStruct := structure.(type) {
	case fee.FlatFee:
		return map[string]any{"amount": feeStruct.Amount.String()}
	case fee.PercentageFee:
		return map[string]any{"rate": feeStruct.Rate.String()}
	case fee.TieredFee:
		tiers := make([]map[string]any, 0, len(feeStruct.Tiers))
		for _, tier := range feeStruct.Tiers {
			td := map[string]any{"rate": tier.Rate.String()}
			if tier.UpTo != nil {
				td["upTo"] = tier.UpTo.String()
			}

			tiers = append(tiers, td)
		}

		return map[string]any{"tiers": tiers}
	default:
		return map[string]any{}
	}
}

// FeeBreakdownToSimulateResponse converts a fee.FeeBreakdown to a SimulateFeeResponse.
func FeeBreakdownToSimulateResponse(gross decimal.Decimal, currency string, breakdown *fee.FeeBreakdown) SimulateFeeResponse {
	if breakdown == nil {
		return SimulateFeeResponse{
			GrossAmount: gross.String(),
			Currency:    currency,
			Items:       []SimulateFeeItem{},
		}
	}

	items := make([]SimulateFeeItem, 0, len(breakdown.ItemFees))
	for _, itemFee := range breakdown.ItemFees {
		items = append(items, SimulateFeeItem{
			Name:     itemFee.ItemName,
			Fee:      itemFee.Fee.Amount.String(),
			BaseUsed: itemFee.BaseUsed.Amount.String(),
		})
	}

	return SimulateFeeResponse{
		GrossAmount: gross.String(),
		NetAmount:   breakdown.NetAmount.Amount.String(),
		TotalFee:    breakdown.TotalFee.Amount.String(),
		Currency:    currency,
		Items:       items,
	}
}
