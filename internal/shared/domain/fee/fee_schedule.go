package fee

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// maxScheduleNameLength is the maximum allowed length for a fee schedule name.
const maxScheduleNameLength = 100

// maxRoundingScale is the maximum allowed rounding scale (decimal places).
const maxRoundingScale = 10

// ApplicationOrder determines how multiple fee items compose.
type ApplicationOrder string

// ApplicationOrder values define how multiple fee items compose within a schedule.
// ApplicationOrderParallel computes each fee independently on the gross amount.
// ApplicationOrderCascading computes each fee on the remaining balance after prior fees.
const (
	ApplicationOrderParallel  ApplicationOrder = "PARALLEL"
	ApplicationOrderCascading ApplicationOrder = "CASCADING"
)

// IsValid returns true if the application order is a recognized value.
func (o ApplicationOrder) IsValid() bool {
	switch o {
	case ApplicationOrderParallel, ApplicationOrderCascading:
		return true
	default:
		return false
	}
}

// RoundingMode determines how intermediate calculations are rounded.
type RoundingMode string

// RoundingMode values define how intermediate fee calculations are rounded.
// RoundingModeHalfUp uses standard rounding where 0.5 rounds up.
const (
	RoundingModeHalfUp   RoundingMode = "HALF_UP"
	RoundingModeBankers  RoundingMode = "BANKERS"
	RoundingModeFloor    RoundingMode = "FLOOR"
	RoundingModeCeil     RoundingMode = "CEIL"
	RoundingModeTruncate RoundingMode = "TRUNCATE"
)

// IsValid returns true if the rounding mode is a recognized value.
func (m RoundingMode) IsValid() bool {
	switch m {
	case RoundingModeHalfUp, RoundingModeBankers, RoundingModeFloor, RoundingModeCeil, RoundingModeTruncate:
		return true
	default:
		return false
	}
}

// FeeScheduleItem represents a single fee component within a schedule.
type FeeScheduleItem struct {
	ID        uuid.UUID
	Name      string
	Priority  int
	Structure FeeStructure
	CreatedAt time.Time
	UpdatedAt time.Time
}

// FeeSchedule represents a collection of fee items applied to transactions.
type FeeSchedule struct {
	ID               uuid.UUID
	TenantID         uuid.UUID
	Name             string
	Currency         string
	ApplicationOrder ApplicationOrder
	RoundingScale    int
	RoundingMode     RoundingMode
	Items            []FeeScheduleItem
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// FeeScheduleItemInput contains parameters for creating a new FeeScheduleItem.
type FeeScheduleItemInput struct {
	Name      string
	Priority  int
	Structure FeeStructure
}

// NewFeeScheduleInput contains parameters for creating a new FeeSchedule.
type NewFeeScheduleInput struct {
	TenantID         uuid.UUID
	Name             string
	Currency         string
	ApplicationOrder ApplicationOrder
	RoundingScale    int
	RoundingMode     RoundingMode
	Items            []FeeScheduleItemInput
}

// NewFeeSchedule creates and validates a new FeeSchedule from the given input.
func NewFeeSchedule(ctx context.Context, input NewFeeScheduleInput) (*FeeSchedule, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.fee.schedule.new")

	if err := asserter.That(ctx, input.TenantID != uuid.Nil, ErrScheduleTenantIDRequired.Error()); err != nil {
		return nil, fmt.Errorf("fee schedule tenant id: %w", ErrScheduleTenantIDRequired)
	}

	if err := asserter.NotEmpty(ctx, input.Name, ErrScheduleNameRequired.Error()); err != nil {
		return nil, fmt.Errorf("fee schedule name: %w", ErrScheduleNameRequired)
	}

	if err := asserter.That(ctx, len(input.Name) <= maxScheduleNameLength,
		ErrScheduleNameTooLong.Error(), "length", len(input.Name)); err != nil {
		return nil, fmt.Errorf("fee schedule name: %w", ErrScheduleNameTooLong)
	}

	currency, err := NormalizeCurrency(input.Currency)
	if err != nil {
		return nil, err
	}

	if err := asserter.That(ctx, input.ApplicationOrder.IsValid(),
		ErrInvalidApplicationOrder.Error(), "order", string(input.ApplicationOrder)); err != nil {
		return nil, fmt.Errorf("fee schedule application order: %w", ErrInvalidApplicationOrder)
	}

	if err := asserter.That(ctx, input.RoundingScale >= 0 && input.RoundingScale <= maxRoundingScale,
		ErrInvalidRoundingScale.Error(), "scale", input.RoundingScale); err != nil {
		return nil, fmt.Errorf("fee schedule rounding scale: %w", ErrInvalidRoundingScale)
	}

	if err := asserter.That(ctx, input.RoundingMode.IsValid(),
		ErrInvalidRoundingMode.Error(), "mode", string(input.RoundingMode)); err != nil {
		return nil, fmt.Errorf("fee schedule rounding mode: %w", ErrInvalidRoundingMode)
	}

	if err := asserter.That(ctx, len(input.Items) > 0,
		ErrScheduleItemsRequired.Error()); err != nil {
		return nil, fmt.Errorf("fee schedule items: %w", ErrScheduleItemsRequired)
	}

	items, err := buildScheduleItems(ctx, asserter, input.Items)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	return &FeeSchedule{
		ID:               uuid.New(),
		TenantID:         input.TenantID,
		Name:             input.Name,
		Currency:         currency,
		ApplicationOrder: input.ApplicationOrder,
		RoundingScale:    input.RoundingScale,
		RoundingMode:     input.RoundingMode,
		Items:            items,
		CreatedAt:        now,
		UpdatedAt:        now,
	}, nil
}

// buildScheduleItems validates and creates FeeScheduleItem values from input.
func buildScheduleItems(ctx context.Context, asserter *assert.Asserter, inputs []FeeScheduleItemInput) ([]FeeScheduleItem, error) {
	seenPriorities := make(map[int]struct{}, len(inputs))
	items := make([]FeeScheduleItem, 0, len(inputs))
	now := time.Now().UTC()

	for i, itemInput := range inputs {
		if err := asserter.NotEmpty(ctx, itemInput.Name,
			ErrItemNameRequired.Error(), "index", i); err != nil {
			return nil, fmt.Errorf("fee schedule item[%d] name: %w", i, ErrItemNameRequired)
		}

		if err := asserter.NotNil(ctx, itemInput.Structure,
			ErrNilFeeStructure.Error(), "index", i); err != nil {
			return nil, fmt.Errorf("fee schedule item[%d] structure: %w", i, ErrNilFeeStructure)
		}

		if err := itemInput.Structure.Validate(ctx); err != nil {
			return nil, fmt.Errorf("fee schedule item[%d] structure validate: %w", i, err)
		}

		if _, exists := seenPriorities[itemInput.Priority]; exists {
			return nil, fmt.Errorf("fee schedule item[%d] priority %d: %w", i, itemInput.Priority, ErrDuplicateItemPriority)
		}

		seenPriorities[itemInput.Priority] = struct{}{}

		items = append(items, FeeScheduleItem{
			ID:        uuid.New(),
			Name:      itemInput.Name,
			Priority:  itemInput.Priority,
			Structure: itemInput.Structure,
			CreatedAt: now,
			UpdatedAt: now,
		})
	}

	return items, nil
}
