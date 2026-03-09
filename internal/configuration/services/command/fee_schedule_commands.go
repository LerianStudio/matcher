package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Sentinel errors for fee schedule operations.
var (
	// ErrNilFeeScheduleRepository is returned when the fee schedule repository is nil.
	ErrNilFeeScheduleRepository = errors.New("fee schedule repository is required")
	// ErrCreatedFeeScheduleNil is returned when the created fee schedule is unexpectedly nil.
	ErrCreatedFeeScheduleNil = errors.New("created fee schedule is nil")
	// ErrUnknownFeeStructureType is returned when the fee structure type is not recognized.
	ErrUnknownFeeStructureType = errors.New("unknown fee structure type")
)

// CreateFeeSchedule creates a new fee schedule.
func (uc *UseCase) CreateFeeSchedule(
	ctx context.Context,
	tenantID uuid.UUID,
	name string,
	currency string,
	applicationOrder string,
	roundingScale int,
	roundingMode string,
	items []fee.FeeScheduleItemInput,
) (*fee.FeeSchedule, error) {
	if uc == nil || uc.feeScheduleRepo == nil {
		return nil, ErrNilFeeScheduleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.create_fee_schedule")
	defer span.End()

	input := fee.NewFeeScheduleInput{
		TenantID:         tenantID,
		Name:             name,
		Currency:         currency,
		ApplicationOrder: fee.ApplicationOrder(applicationOrder),
		RoundingScale:    roundingScale,
		RoundingMode:     fee.RoundingMode(roundingMode),
		Items:            items,
	}

	entity, err := fee.NewFeeSchedule(ctx, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid fee schedule input", err)
		return nil, fmt.Errorf("create fee schedule: %w", err)
	}

	created, err := uc.feeScheduleRepo.Create(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create fee schedule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create fee schedule")

		return nil, fmt.Errorf("creating fee schedule: %w", err)
	}

	if created == nil {
		return nil, ErrCreatedFeeScheduleNil
	}

	uc.publishAudit(ctx, "fee_schedule", created.ID, "create", map[string]any{
		"name":     created.Name,
		"currency": created.Currency,
	})

	return created, nil
}

// UpdateFeeSchedule modifies an existing fee schedule.
func (uc *UseCase) UpdateFeeSchedule(
	ctx context.Context,
	scheduleID uuid.UUID,
	name *string,
	applicationOrder *string,
	roundingScale *int,
	roundingMode *string,
) (*fee.FeeSchedule, error) {
	if uc == nil || uc.feeScheduleRepo == nil {
		return nil, ErrNilFeeScheduleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.update_fee_schedule")
	defer span.End()

	entity, err := uc.feeScheduleRepo.GetByID(ctx, scheduleID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load fee schedule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee schedule")

		return nil, fmt.Errorf("finding fee schedule: %w", err)
	}

	if entity == nil {
		return nil, fee.ErrFeeScheduleNotFound
	}

	for _, update := range []func() error{
		func() error { return updateScheduleName(entity, name) },
		func() error { return updateScheduleApplicationOrder(entity, applicationOrder) },
		func() error { return updateScheduleRoundingScale(entity, roundingScale) },
		func() error { return updateScheduleRoundingMode(entity, roundingMode) },
	} {
		if err := update(); err != nil {
			return nil, err
		}
	}

	entity.UpdatedAt = time.Now().UTC()

	updated, err := uc.feeScheduleRepo.Update(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update fee schedule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update fee schedule")

		return nil, fmt.Errorf("updating fee schedule: %w", err)
	}

	uc.publishAudit(ctx, "fee_schedule", updated.ID, "update", map[string]any{
		"name": updated.Name,
	})

	return updated, nil
}

// DeleteFeeSchedule removes a fee schedule.
func (uc *UseCase) DeleteFeeSchedule(ctx context.Context, scheduleID uuid.UUID) error {
	if uc == nil || uc.feeScheduleRepo == nil {
		return ErrNilFeeScheduleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.delete_fee_schedule")
	defer span.End()

	if _, err := uc.feeScheduleRepo.GetByID(ctx, scheduleID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load fee schedule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee schedule")

		return fmt.Errorf("finding fee schedule: %w", err)
	}

	if err := uc.feeScheduleRepo.Delete(ctx, scheduleID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete fee schedule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to delete fee schedule")

		return fmt.Errorf("deleting fee schedule: %w", err)
	}

	uc.publishAudit(ctx, "fee_schedule", scheduleID, "delete", nil)

	return nil
}

// ParseFeeStructureFromRequest converts a request DTO item to a fee.FeeStructure.
func ParseFeeStructureFromRequest(structureType string, structure map[string]any) (fee.FeeStructure, error) {
	switch fee.FeeStructureType(structureType) {
	case fee.FeeStructureFlat:
		amountStr, ok := structure["amount"].(string)
		if !ok {
			return nil, fmt.Errorf("flat fee requires string 'amount' field: %w", fee.ErrNilFeeStructure)
		}

		amount, err := decimal.NewFromString(amountStr)
		if err != nil {
			return nil, fmt.Errorf("invalid flat fee amount: %w", err)
		}

		return fee.FlatFee{Amount: amount}, nil

	case fee.FeeStructurePercentage:
		rateStr, ok := structure["rate"].(string)
		if !ok {
			return nil, fmt.Errorf("percentage fee requires string 'rate' field: %w", fee.ErrNilFeeStructure)
		}

		rate, err := decimal.NewFromString(rateStr)
		if err != nil {
			return nil, fmt.Errorf("invalid percentage rate: %w", err)
		}

		return fee.PercentageFee{Rate: rate}, nil

	case fee.FeeStructureTiered:
		return parseTieredFromRequest(structure)

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownFeeStructureType, structureType)
	}
}

func parseTieredFromRequest(structure map[string]any) (fee.TieredFee, error) {
	tiersRaw, ok := structure["tiers"]
	if !ok {
		return fee.TieredFee{}, fmt.Errorf("tiered fee requires 'tiers' array: %w", fee.ErrInvalidTieredDefinition)
	}

	tiersSlice, ok := tiersRaw.([]any)
	if !ok {
		return fee.TieredFee{}, fmt.Errorf("tiers must be an array: %w", fee.ErrInvalidTieredDefinition)
	}

	tiers := make([]fee.Tier, 0, len(tiersSlice))

	for i, tierRaw := range tiersSlice {
		tierMap, ok := tierRaw.(map[string]any)
		if !ok {
			return fee.TieredFee{}, fmt.Errorf("tier[%d] must be an object: %w", i, fee.ErrInvalidTieredDefinition)
		}

		rateStr, ok := tierMap["rate"].(string)
		if !ok {
			return fee.TieredFee{}, fmt.Errorf("tier[%d] requires string 'rate': %w", i, fee.ErrInvalidTieredDefinition)
		}

		rate, err := decimal.NewFromString(rateStr)
		if err != nil {
			return fee.TieredFee{}, fmt.Errorf("tier[%d] invalid rate: %w", i, err)
		}

		tier := fee.Tier{Rate: rate}

		if upToRaw, exists := tierMap["upTo"]; exists && upToRaw != nil {
			upToStr, ok := upToRaw.(string)
			if !ok {
				return fee.TieredFee{}, fmt.Errorf("tier[%d] 'upTo' must be a string: %w", i, fee.ErrInvalidTieredDefinition)
			}

			upTo, err := decimal.NewFromString(upToStr)
			if err != nil {
				return fee.TieredFee{}, fmt.Errorf("tier[%d] invalid upTo: %w", i, err)
			}

			tier.UpTo = &upTo
		}

		tiers = append(tiers, tier)
	}

	return fee.TieredFee{Tiers: tiers}, nil
}

func updateScheduleName(entity *fee.FeeSchedule, name *string) error {
	if name == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*name)
	if trimmed == "" {
		return fee.ErrScheduleNameRequired
	}

	entity.Name = trimmed

	return nil
}

func updateScheduleApplicationOrder(entity *fee.FeeSchedule, applicationOrder *string) error {
	if applicationOrder == nil {
		return nil
	}

	order := fee.ApplicationOrder(*applicationOrder)
	if !order.IsValid() {
		return fee.ErrInvalidApplicationOrder
	}

	entity.ApplicationOrder = order

	return nil
}

func updateScheduleRoundingScale(entity *fee.FeeSchedule, roundingScale *int) error {
	if roundingScale == nil {
		return nil
	}

	if *roundingScale < 0 || *roundingScale > 10 {
		return fee.ErrInvalidRoundingScale
	}

	entity.RoundingScale = *roundingScale

	return nil
}

func updateScheduleRoundingMode(entity *fee.FeeSchedule, roundingMode *string) error {
	if roundingMode == nil {
		return nil
	}

	mode := fee.RoundingMode(*roundingMode)
	if !mode.IsValid() {
		return fee.ErrInvalidRoundingMode
	}

	entity.RoundingMode = mode

	return nil
}
