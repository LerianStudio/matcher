// Package command contains command (write) use cases for the matching context.
package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// CreateAdjustmentInput contains the input parameters for creating an adjustment.
type CreateAdjustmentInput struct {
	TenantID      uuid.UUID
	ContextID     uuid.UUID
	MatchGroupID  *uuid.UUID
	TransactionID *uuid.UUID
	Type          string
	Direction     string
	Amount        decimal.Decimal
	Currency      string
	Description   string
	Reason        string
	CreatedBy     string
}

// Sentinel errors for adjustment operations.
var (
	ErrAdjustmentTenantIDRequired    = errors.New("tenant id is required")
	ErrAdjustmentContextIDRequired   = errors.New("context id is required")
	ErrAdjustmentTargetRequired      = errors.New("match group id or transaction id is required")
	ErrAdjustmentTypeRequired        = errors.New("type is required")
	ErrAdjustmentTypeInvalid         = errors.New("invalid adjustment type")
	ErrAdjustmentDirectionRequired   = errors.New("direction is required")
	ErrAdjustmentDirectionInvalid    = errors.New("invalid adjustment direction")
	ErrAdjustmentAmountNotPositive   = errors.New("amount must be positive")
	ErrAdjustmentCurrencyRequired    = errors.New("currency is required")
	ErrAdjustmentDescriptionRequired = errors.New("description is required")
	ErrAdjustmentReasonRequired      = errors.New("reason is required")
	ErrAdjustmentCreatedByRequired   = errors.New("created_by is required")
	ErrAdjustmentContextNotFound     = errors.New("context not found")
	ErrAdjustmentContextNotActive    = errors.New("context is not active")
	ErrAdjustmentMatchGroupNotFound  = errors.New("match group not found")
	ErrAdjustmentTransactionNotFound = errors.New("transaction not found")
)

// prepareAdjustment validates input, context, targets and creates the adjustment entity.
func (uc *UseCase) prepareAdjustment(
	ctx context.Context,
	in CreateAdjustmentInput,
) (*matchingEntities.Adjustment, error) {
	if err := uc.verifyAdjustmentContext(ctx, in); err != nil {
		return nil, err
	}

	if err := uc.verifyAdjustmentTargets(ctx, in); err != nil {
		return nil, err
	}

	adjustmentType := matchingEntities.AdjustmentType(in.Type)
	if !adjustmentType.IsValid() {
		return nil, ErrAdjustmentTypeInvalid
	}

	direction := matchingEntities.AdjustmentDirection(in.Direction)
	if !direction.IsValid() {
		return nil, ErrAdjustmentDirectionInvalid
	}

	return matchingEntities.NewAdjustment(
		ctx, in.ContextID, in.MatchGroupID, in.TransactionID,
		adjustmentType, direction, in.Amount, in.Currency, in.Description, in.Reason, in.CreatedBy,
	)
}

// persistAdjustmentWithAudit creates adjustment and audit log in a single transaction.
func (uc *UseCase) persistAdjustmentWithAudit(
	ctx context.Context,
	adjustment *matchingEntities.Adjustment,
	in CreateAdjustmentInput,
) (*matchingEntities.Adjustment, error) {
	auditLog, err := uc.buildAdjustmentAuditLogEntity(ctx, adjustment, in)
	if err != nil {
		return nil, err
	}

	created, err := uc.adjustmentRepo.CreateWithAuditLog(ctx, adjustment, auditLog)
	if err != nil {
		return nil, fmt.Errorf("persist adjustment transaction: %w", err)
	}

	return created, nil
}

// buildAdjustmentAuditLogEntity builds the audit log entity for an adjustment.
func (uc *UseCase) buildAdjustmentAuditLogEntity(
	ctx context.Context,
	adjustment *matchingEntities.Adjustment,
	in CreateAdjustmentInput,
) (*sharedDomain.AuditLog, error) {
	auditChanges, err := buildAdjustmentAuditChanges(adjustment.ID, in)
	if err != nil {
		return nil, fmt.Errorf("build audit changes: %w", err)
	}

	actorID := in.CreatedBy

	auditLog, err := sharedDomain.NewAuditLog(
		ctx,
		in.TenantID,
		"adjustment",
		adjustment.ID,
		"CREATE",
		&actorID,
		auditChanges,
	)
	if err != nil {
		return nil, fmt.Errorf("create audit log entity: %w", err)
	}

	return auditLog, nil
}

// CreateAdjustment creates a balancing journal entry to resolve a variance.
// This operation is atomic: both the adjustment creation and audit log are committed
// together or both are rolled back, ensuring SOX compliance.
func (uc *UseCase) CreateAdjustment(
	ctx context.Context,
	in CreateAdjustmentInput,
) (*matchingEntities.Adjustment, error) {
	if err := uc.validateCreateAdjustmentInput(in); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "command.matching.create_adjustment")

	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "matcher", in, nil)

	adjustment, err := uc.prepareAdjustment(ctx, in)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to prepare adjustment", err)
		return nil, err
	}

	created, err := uc.persistAdjustmentWithAudit(ctx, adjustment, in)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to persist adjustment with audit", err)
		return nil, err
	}

	logger.With(
		libLog.String("id", created.ID.String()),
		libLog.String("type", string(created.Type)),
		libLog.String("amount", created.Amount.String()),
		libLog.String("currency", created.Currency),
	).Log(ctx, libLog.LevelInfo, "adjustment created")

	return created, nil
}

// buildAdjustmentAuditChanges creates a JSON payload with adjustment details for audit logging.
func buildAdjustmentAuditChanges(adjustmentID uuid.UUID, in CreateAdjustmentInput) ([]byte, error) {
	payload := map[string]any{
		"entity_type": "adjustment",
		"entity_id":   adjustmentID.String(),
		"action":      "CREATE",
		"actor":       in.CreatedBy,
		"occurred_at": time.Now().UTC(),
		"context_id":  in.ContextID.String(),
		"type":        in.Type,
		"direction":   in.Direction,
		"amount":      in.Amount.String(),
		"currency":    in.Currency,
		"description": in.Description,
		"reason":      in.Reason,
	}

	if in.MatchGroupID != nil {
		payload["match_group_id"] = in.MatchGroupID.String()
	}

	if in.TransactionID != nil {
		payload["transaction_id"] = in.TransactionID.String()
	}

	return json.Marshal(payload)
}

func (uc *UseCase) verifyAdjustmentContext(ctx context.Context, in CreateAdjustmentInput) error {
	ctxInfo, err := uc.contextProvider.FindByID(ctx, in.TenantID, in.ContextID)
	if err != nil {
		return fmt.Errorf("find context: %w", err)
	}

	if ctxInfo == nil {
		return ErrAdjustmentContextNotFound
	}

	if !ctxInfo.Active {
		return ErrAdjustmentContextNotActive
	}

	return nil
}

func (uc *UseCase) verifyAdjustmentTargets(ctx context.Context, in CreateAdjustmentInput) error {
	if in.MatchGroupID != nil {
		group, err := uc.matchGroupRepo.FindByID(ctx, in.ContextID, *in.MatchGroupID)
		if err != nil {
			return fmt.Errorf("find match group: %w", err)
		}

		if group == nil {
			return ErrAdjustmentMatchGroupNotFound
		}
	}

	if in.TransactionID != nil {
		txns, err := uc.txRepo.FindByContextAndIDs(
			ctx,
			in.ContextID,
			[]uuid.UUID{*in.TransactionID},
		)
		if err != nil {
			return fmt.Errorf("find transactions: %w", err)
		}

		if len(txns) == 0 {
			return ErrAdjustmentTransactionNotFound
		}
	}

	return nil
}

func (uc *UseCase) validateCreateAdjustmentInput(in CreateAdjustmentInput) error {
	if in.TenantID == uuid.Nil {
		return ErrAdjustmentTenantIDRequired
	}

	if in.ContextID == uuid.Nil {
		return ErrAdjustmentContextIDRequired
	}

	if in.MatchGroupID == nil && in.TransactionID == nil {
		return ErrAdjustmentTargetRequired
	}

	if in.Type == "" {
		return ErrAdjustmentTypeRequired
	}

	if in.Direction == "" {
		return ErrAdjustmentDirectionRequired
	}

	if !in.Amount.IsPositive() {
		return ErrAdjustmentAmountNotPositive
	}

	if in.Currency == "" {
		return ErrAdjustmentCurrencyRequired
	}

	if in.Description == "" {
		return ErrAdjustmentDescriptionRequired
	}

	if in.Reason == "" {
		return ErrAdjustmentReasonRequired
	}

	if in.CreatedBy == "" {
		return ErrAdjustmentCreatedByRequired
	}

	return nil
}
