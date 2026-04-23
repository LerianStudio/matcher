package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/adapters/outboxtelemetry"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// UnmatchInput contains the parameters for breaking a match group.
type UnmatchInput struct {
	TenantID     uuid.UUID
	ContextID    uuid.UUID
	MatchGroupID uuid.UUID
	Reason       string
}

// Unmatch sentinel errors.
var (
	ErrUnmatchContextIDRequired    = errors.New("context id is required")
	ErrUnmatchMatchGroupIDRequired = errors.New("match group id is required")
	ErrUnmatchReasonRequired       = errors.New("reason is required")
	ErrMatchGroupNotFound          = errors.New("match group not found")
)

// Unmatch breaks an existing match group, reverting transaction statuses to unmatched.
func (uc *UseCase) Unmatch(ctx context.Context, input UnmatchInput) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.unmatch")
	defer span.End()

	if err := validateUnmatchInput(input); err != nil {
		return err
	}

	if err := validateTenantFromContextStrict(ctx, input.TenantID); err != nil {
		return err
	}

	group, err := uc.loadMatchGroup(ctx, logger, input.ContextID, input.MatchGroupID)
	if err != nil {
		return err
	}

	wasConfirmed := group.Status == matchingVO.MatchGroupStatusConfirmed

	if err := uc.txRepo.WithTx(ctx, func(tx repositories.Tx) error {
		if txErr := uc.rejectOrRevokeGroup(ctx, logger, tx, group, input.Reason, wasConfirmed); txErr != nil {
			return txErr
		}

		if txErr := uc.revertTransactionStatuses(ctx, logger, tx, input.ContextID, input.MatchGroupID); txErr != nil {
			return txErr
		}

		if wasConfirmed {
			if txErr := uc.enqueueUnmatchEvent(ctx, tx, group, input.Reason); txErr != nil {
				return txErr
			}
		}

		return nil
	}); err != nil {
		return err
	}

	logger.With(
		libLog.String("group_id", input.MatchGroupID.String()),
		libLog.String("reason", input.Reason),
	).Log(ctx, libLog.LevelInfo, "successfully unmatched group")

	return nil
}

func validateUnmatchInput(input UnmatchInput) error {
	if input.TenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	if input.ContextID == uuid.Nil {
		return ErrUnmatchContextIDRequired
	}

	if input.MatchGroupID == uuid.Nil {
		return ErrUnmatchMatchGroupIDRequired
	}

	if input.Reason == "" {
		return ErrUnmatchReasonRequired
	}

	return nil
}

func validateTenantFromContext(ctx context.Context, tenantID uuid.UUID) error {
	if tenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	rawTenantID, ok := ctx.Value(auth.TenantIDKey).(string)
	if !ok {
		return nil
	}

	ctxTenantID := strings.TrimSpace(rawTenantID)
	if ctxTenantID == "" {
		return nil
	}

	ctxTenantUUID, err := uuid.Parse(ctxTenantID)
	if err != nil {
		return ErrTenantIDRequired
	}

	if tenantID != ctxTenantUUID {
		return ErrTenantIDMismatch
	}

	return nil
}

func validateTenantFromContextStrict(ctx context.Context, tenantID uuid.UUID) error {
	if tenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	ctxTenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	if ctxTenantID == "" {
		ctxTenantID = strings.TrimSpace(auth.DefaultTenantID)
	}

	if ctxTenantID == "" {
		return ErrTenantIDRequired
	}

	ctxTenantUUID, err := uuid.Parse(ctxTenantID)
	if err != nil {
		return ErrTenantIDRequired
	}

	if tenantID != ctxTenantUUID {
		return ErrTenantIDMismatch
	}

	return nil
}

func (uc *UseCase) loadMatchGroup(
	ctx context.Context,
	logger libLog.Logger,
	contextID, matchGroupID uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	group, err := uc.matchGroupRepo.FindByID(ctx, contextID, matchGroupID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMatchGroupNotFound
		}

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find match group")

		return nil, fmt.Errorf("find match group: %w", err)
	}

	if group == nil {
		return nil, ErrMatchGroupNotFound
	}

	return group, nil
}

func (uc *UseCase) rejectOrRevokeGroup(
	ctx context.Context,
	logger libLog.Logger,
	tx repositories.Tx,
	group *matchingEntities.MatchGroup,
	reason string,
	wasConfirmed bool,
) error {
	var err error

	action := "reject"
	if wasConfirmed {
		action = "revoke"
		err = group.Revoke(ctx, reason)
	} else {
		err = group.Reject(ctx, reason)
	}

	if err != nil {
		logger.With(libLog.String("action", action), libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update match group status")
		return fmt.Errorf("%s match group: %w", action, err)
	}

	if _, err := uc.matchGroupRepo.UpdateWithTx(ctx, tx, group); err != nil {
		logger.With(libLog.String("action", action), libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update match group after status change")
		return fmt.Errorf("update match group: %w", err)
	}

	return nil
}

func (uc *UseCase) enqueueUnmatchEvent(
	ctx context.Context,
	tx repositories.Tx,
	group *matchingEntities.MatchGroup,
	reason string,
) error {
	if uc.outboxRepoTx == nil {
		return ErrOutboxRepoNotConfigured
	}

	if tx == nil {
		return ErrOutboxRequiresSQLTx
	}

	if len(group.Items) == 0 {
		items, err := uc.matchItemRepo.ListByMatchGroupID(ctx, group.ID)
		if err != nil {
			return fmt.Errorf("load match items for unmatch event: %w", err)
		}

		group.Items = items
	}

	tenantIDStr := auth.GetTenantID(ctx)
	if tenantIDStr == "" {
		tenantIDStr = auth.DefaultTenantID
	}

	tenantUUID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return fmt.Errorf("parse tenant id: %w", err)
	}

	tenantSlug := auth.GetTenantSlug(ctx)

	event, err := matchingEntities.NewMatchUnmatchedEvent(
		ctx,
		tenantUUID,
		tenantSlug,
		group,
		reason,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("build match unmatched event: %w", err)
	}

	// Guard against pathological groups whose transaction list alone
	// would overflow the broker cap. See the MatchConfirmedEvent path
	// for the rationale behind matchEventEnvelopeHeadroomBytes. The
	// domain helper is pure; the WARN line + metric are emitted here so
	// the domain stays free of logging deps.
	maxIDBytes := shared.DefaultOutboxMaxPayloadBytes - matchEventEnvelopeHeadroomBytes
	truncatedIDs, originalCount := shared.TruncateIDListIfTooLarge(event.TransactionIDs, maxIDBytes)

	if len(truncatedIDs) != originalCount {
		event.TransactionIDs = truncatedIDs
		event.TruncatedIDCount = originalCount

		outboxtelemetry.RecordIDListTruncated(
			ctx,
			shared.EventTypeMatchUnmatched,
			event.MatchID,
			originalCount,
			len(truncatedIDs),
			maxIDBytes,
		)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal match unmatched event: %w", err)
	}

	outboxEvent, err := shared.NewOutboxEvent(ctx, event.EventType, event.ID(), body)
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	if _, err := uc.outboxRepoTx.CreateWithTx(ctx, tx, outboxEvent); err != nil {
		return fmt.Errorf("create outbox entry: %w", err)
	}

	return nil
}

func (uc *UseCase) revertTransactionStatuses(
	ctx context.Context,
	logger libLog.Logger,
	tx repositories.Tx,
	contextID, matchGroupID uuid.UUID,
) error {
	items, err := uc.matchItemRepo.ListByMatchGroupID(ctx, matchGroupID)
	if err != nil {
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list match items")
		return fmt.Errorf("list match items: %w", err)
	}

	if len(items) == 0 {
		return nil
	}

	transactionIDs := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		transactionIDs = append(transactionIDs, item.TransactionID)
	}

	if err := uc.txRepo.MarkUnmatchedWithTx(ctx, tx, contextID, transactionIDs); err != nil {
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to mark transactions unmatched")
		return fmt.Errorf("mark transactions unmatched: %w", err)
	}

	return nil
}
