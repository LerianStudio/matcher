package command

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/adapters/outboxtelemetry"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// matchEventEnvelopeHeadroomBytes reserves space in the outbox payload
// cap for the non-ID fields of MatchConfirmedEvent / MatchUnmatchedEvent
// (tenant id, tenant slug, context id, run id, match id, rule id,
// timestamps, event type, optional reason up to maxReasonLength). Four
// KiB is comfortably above the worst-case envelope at ~1 KiB and avoids
// wire-format cliffs when tenant slugs grow or reason strings are
// enriched in the future.
const matchEventEnvelopeHeadroomBytes = 4 * 1024

func (uc *UseCase) enqueueMatchConfirmedEvents(
	ctx context.Context,
	tx repositories.Tx,
	groups []*matchingEntities.MatchGroup,
) error {
	if uc.outboxRepoTx == nil {
		return ErrOutboxRepoNotConfigured
	}

	if tx == nil {
		return ErrOutboxRequiresSQLTx
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
	for _, group := range groups {
		if err := uc.enqueueGroupEvent(ctx, tx, group, tenantUUID, tenantSlug); err != nil {
			return err
		}
	}

	return nil
}

func (uc *UseCase) enqueueGroupEvent(
	ctx context.Context,
	tx repositories.Tx,
	group *matchingEntities.MatchGroup,
	tenantUUID uuid.UUID,
	tenantSlug string,
) error {
	if group == nil || group.Status != matchingVO.MatchGroupStatusConfirmed {
		return nil
	}

	event, err := matchingEntities.NewMatchConfirmedEvent(
		ctx,
		tenantUUID,
		tenantSlug,
		group,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("build match confirmed event: %w", err)
	}

	// Guard against pathological match groups whose transaction list
	// alone would overflow the broker's per-event payload cap. Observed
	// groups are small (<= 10^2 ids); the budget below caps that at the
	// low six figures. The domain helper is pure; the WARN line + metric
	// are emitted here so the domain stays free of logging deps.
	maxIDBytes := shared.DefaultOutboxMaxPayloadBytes - matchEventEnvelopeHeadroomBytes
	truncatedIDs, originalCount := shared.TruncateIDListIfTooLarge(event.TransactionIDs, maxIDBytes)

	if len(truncatedIDs) != originalCount {
		event.TransactionIDs = truncatedIDs
		event.TruncatedIDCount = originalCount

		outboxtelemetry.RecordIDListTruncated(
			ctx,
			shared.EventTypeMatchConfirmed,
			event.MatchID,
			originalCount,
			len(truncatedIDs),
			maxIDBytes,
		)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal match confirmed event: %w", err)
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
