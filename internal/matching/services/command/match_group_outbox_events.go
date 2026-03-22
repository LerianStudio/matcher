package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func (uc *UseCase) enqueueMatchConfirmedEvents(
	ctx context.Context,
	tx repositories.Tx,
	groups []*matchingEntities.MatchGroup,
) error {
	if uc.outboxRepoTx == nil {
		return ErrOutboxRepoNotConfigured
	}

	sqlTx, ok := tx.(*sql.Tx)
	if !ok || sqlTx == nil {
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
		if err := uc.enqueueGroupEvent(ctx, sqlTx, group, tenantUUID, tenantSlug); err != nil {
			return err
		}
	}

	return nil
}

func (uc *UseCase) enqueueGroupEvent(
	ctx context.Context,
	sqlTx *sql.Tx,
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

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal match confirmed event: %w", err)
	}

	outboxEvent, err := shared.NewOutboxEvent(ctx, event.EventType, event.ID(), body)
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	if _, err := uc.outboxRepoTx.CreateWithTx(ctx, sqlTx, outboxEvent); err != nil {
		return fmt.Errorf("create outbox entry: %w", err)
	}

	return nil
}
