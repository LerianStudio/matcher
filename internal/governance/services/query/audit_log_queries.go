// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Sentinel errors for audit log query inputs.
var (
	// ErrAuditLogIDRequired is returned when GetAuditLog is called without an ID.
	ErrAuditLogIDRequired = errors.New("audit log id is required")

	// ErrEntityTypeRequired is returned when ListAuditLogsByEntity is called
	// without an entity type.
	ErrEntityTypeRequired = errors.New("entity type is required")

	// ErrEntityIDRequired is returned when ListAuditLogsByEntity is called
	// without an entity id.
	ErrEntityIDRequired = errors.New("entity id is required")
)

// GetAuditLogInput is the input for GetAuditLog.
type GetAuditLogInput struct {
	ID uuid.UUID
}

// ListAuditLogsInput is the input for ListAuditLogs.
type ListAuditLogsInput struct {
	Filter sharedDomain.AuditLogFilter
	Cursor *sharedhttp.TimestampCursor
	Limit  int
}

// ListAuditLogsByEntityInput is the input for ListAuditLogsByEntity.
type ListAuditLogsByEntityInput struct {
	EntityType string
	EntityID   uuid.UUID
	Cursor     *sharedhttp.TimestampCursor
	Limit      int
}

// GetAuditLog retrieves a single audit log by ID.
//
// A nil repository result is mapped to governanceErrors.ErrAuditLogNotFound so
// callers have a single sentinel to check, regardless of whether the underlying
// repository returns the sentinel directly or returns (nil, nil).
func (uc *UseCase) GetAuditLog(
	ctx context.Context,
	input GetAuditLogInput,
) (*sharedDomain.AuditLog, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.governance.get_audit_log")
	defer span.End()

	if input.ID == uuid.Nil {
		return nil, ErrAuditLogIDRequired
	}

	auditLog, err := uc.repo.GetByID(ctx, input.ID)
	if err != nil {
		if errors.Is(err, governanceErrors.ErrAuditLogNotFound) {
			return nil, err
		}

		libOpentelemetry.HandleSpanError(span, "failed to get audit log", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to get audit log")

		return nil, fmt.Errorf("get audit log: %w", err)
	}

	if auditLog == nil {
		return nil, governanceErrors.ErrAuditLogNotFound
	}

	return auditLog, nil
}

// ListAuditLogs retrieves audit logs using optional filters and cursor
// pagination. Returns the logs, the next cursor (empty if no more pages), and
// any error.
func (uc *UseCase) ListAuditLogs(
	ctx context.Context,
	input ListAuditLogsInput,
) ([]*sharedDomain.AuditLog, string, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.governance.list_audit_logs")
	defer span.End()

	logs, nextCursor, err := uc.repo.List(ctx, input.Filter, input.Cursor, input.Limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list audit logs", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list audit logs")

		return nil, "", fmt.Errorf("list audit logs: %w", err)
	}

	return logs, nextCursor, nil
}

// ListAuditLogsByEntity retrieves audit logs for a specific entity using
// cursor pagination. Returns the logs, the next cursor (empty if no more
// pages), and any error.
func (uc *UseCase) ListAuditLogsByEntity(
	ctx context.Context,
	input ListAuditLogsByEntityInput,
) ([]*sharedDomain.AuditLog, string, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.governance.list_audit_logs_by_entity")
	defer span.End()

	if input.EntityType == "" {
		return nil, "", ErrEntityTypeRequired
	}

	if input.EntityID == uuid.Nil {
		return nil, "", ErrEntityIDRequired
	}

	logs, nextCursor, err := uc.repo.ListByEntity(
		ctx,
		input.EntityType,
		input.EntityID,
		input.Cursor,
		input.Limit,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list audit logs by entity", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list audit logs by entity")

		return nil, "", fmt.Errorf("list audit logs by entity: %w", err)
	}

	return logs, nextCursor, nil
}
