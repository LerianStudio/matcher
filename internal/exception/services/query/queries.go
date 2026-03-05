// Package query provides query use cases for the exception service.
package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Query use case errors.
var (
	ErrNilExceptionRepository = errors.New("exception repository is required")
	ErrNilDisputeRepository   = errors.New("dispute repository is required")
	ErrNilAuditRepository     = errors.New("audit repository is required")
	ErrNilUseCase             = errors.New("exception query use case is required")
	ErrTenantIDRequired       = errors.New("tenant id is required")
	ErrDisputeNotFound        = errors.New("dispute not found")
)

// TenantExtractor extracts tenant information from context.
type TenantExtractor interface {
	GetTenantID(ctx context.Context) uuid.UUID
}

// UseCase implements exception query operations.
type UseCase struct {
	exceptionRepo   repositories.ExceptionRepository
	disputeRepo     repositories.DisputeRepository
	auditRepo       sharedPorts.AuditLogRepository
	tenantExtractor TenantExtractor
}

// NewUseCase creates a new query use case with required dependencies.
func NewUseCase(
	exceptionRepo repositories.ExceptionRepository,
	disputeRepo repositories.DisputeRepository,
	auditRepo sharedPorts.AuditLogRepository,
	tenantExtractor TenantExtractor,
) (*UseCase, error) {
	if exceptionRepo == nil {
		return nil, ErrNilExceptionRepository
	}

	if disputeRepo == nil {
		return nil, ErrNilDisputeRepository
	}

	if auditRepo == nil {
		return nil, ErrNilAuditRepository
	}

	return &UseCase{
		exceptionRepo:   exceptionRepo,
		disputeRepo:     disputeRepo,
		auditRepo:       auditRepo,
		tenantExtractor: tenantExtractor,
	}, nil
}

// ListQuery contains parameters for listing exceptions.
type ListQuery struct {
	Filter repositories.ExceptionFilter
	Cursor repositories.CursorFilter
}

// GetException retrieves an exception by ID.
func (uc *UseCase) GetException(
	ctx context.Context,
	exceptionID uuid.UUID,
) (*entities.Exception, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "query.get_exception")

	defer span.End()

	result, err := uc.exceptionRepo.FindByID(ctx, exceptionID)
	if err != nil {
		if errors.Is(err, entities.ErrExceptionNotFound) {
			return nil, entities.ErrExceptionNotFound
		}

		return nil, fmt.Errorf("finding exception: %w", err)
	}

	if result == nil {
		return nil, entities.ErrExceptionNotFound
	}

	return result, nil
}

// ListExceptions retrieves exceptions with optional filters and pagination.
func (uc *UseCase) ListExceptions(
	ctx context.Context,
	query ListQuery,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	if uc == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilUseCase
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "query.list_exceptions")

	defer span.End()

	exceptions, pagination, err := uc.exceptionRepo.List(ctx, query.Filter, query.Cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing exceptions: %w", err)
	}

	return exceptions, pagination, nil
}

// HistoryEntry represents an audit log entry in exception history.
type HistoryEntry struct {
	ID        uuid.UUID
	Action    string
	ActorID   *string
	Changes   []byte
	CreatedAt string
}

// GetHistory retrieves the audit history for an exception.
func (uc *UseCase) GetHistory(
	ctx context.Context,
	exceptionID uuid.UUID,
	cursor string,
	limit int,
) ([]HistoryEntry, string, error) {
	if uc == nil {
		return nil, "", ErrNilUseCase
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "query.get_history")

	defer span.End()

	var tenantID uuid.UUID
	if uc.tenantExtractor != nil {
		tenantID = uc.tenantExtractor.GetTenantID(ctx)
	}

	if tenantID == uuid.Nil {
		return nil, "", ErrTenantIDRequired
	}

	var cursorPtr *libHTTP.TimestampCursor

	if cursor != "" {
		decoded, err := libHTTP.DecodeTimestampCursor(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("decoding cursor: %w", err)
		}

		cursorPtr = decoded
	}

	if limit <= 0 {
		limit = constants.DefaultPaginationLimit
	}

	logs, nextCursor, err := uc.auditRepo.ListByEntity(
		ctx,
		"exception",
		exceptionID,
		cursorPtr,
		limit,
	)
	if err != nil {
		return nil, "", fmt.Errorf("fetching audit history: %w", err)
	}

	entries := make([]HistoryEntry, len(logs))
	for i, log := range logs {
		entries[i] = HistoryEntry{
			ID:        log.ID,
			Action:    log.Action,
			ActorID:   log.ActorID,
			Changes:   log.Changes,
			CreatedAt: log.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
	}

	return entries, nextCursor, nil
}

// DisputeListQuery contains parameters for listing disputes.
type DisputeListQuery struct {
	Filter repositories.DisputeFilter
	Cursor repositories.CursorFilter
}

// ListDisputes retrieves disputes with optional filters and pagination.
func (uc *UseCase) ListDisputes(
	ctx context.Context,
	query DisputeListQuery,
) ([]*dispute.Dispute, libHTTP.CursorPagination, error) {
	if uc == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilUseCase
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "query.list_disputes")

	defer span.End()

	disputes, pagination, err := uc.disputeRepo.List(ctx, query.Filter, query.Cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing disputes: %w", err)
	}

	return disputes, pagination, nil
}

// GetDispute retrieves a dispute by ID.
func (uc *UseCase) GetDispute(
	ctx context.Context,
	disputeID uuid.UUID,
) (*dispute.Dispute, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "query.get_dispute")

	defer span.End()

	result, err := uc.disputeRepo.FindByID(ctx, disputeID)
	if err != nil {
		if errors.Is(err, dispute.ErrNotFound) {
			return nil, ErrDisputeNotFound
		}

		return nil, fmt.Errorf("finding dispute: %w", err)
	}

	if result == nil {
		return nil, ErrDisputeNotFound
	}

	return result, nil
}
