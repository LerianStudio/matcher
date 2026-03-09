package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// Bulk operation errors.
var (
	ErrBulkEmptyIDs          = errors.New("exception ids list is empty")
	ErrBulkTooManyIDs        = errors.New("too many exception ids (max 100)")
	ErrBulkAssigneeEmpty     = errors.New("assignee is required for bulk assign")
	ErrBulkResolutionEmpty   = errors.New("resolution is required for bulk resolve")
	ErrBulkTargetSystemEmpty = errors.New("target system is required for bulk dispatch")
)

// maxBulkIDs is the maximum number of exception IDs allowed per bulk operation.
const maxBulkIDs = 100

// BulkAssignInput contains parameters for a bulk assign operation.
type BulkAssignInput struct {
	ExceptionIDs []uuid.UUID
	Assignee     string
}

// BulkResolveInput contains parameters for a bulk resolve operation.
type BulkResolveInput struct {
	ExceptionIDs []uuid.UUID
	Resolution   string
	Reason       string
}

// BulkDispatchInput contains parameters for a bulk dispatch operation.
type BulkDispatchInput struct {
	ExceptionIDs []uuid.UUID
	TargetSystem string
	Queue        string
}

// BulkActionResult contains the results of a bulk operation.
type BulkActionResult struct {
	Succeeded []uuid.UUID
	Failed    []BulkItemFailure
}

// BulkItemFailure represents a single failure in a bulk operation.
type BulkItemFailure struct {
	ExceptionID uuid.UUID
	Error       string
}

// BulkAssign assigns multiple exceptions to the specified assignee.
func (uc *UseCase) BulkAssign(ctx context.Context, input BulkAssignInput) (*BulkActionResult, error) {
	dedupedIDs, err := validateBulkIDs(input.ExceptionIDs)
	if err != nil {
		return nil, err
	}

	assignee := strings.TrimSpace(input.Assignee)
	if assignee == "" {
		return nil, ErrBulkAssigneeEmpty
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.bulk_assign_exceptions")
	defer span.End()

	actor := strings.TrimSpace(uc.actorExtractor.GetActor(ctx))
	if actor == "" {
		return nil, ErrActorRequired
	}

	result := &BulkActionResult{
		Succeeded: make([]uuid.UUID, 0, len(dedupedIDs)),
		Failed:    make([]BulkItemFailure, 0),
	}

	for _, exceptionID := range dedupedIDs {
		err := uc.assignSingle(ctx, exceptionID, assignee, actor)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "bulk assign item failed", err)

			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("bulk assign failed for %s: %v", exceptionID, err))

			result.Failed = append(result.Failed, BulkItemFailure{
				ExceptionID: exceptionID,
				Error:       err.Error(),
			})

			continue
		}

		result.Succeeded = append(result.Succeeded, exceptionID)
	}

	return result, nil
}

func (uc *UseCase) assignSingle(
	ctx context.Context,
	exceptionID uuid.UUID,
	assignee string,
	actor string,
) error {
	exception, err := uc.exceptionRepo.FindByID(ctx, exceptionID)
	if err != nil {
		return fmt.Errorf("find exception: %w", err)
	}

	if err := exception.Assign(ctx, assignee, nil); err != nil {
		return fmt.Errorf("assign exception: %w", err)
	}

	tx, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := uc.exceptionRepo.UpdateWithTx(ctx, tx, exception); err != nil {
		return fmt.Errorf("update exception: %w", err)
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, tx, ports.AuditEvent{
		ExceptionID: exceptionID,
		Action:      "BULK_ASSIGN",
		Actor:       actor,
		Notes:       fmt.Sprintf("Assigned to %s via bulk action", assignee),
		OccurredAt:  time.Now().UTC(),
		Metadata: map[string]string{
			"assignee": assignee,
		},
	}); err != nil {
		return fmt.Errorf("publish audit: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// BulkResolve resolves multiple exceptions with the specified resolution.
func (uc *UseCase) BulkResolve(ctx context.Context, input BulkResolveInput) (*BulkActionResult, error) {
	dedupedIDs, err := validateBulkIDs(input.ExceptionIDs)
	if err != nil {
		return nil, err
	}

	resolution := strings.TrimSpace(input.Resolution)
	if resolution == "" {
		return nil, ErrBulkResolutionEmpty
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.bulk_resolve_exceptions")
	defer span.End()

	actor := strings.TrimSpace(uc.actorExtractor.GetActor(ctx))
	if actor == "" {
		return nil, ErrActorRequired
	}

	result := &BulkActionResult{
		Succeeded: make([]uuid.UUID, 0, len(dedupedIDs)),
		Failed:    make([]BulkItemFailure, 0),
	}

	reason := strings.TrimSpace(input.Reason)

	for _, exceptionID := range dedupedIDs {
		err := uc.resolveSingle(ctx, exceptionID, resolution, reason, actor)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "bulk resolve item failed", err)

			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("bulk resolve failed for %s: %v", exceptionID, err))

			result.Failed = append(result.Failed, BulkItemFailure{
				ExceptionID: exceptionID,
				Error:       err.Error(),
			})

			continue
		}

		result.Succeeded = append(result.Succeeded, exceptionID)
	}

	return result, nil
}

func (uc *UseCase) resolveSingle(
	ctx context.Context,
	exceptionID uuid.UUID,
	resolution string,
	reason string,
	actor string,
) error {
	exception, err := uc.exceptionRepo.FindByID(ctx, exceptionID)
	if err != nil {
		return fmt.Errorf("find exception: %w", err)
	}

	// Guard: skip exceptions that are already being resolved by another process.
	if exception.Status == value_objects.ExceptionStatusPendingResolution {
		return entities.ErrExceptionPendingResolution
	}

	if err := value_objects.ValidateResolutionTransition(
		exception.Status,
		value_objects.ExceptionStatusResolved,
	); err != nil {
		return fmt.Errorf("validate transition: %w", err)
	}

	var opts []entities.ResolveOption
	if reason != "" {
		opts = append(opts, entities.WithResolutionReason(reason))
	}

	if err := exception.Resolve(ctx, resolution, opts...); err != nil {
		return fmt.Errorf("resolve exception: %w", err)
	}

	tx, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := uc.exceptionRepo.UpdateWithTx(ctx, tx, exception); err != nil {
		return fmt.Errorf("update exception: %w", err)
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, tx, ports.AuditEvent{
		ExceptionID: exceptionID,
		Action:      "BULK_RESOLVE",
		Actor:       actor,
		Notes:       resolution,
		OccurredAt:  time.Now().UTC(),
		Metadata: map[string]string{
			"reason": reason,
		},
	}); err != nil {
		return fmt.Errorf("publish audit: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func validateBulkIDs(ids []uuid.UUID) ([]uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, ErrBulkEmptyIDs
	}

	if len(ids) > maxBulkIDs {
		return nil, ErrBulkTooManyIDs
	}

	// Deduplicate IDs to prevent processing the same exception twice.
	seen := make(map[uuid.UUID]struct{}, len(ids))
	deduped := make([]uuid.UUID, 0, len(ids))

	for _, id := range ids {
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			deduped = append(deduped, id)
		}
	}

	return deduped, nil
}
