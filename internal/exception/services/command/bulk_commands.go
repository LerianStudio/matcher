package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// preloadExceptions fetches all exceptions for the given ids in a single
// query and returns them keyed by id. Ids not present in the returned map
// are treated as ErrExceptionNotFound by the per-item caller. The map
// replaces the N round-trip pre-load that bulk paths previously made,
// one FindByID per item, without changing the per-item transaction
// boundary: the map is read before each per-item tx begins, so any item
// hitting ErrConcurrentModification or a transient failure still rolls
// back only that item's UPDATE and leaves the rest of the batch
// independently committable.
func (uc *ExceptionUseCase) preloadExceptions(
	ctx context.Context,
	ids []uuid.UUID,
) (map[uuid.UUID]*entities.Exception, error) {
	found, err := uc.exceptionRepo.FindByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("preload exceptions: %w", err)
	}

	byID := make(map[uuid.UUID]*entities.Exception, len(found))
	for _, ex := range found {
		if ex == nil {
			continue
		}

		byID[ex.ID] = ex
	}

	return byID, nil
}

// resolvePreloaded returns the preloaded entity for exceptionID or
// entities.ErrExceptionNotFound when the id was requested but absent
// from the store. Keeps the sentinel flow identical to the previous
// FindByID-per-item call path so `isBusinessError` still classifies the
// not-found case as a business error (kept out of span.ERROR status).
func resolvePreloaded(
	preloaded map[uuid.UUID]*entities.Exception,
	exceptionID uuid.UUID,
) (*entities.Exception, error) {
	ex, ok := preloaded[exceptionID]
	if !ok {
		return nil, fmt.Errorf("find exception: %w", entities.ErrExceptionNotFound)
	}

	return ex, nil
}

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
//
// Execution shape:
//   - 1 FindByIDs preload (replaces N FindByID round-trips).
//   - N small transactions (one BEGIN + UPDATE + outbox insert + COMMIT
//     per exception).
//
// The per-item transaction boundary is preserved on purpose: the repository's
// optimistic-locking UPDATE (WHERE version=$13) returns
// ErrConcurrentModification on 0 rows affected, and existing partial-success
// reporting relies on each item's commit being independent of its peers.
// A single outer transaction would couple all items' fates and regress both
// guarantees on any single infrastructure blip mid-batch.
func (uc *ExceptionUseCase) BulkAssign(ctx context.Context, input BulkAssignInput) (*BulkActionResult, error) {
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

	preloaded, err := uc.preloadExceptions(ctx, dedupedIDs)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bulk assign preload failed", err)

		return nil, err
	}

	result := &BulkActionResult{
		Succeeded: make([]uuid.UUID, 0, len(dedupedIDs)),
		Failed:    make([]BulkItemFailure, 0),
	}

	for _, exceptionID := range dedupedIDs {
		err := uc.assignSingle(ctx, preloaded, exceptionID, assignee, actor)
		if err != nil {
			if isBusinessError(err) {
				libOpentelemetry.HandleSpanBusinessErrorEvent(span, "bulk assign item failed", err)
			} else {
				libOpentelemetry.HandleSpanError(span, "bulk assign item failed", err)
			}

			libLog.SafeError(logger, ctx, fmt.Sprintf("bulk assign failed for %s", exceptionID), err, runtime.IsProductionMode())

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

func (uc *ExceptionUseCase) assignSingle(
	ctx context.Context,
	preloaded map[uuid.UUID]*entities.Exception,
	exceptionID uuid.UUID,
	assignee string,
	actor string,
) error {
	exception, err := resolvePreloaded(preloaded, exceptionID)
	if err != nil {
		return err
	}

	if err := exception.Assign(ctx, assignee, nil); err != nil {
		return fmt.Errorf("assign exception: %w", err)
	}

	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = txLease.Rollback()
	}()

	if _, err := uc.exceptionRepo.UpdateWithTx(ctx, txLease.SQLTx(), exception); err != nil {
		return fmt.Errorf("update exception: %w", err)
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, txLease.SQLTx(), ports.AuditEvent{
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

	if err := txLease.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// BulkResolve resolves multiple exceptions with the specified resolution.
//
// Execution shape matches BulkAssign: one FindByIDs preload feeds N small
// per-item transactions. See BulkAssign for the rationale on why the
// per-item transaction boundary is deliberate (optimistic-locking
// detection and partial-success reporting).
func (uc *ExceptionUseCase) BulkResolve(ctx context.Context, input BulkResolveInput) (*BulkActionResult, error) {
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

	preloaded, err := uc.preloadExceptions(ctx, dedupedIDs)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bulk resolve preload failed", err)

		return nil, err
	}

	result := &BulkActionResult{
		Succeeded: make([]uuid.UUID, 0, len(dedupedIDs)),
		Failed:    make([]BulkItemFailure, 0),
	}

	reason := strings.TrimSpace(input.Reason)

	for _, exceptionID := range dedupedIDs {
		err := uc.resolveSingle(ctx, preloaded, exceptionID, resolution, reason, actor)
		if err != nil {
			if isBusinessError(err) {
				libOpentelemetry.HandleSpanBusinessErrorEvent(span, "bulk resolve item failed", err)
			} else {
				libOpentelemetry.HandleSpanError(span, "bulk resolve item failed", err)
			}

			libLog.SafeError(logger, ctx, fmt.Sprintf("bulk resolve failed for %s", exceptionID), err, runtime.IsProductionMode())

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

func (uc *ExceptionUseCase) resolveSingle(
	ctx context.Context,
	preloaded map[uuid.UUID]*entities.Exception,
	exceptionID uuid.UUID,
	resolution string,
	reason string,
	actor string,
) error {
	exception, err := resolvePreloaded(preloaded, exceptionID)
	if err != nil {
		return err
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

	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = txLease.Rollback()
	}()

	if _, err := uc.exceptionRepo.UpdateWithTx(ctx, txLease.SQLTx(), exception); err != nil {
		return fmt.Errorf("update exception: %w", err)
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, txLease.SQLTx(), ports.AuditEvent{
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

	if err := txLease.Commit(); err != nil {
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
