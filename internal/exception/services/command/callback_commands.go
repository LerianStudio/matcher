package command

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ProcessCallbackCommand contains parameters for processing a callback.
type ProcessCallbackCommand struct {
	IdempotencyKey  string
	ExceptionID     uuid.UUID
	CallbackType    string
	ExternalSystem  string
	ExternalIssueID string
	Status          string
	ResolutionNotes string
	Assignee        string
	DueAt           *time.Time
	UpdatedAt       *time.Time
	Payload         map[string]any
}

type callbackParams struct {
	idempotencyKey  shared.IdempotencyKey
	dedupeKey       shared.IdempotencyKey
	externalSystem  string
	externalIssueID string
	status          value_objects.ExceptionStatus
	resolutionNotes *string
	assignee        string
	dueAt           *time.Time
	updatedAt       *time.Time
}

func idempotencyKeyHash(key shared.IdempotencyKey) string {
	hash := sha256.Sum256([]byte(key.String()))

	return hex.EncodeToString(hash[:])
}

// validateCallbackDeps checks the dependencies required by ProcessCallback.
// Safe on a nil receiver — returns ErrNilIdempotencyRepository so
// nil-UseCase callers get a deterministic error rather than a panic.
func (uc *ExceptionUseCase) validateCallbackDeps() error {
	if uc == nil || sharedPorts.IsNilValue(uc.idempotencyRepo) {
		return ErrNilIdempotencyRepository
	}

	if sharedPorts.IsNilValue(uc.exceptionRepo) {
		return ErrNilExceptionRepository
	}

	if sharedPorts.IsNilValue(uc.auditPublisher) {
		return ErrNilAuditPublisher
	}

	if sharedPorts.IsNilValue(uc.infraProvider) {
		return ErrNilInfraProvider
	}

	if sharedPorts.IsNilValue(uc.rateLimiter) {
		return ErrNilCallbackRateLimiter
	}

	return nil
}

func (uc *ExceptionUseCase) validateCallback(cmd ProcessCallbackCommand) (*callbackParams, error) {
	if err := uc.validateCallbackDeps(); err != nil {
		return nil, err
	}

	if cmd.ExceptionID == uuid.Nil {
		return nil, ErrExceptionIDRequired
	}

	idempotencyKey, err := parseIdempotencyKey(cmd.IdempotencyKey)
	if err != nil {
		return nil, err
	}

	externalSystem, err := resolveExternalSystem(cmd)
	if err != nil {
		return nil, err
	}

	externalIssueID, err := resolveExternalIssueID(cmd)
	if err != nil {
		return nil, err
	}

	status, err := resolveCallbackStatus(cmd)
	if err != nil {
		return nil, err
	}

	resolutionNotes := resolveResolutionNotes(cmd)
	assignee := resolveAssignee(cmd)

	dueAt, err := resolveDueAt(cmd)
	if err != nil {
		return nil, err
	}

	updatedAt, err := resolveUpdatedAt(cmd)
	if err != nil {
		return nil, err
	}

	return &callbackParams{
		idempotencyKey:  idempotencyKey,
		externalSystem:  externalSystem,
		externalIssueID: externalIssueID,
		status:          status,
		resolutionNotes: resolutionNotes,
		assignee:        assignee,
		dueAt:           dueAt,
		updatedAt:       updatedAt,
	}, nil
}

// ProcessCallback processes a callback with rate limiting and idempotency guarantees.
// Rate limiting is checked first (before idempotency) to reject flooding attacks early.
// The rate limit key is tenant-scoped and external-system-scoped so each integration
// gets an isolated budget per tenant.
func (uc *ExceptionUseCase) ProcessCallback(ctx context.Context, cmd ProcessCallbackCommand) error {
	params, err := uc.validateCallback(cmd)
	if err != nil {
		return err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.process_callback")
	defer span.End()

	// Rate limit check: uses the external system as the rate limit key so each
	// integration (JIRA, WEBHOOK, SERVICENOW, etc.) gets its own budget.
	if err := uc.checkRateLimit(ctx, params.externalSystem, span); err != nil {
		return err
	}

	params.dedupeKey = scopeCallbackIdempotencyKey(params.idempotencyKey, cmd.ExceptionID, params.externalSystem)

	acquired, err := uc.idempotencyRepo.TryAcquire(ctx, params.dedupeKey)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to acquire idempotency lock", err)
		return fmt.Errorf("try acquire idempotency: %w", err)
	}

	if !acquired {
		cachedResult, cachedErr := uc.idempotencyRepo.GetCachedResult(ctx, params.dedupeKey)
		if cachedErr != nil {
			libOpentelemetry.HandleSpanError(span, "failed to inspect idempotency state", cachedErr)
			return fmt.Errorf("get cached idempotency result: %w", cachedErr)
		}

		return uc.handleExistingCallback(ctx, cmd, params, logger, span, cachedResult)
	}

	return uc.processCallback(ctx, cmd, params, logger, span)
}

// checkRateLimit verifies the callback is within the configured rate limit.
func (uc *ExceptionUseCase) checkRateLimit(
	ctx context.Context,
	rateLimitKey string,
	span trace.Span,
) error {
	allowed, err := uc.rateLimiter.Allow(ctx, rateLimitKey)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "rate limiter check failed", err)
		return fmt.Errorf("callback rate limiter: %w", err)
	}

	if !allowed {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "callback rate limit exceeded", ErrCallbackRateLimitExceeded)
		return ErrCallbackRateLimitExceeded
	}

	return nil
}

func (uc *ExceptionUseCase) handleExistingCallback(
	ctx context.Context,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	logger libLog.Logger,
	span trace.Span,
	cachedResult *shared.IdempotencyResult,
) error {
	if cachedResult == nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "idempotency state missing", ErrCallbackRetryable)
		return ErrCallbackRetryable
	}

	switch cachedResult.Status {
	case shared.IdempotencyStatusComplete:
		return uc.handleDuplicateCallback(ctx, cmd, params, logger, span)
	case shared.IdempotencyStatusPending:
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "callback is still processing", ErrCallbackInProgress)
		return ErrCallbackInProgress
	case shared.IdempotencyStatusFailed:
		reacquired, err := uc.idempotencyRepo.TryReacquireFromFailed(ctx, params.dedupeKey)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to reacquire failed idempotency key", err)
			return fmt.Errorf("reacquire failed callback: %w", err)
		}

		if reacquired {
			return uc.processCallback(ctx, cmd, params, logger, span)
		}

		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "callback is still processing", ErrCallbackInProgress)

		return ErrCallbackInProgress
	case shared.IdempotencyStatusUnknown:
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "callback requires retry", ErrCallbackRetryable)
		return ErrCallbackRetryable
	default:
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid idempotency status", ErrCallbackRetryable)
		return ErrCallbackRetryable
	}
}

func scopeCallbackIdempotencyKey(
	clientKey shared.IdempotencyKey,
	exceptionID uuid.UUID,
	externalSystem string,
) shared.IdempotencyKey {
	base := strings.ToUpper(strings.TrimSpace(externalSystem)) + ":" + exceptionID.String() + ":" + clientKey.String()
	hash := sha256.Sum256([]byte(base))

	return shared.IdempotencyKey("cb_" + hex.EncodeToString(hash[:]))
}

func (uc *ExceptionUseCase) applyCallback(
	ctx context.Context,
	exception *entities.Exception,
	params *callbackParams,
) error {
	if exception == nil {
		return entities.ErrExceptionNil
	}

	// External tracking fields are set before the status check so callbacks always
	// update the external reference, even when the status has not changed.
	exception.ExternalSystem = pointers.String(params.externalSystem)
	exception.ExternalIssueID = pointers.String(params.externalIssueID)

	if exception.Status == params.status {
		applyCallbackMetadataUpdate(exception, params)
		return nil
	}

	if err := value_objects.ValidateResolutionTransition(exception.Status, params.status); err != nil {
		return fmt.Errorf("validate resolution transition: %w", err)
	}

	// OPEN is not a valid callback target — exceptions are created in OPEN status
	// and the transition table only allows OPEN → ASSIGNED or OPEN → RESOLVED.
	// The transition table allows PENDING_RESOLUTION → OPEN (for revert/abort),
	// but callbacks should only drive forward to ASSIGNED or RESOLVED.
	if params.status == value_objects.ExceptionStatusOpen {
		return ErrCallbackOpenNotValidTarget
	}

	return applyCallbackStatusTransition(ctx, exception, params)
}

// applyCallbackMetadataUpdate applies assignment metadata changes (assignee, due date)
// when the callback status matches the current exception status.
func applyCallbackMetadataUpdate(exception *entities.Exception, params *callbackParams) {
	updated := false

	if trimmed := strings.TrimSpace(params.assignee); trimmed != "" {
		exception.AssignedTo = &trimmed
		updated = true
	}

	if params.dueAt != nil {
		exception.DueAt = params.dueAt
		updated = true
	}

	if updated {
		exception.UpdatedAt = time.Now().UTC()
	}
}

// applyCallbackStatusTransition dispatches the status transition to the appropriate
// exception method (Assign or Resolve) based on the target status.
func applyCallbackStatusTransition(
	ctx context.Context,
	exception *entities.Exception,
	params *callbackParams,
) error {
	switch params.status {
	case value_objects.ExceptionStatusAssigned:
		if strings.TrimSpace(params.assignee) == "" {
			return ErrCallbackAssigneeRequired
		}

		if err := exception.Assign(ctx, params.assignee, params.dueAt); err != nil {
			return fmt.Errorf("assign exception: %w", err)
		}

		return nil
	case value_objects.ExceptionStatusResolved:
		notes := params.resolutionNotes
		if notes == nil || strings.TrimSpace(*notes) == "" {
			notes = pointers.String(fmt.Sprintf("Resolved via %s callback", params.externalSystem))
		}

		if err := exception.Resolve(ctx, *notes); err != nil {
			return fmt.Errorf("resolve exception: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("%w: %s", ErrCallbackStatusUnsupported, params.status)
	}
}

func (uc *ExceptionUseCase) markIdempotencyFailed(
	ctx context.Context,
	key shared.IdempotencyKey,
) {
	if err := uc.idempotencyRepo.MarkFailed(ctx, key); err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to mark idempotency failed: %v", err))
	}
}

func parseIdempotencyKey(key string) (shared.IdempotencyKey, error) {
	parsedKey, err := shared.ParseIdempotencyKey(key)
	if err != nil {
		if errors.Is(err, shared.ErrEmptyIdempotencyKey) {
			return "", shared.ErrEmptyIdempotencyKey
		}

		if errors.Is(err, shared.ErrInvalidIdempotencyKey) {
			return "", shared.ErrInvalidIdempotencyKey
		}

		return "", fmt.Errorf("parse callback idempotency key: %w", err)
	}

	return parsedKey, nil
}

func resolveExternalSystem(cmd ProcessCallbackCommand) (string, error) {
	externalSystem := normalizeCallbackString(cmd.ExternalSystem)
	if externalSystem == "" {
		externalSystem = normalizeCallbackString(cmd.CallbackType)
	}

	if externalSystem == "" {
		externalSystem = payloadString(cmd.Payload, "external_system", "externalSystem")
	}

	if externalSystem == "" {
		return "", ErrCallbackExternalSystem
	}

	return strings.ToUpper(externalSystem), nil
}

func resolveExternalIssueID(cmd ProcessCallbackCommand) (string, error) {
	externalIssueID := normalizeCallbackString(cmd.ExternalIssueID)
	if externalIssueID == "" {
		externalIssueID = payloadString(cmd.Payload, "external_issue_id", "externalIssueID")
	}

	if externalIssueID == "" {
		return "", ErrCallbackExternalIssueID
	}

	return externalIssueID, nil
}

func resolveCallbackStatus(cmd ProcessCallbackCommand) (value_objects.ExceptionStatus, error) {
	statusValue := normalizeCallbackString(cmd.Status)
	if statusValue == "" {
		statusValue = payloadString(cmd.Payload, "status")
	}

	if statusValue == "" {
		return value_objects.ExceptionStatus(""), ErrCallbackStatusRequired
	}

	status, err := value_objects.ParseExceptionStatus(strings.ToUpper(statusValue))
	if err != nil {
		return value_objects.ExceptionStatus(""), fmt.Errorf("parse status: %w", err)
	}

	return status, nil
}

func resolveResolutionNotes(cmd ProcessCallbackCommand) *string {
	resolutionNotes := normalizeOptionalString(cmd.ResolutionNotes)
	if resolutionNotes == nil {
		resolutionNotes = normalizeOptionalString(
			payloadString(cmd.Payload, "resolution_notes", "resolutionNotes"),
		)
	}

	return resolutionNotes
}

func resolveAssignee(cmd ProcessCallbackCommand) string {
	assignee := normalizeCallbackString(cmd.Assignee)
	if assignee == "" {
		assignee = payloadString(cmd.Payload, "assignee")
	}

	return assignee
}

func resolveDueAt(cmd ProcessCallbackCommand) (*time.Time, error) {
	if cmd.DueAt != nil {
		return cmd.DueAt, nil
	}

	parsedDueAt, err := payloadTime(cmd.Payload, "due_at", "dueAt")
	if err != nil {
		return nil, err
	}

	return parsedDueAt, nil
}

func resolveUpdatedAt(cmd ProcessCallbackCommand) (*time.Time, error) {
	if cmd.UpdatedAt != nil {
		return cmd.UpdatedAt, nil
	}

	parsedUpdatedAt, err := payloadTime(cmd.Payload, "updated_at", "updatedAt")
	if err != nil {
		return nil, err
	}

	return parsedUpdatedAt, nil
}

func (uc *ExceptionUseCase) handleDuplicateCallback(
	ctx context.Context,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	logger libLog.Logger,
	span trace.Span,
) error {
	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("duplicate callback ignored (idempotency_key_hash=%s)", idempotencyKeyHash(params.idempotencyKey)))

	if err := uc.auditPublisher.PublishExceptionEvent(ctx, ports.AuditEvent{
		ExceptionID: cmd.ExceptionID,
		Action:      "CALLBACK_DUPLICATE_IGNORED",
		Actor:       "system",
		OccurredAt:  time.Now().UTC(),
		Metadata: map[string]string{
			"idempotency_key_hash": idempotencyKeyHash(params.idempotencyKey),
			"callback_type":        normalizeCallbackString(cmd.CallbackType),
			"external_system":      params.externalSystem,
			"external_issue_id":    params.externalIssueID,
			"status":               params.status.String(),
		},
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed for duplicate", err)
		return fmt.Errorf("publish duplicate audit: %w", err)
	}

	return nil
}

func (uc *ExceptionUseCase) processCallback(
	ctx context.Context,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	logger libLog.Logger,
	span trace.Span,
) error {
	exception, err := uc.exceptionRepo.FindByID(ctx, cmd.ExceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find exception", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to find exception: %v", err))

		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("find exception: %w", err)
	}

	if err := uc.applyCallback(ctx, exception, params); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "failed to apply callback", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("apply callback: %w", err)
	}

	// Atomic transaction: update exception state AND create audit log in same transaction.
	// This ensures SOX compliance - if either fails, both are rolled back.
	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = txLease.Rollback() // No-op if already committed
	}()

	updated, err := uc.exceptionRepo.UpdateWithTx(ctx, txLease.SQLTx(), exception)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update exception", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("update exception: %w", err)
	}

	if updated == nil {
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("update exception: %w", ErrUnexpectedNilResult)
	}

	if err := uc.publishCallbackAudit(ctx, txLease.SQLTx(), cmd, params, updated, span); err != nil {
		return err
	}

	if err := txLease.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("commit transaction: %w", err)
	}

	if err := uc.idempotencyRepo.MarkComplete(ctx, params.dedupeKey, nil, 0); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark idempotency complete", err)

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to mark idempotency complete after successful processing: %v", err))
	}

	return nil
}

func (uc *ExceptionUseCase) publishCallbackAudit(
	ctx context.Context,
	tx *sql.Tx,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	updated *entities.Exception,
	span trace.Span,
) error {
	auditNotes := ""
	if params.resolutionNotes != nil {
		auditNotes = *params.resolutionNotes
	}

	callbackType := normalizeCallbackString(cmd.CallbackType)
	if callbackType == "" {
		callbackType = params.externalSystem
	}

	metadata := map[string]string{
		"idempotency_key_hash": idempotencyKeyHash(params.idempotencyKey),
		"callback_type":        callbackType,
		"external_system":      params.externalSystem,
		"external_issue_id":    params.externalIssueID,
		"status":               params.status.String(),
	}

	if params.assignee != "" {
		metadata["assignee"] = params.assignee
	}

	if params.dueAt != nil {
		metadata["due_at"] = params.dueAt.UTC().Format(time.RFC3339)
	}

	if params.updatedAt != nil {
		metadata["updated_at"] = params.updatedAt.UTC().Format(time.RFC3339)
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, tx, ports.AuditEvent{
		ExceptionID: updated.ID,
		Action:      "CALLBACK_PROCESSED",
		Actor:       "system",
		Notes:       auditNotes,
		OccurredAt:  time.Now().UTC(),
		Metadata:    metadata,
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("publish audit: %w", err)
	}

	return nil
}

func normalizeCallbackString(value string) string {
	return strings.TrimSpace(value)
}

func normalizeOptionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}

func payloadString(payload map[string]any, keys ...string) string {
	if payload == nil {
		return ""
	}

	for _, key := range keys {
		if value, ok := payload[key]; ok {
			switch typed := value.(type) {
			case string:
				return strings.TrimSpace(typed)
			case fmt.Stringer:
				return strings.TrimSpace(typed.String())
			}
		}
	}

	return ""
}

func payloadTime(payload map[string]any, keys ...string) (*time.Time, error) {
	if payload == nil {
		return nil, nil
	}

	for _, key := range keys {
		if value, ok := payload[key]; ok {
			switch typed := value.(type) {
			case time.Time:
				copyValue := typed
				return &copyValue, nil
			case *time.Time:
				if typed != nil {
					copyValue := *typed
					return &copyValue, nil
				}
			case string:
				trimmed := strings.TrimSpace(typed)
				if trimmed == "" {
					return nil, nil
				}

				parsed, err := time.Parse(time.RFC3339, trimmed)
				if err != nil {
					return nil, fmt.Errorf("parse %s: %w", key, err)
				}

				return &parsed, nil
			}
		}
	}

	return nil, nil
}
