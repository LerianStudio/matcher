package command

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
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

// CallbackUseCase handles callback processing with idempotency and rate limiting.
type CallbackUseCase struct {
	idempotencyRepo repositories.CallbackIdempotencyRepository
	exceptionRepo   repositories.ExceptionRepository
	auditPublisher  ports.AuditPublisher
	infraProvider   sharedPorts.InfrastructureProvider
	rateLimiter     ports.CallbackRateLimiter
}

// NewCallbackUseCase creates a new CallbackUseCase with the required dependencies.
func NewCallbackUseCase(
	idempotencyRepo repositories.CallbackIdempotencyRepository,
	exceptionRepo repositories.ExceptionRepository,
	auditPublisher ports.AuditPublisher,
	infraProvider sharedPorts.InfrastructureProvider,
	rateLimiter ports.CallbackRateLimiter,
) (*CallbackUseCase, error) {
	if isNilInterface(idempotencyRepo) {
		return nil, ErrNilIdempotencyRepository
	}

	if isNilInterface(exceptionRepo) {
		return nil, ErrNilExceptionRepository
	}

	if isNilInterface(auditPublisher) {
		return nil, ErrNilAuditPublisher
	}

	if isNilInterface(infraProvider) {
		return nil, ErrNilInfraProvider
	}

	if isNilInterface(rateLimiter) {
		return nil, ErrNilCallbackRateLimiter
	}

	return &CallbackUseCase{
		idempotencyRepo: idempotencyRepo,
		exceptionRepo:   exceptionRepo,
		auditPublisher:  auditPublisher,
		infraProvider:   infraProvider,
		rateLimiter:     rateLimiter,
	}, nil
}

type callbackParams struct {
	idempotencyKey  value_objects.IdempotencyKey
	dedupeKey       value_objects.IdempotencyKey
	externalSystem  string
	externalIssueID string
	status          value_objects.ExceptionStatus
	resolutionNotes *string
	assignee        string
	dueAt           *time.Time
	updatedAt       *time.Time
}

func idempotencyKeyHash(key value_objects.IdempotencyKey) string {
	hash := sha256.Sum256([]byte(key.String()))

	return hex.EncodeToString(hash[:])
}

func (uc *CallbackUseCase) validateCallback(cmd ProcessCallbackCommand) (*callbackParams, error) {
	if err := uc.validateDependencies(); err != nil {
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
func (uc *CallbackUseCase) ProcessCallback(ctx context.Context, cmd ProcessCallbackCommand) error {
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
func (uc *CallbackUseCase) checkRateLimit(
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
		libOpentelemetry.HandleSpanError(span, "callback rate limit exceeded", ErrCallbackRateLimitExceeded)
		return ErrCallbackRateLimitExceeded
	}

	return nil
}

func (uc *CallbackUseCase) handleExistingCallback(
	ctx context.Context,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	logger libLog.Logger,
	span trace.Span,
	cachedResult *value_objects.IdempotencyResult,
) error {
	if cachedResult == nil {
		libOpentelemetry.HandleSpanError(span, "idempotency state missing", ErrCallbackRetryable)
		return ErrCallbackRetryable
	}

	switch cachedResult.Status {
	case value_objects.IdempotencyStatusComplete:
		return uc.handleDuplicateCallback(ctx, cmd, params, logger, span)
	case value_objects.IdempotencyStatusPending:
		libOpentelemetry.HandleSpanError(span, "callback is still processing", ErrCallbackInProgress)
		return ErrCallbackInProgress
	case value_objects.IdempotencyStatusFailed, value_objects.IdempotencyStatusUnknown:
		libOpentelemetry.HandleSpanError(span, "callback requires retry", ErrCallbackRetryable)
		return ErrCallbackRetryable
	default:
		libOpentelemetry.HandleSpanError(span, "invalid idempotency status", ErrCallbackRetryable)
		return ErrCallbackRetryable
	}
}

func scopeCallbackIdempotencyKey(
	clientKey value_objects.IdempotencyKey,
	exceptionID uuid.UUID,
	externalSystem string,
) value_objects.IdempotencyKey {
	base := strings.ToUpper(strings.TrimSpace(externalSystem)) + ":" + exceptionID.String() + ":" + clientKey.String()
	hash := sha256.Sum256([]byte(base))

	return value_objects.IdempotencyKey("cb_" + hex.EncodeToString(hash[:]))
}

func (uc *CallbackUseCase) applyCallback(
	ctx context.Context,
	exception *entities.Exception,
	params *callbackParams,
) error {
	if exception == nil {
		return entities.ErrExceptionNil
	}

	// External tracking fields are set before the status check so callbacks always
	// update the external reference, even when the status has not changed.
	externalSystem := params.externalSystem
	externalIssueID := params.externalIssueID
	exception.ExternalSystem = &externalSystem
	exception.ExternalIssueID = &externalIssueID

	if exception.Status == params.status {
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
			defaultNotes := fmt.Sprintf("Resolved via %s callback", params.externalSystem)
			notes = &defaultNotes
		}

		if err := exception.Resolve(ctx, *notes); err != nil {
			return fmt.Errorf("resolve exception: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("%w: %s", ErrCallbackStatusUnsupported, params.status)
	}
}

func (uc *CallbackUseCase) markIdempotencyFailed(
	ctx context.Context,
	key value_objects.IdempotencyKey,
) {
	if err := uc.idempotencyRepo.MarkFailed(ctx, key); err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to mark idempotency failed: %v", err))
	}
}

func (uc *CallbackUseCase) validateDependencies() error {
	if uc == nil || isNilInterface(uc.idempotencyRepo) {
		return ErrNilIdempotencyRepository
	}

	if isNilInterface(uc.exceptionRepo) {
		return ErrNilExceptionRepository
	}

	if isNilInterface(uc.auditPublisher) {
		return ErrNilAuditPublisher
	}

	if isNilInterface(uc.infraProvider) {
		return ErrNilInfraProvider
	}

	if isNilInterface(uc.rateLimiter) {
		return ErrNilCallbackRateLimiter
	}

	return nil
}

func parseIdempotencyKey(key string) (value_objects.IdempotencyKey, error) {
	var idempotencyKey value_objects.IdempotencyKey

	idempotencyKey, err := value_objects.ParseIdempotencyKey(key)
	if err != nil {
		return idempotencyKey, fmt.Errorf("parse idempotency key: %w", err)
	}

	return idempotencyKey, nil
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

func (uc *CallbackUseCase) handleDuplicateCallback(
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

func (uc *CallbackUseCase) processCallback(
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
		libOpentelemetry.HandleSpanError(span, "failed to apply callback", err)
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

func (uc *CallbackUseCase) publishCallbackAudit(
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

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}

	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
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
