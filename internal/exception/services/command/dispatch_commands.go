package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Dispatch errors.
var (
	ErrNilExternalConnector    = errors.New("external connector is required")
	ErrTargetSystemRequired    = errors.New("target system is required")
	ErrUnsupportedTargetSystem = errors.New("unsupported target system")
)

// DispatchCommand contains parameters for dispatching an exception to an external system.
type DispatchCommand struct {
	ExceptionID  uuid.UUID
	TargetSystem string
	Queue        string
}

// DispatchResult contains the result of dispatching an exception.
type DispatchResult struct {
	ExceptionID       uuid.UUID `json:"exceptionId"`
	Target            string    `json:"target"`
	ExternalReference string    `json:"externalReference,omitempty"`
	Acknowledged      bool      `json:"acknowledged"`
	DispatchedAt      time.Time `json:"dispatchedAt"`
}

// DispatchUseCase handles exception dispatch operations.
type DispatchUseCase struct {
	exceptionFinder ports.ExceptionFinder
	connector       ports.ExternalConnector
	auditPublisher  ports.AuditPublisher
	actorExtractor  ports.ActorExtractor
	infraProvider   sharedPorts.InfrastructureProvider
}

// NewDispatchUseCase creates a new dispatch use case with required dependencies.
func NewDispatchUseCase(
	exceptionFinder ports.ExceptionFinder,
	connector ports.ExternalConnector,
	audit ports.AuditPublisher,
	actor ports.ActorExtractor,
	infraProvider sharedPorts.InfrastructureProvider,
) (*DispatchUseCase, error) {
	if exceptionFinder == nil {
		return nil, ErrNilExceptionRepository
	}

	if connector == nil {
		return nil, ErrNilExternalConnector
	}

	if audit == nil {
		return nil, ErrNilAuditPublisher
	}

	if actor == nil {
		return nil, ErrNilActorExtractor
	}

	if infraProvider == nil {
		return nil, ErrNilInfraProvider
	}

	return &DispatchUseCase{
		exceptionFinder: exceptionFinder,
		connector:       connector,
		auditPublisher:  audit,
		actorExtractor:  actor,
		infraProvider:   infraProvider,
	}, nil
}

type dispatchParams struct {
	actor  string
	target services.RoutingTarget
	queue  string
}

func (uc *DispatchUseCase) validateDispatch(
	ctx context.Context,
	cmd DispatchCommand,
) (*dispatchParams, error) {
	if cmd.ExceptionID == uuid.Nil {
		return nil, ErrExceptionIDRequired
	}

	targetStr := strings.TrimSpace(strings.ToUpper(cmd.TargetSystem))
	if targetStr == "" {
		return nil, ErrTargetSystemRequired
	}

	target := services.RoutingTarget(targetStr)
	if !target.IsValid() {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedTargetSystem, targetStr)
	}

	actor := strings.TrimSpace(uc.actorExtractor.GetActor(ctx))
	if actor == "" {
		return nil, ErrActorRequired
	}

	return &dispatchParams{
		actor:  actor,
		target: target,
		queue:  strings.TrimSpace(cmd.Queue),
	}, nil
}

// Dispatch sends an exception to an external system.
func (uc *DispatchUseCase) Dispatch(
	ctx context.Context,
	cmd DispatchCommand,
) (*DispatchResult, error) {
	params, err := uc.validateDispatch(ctx, cmd)
	if err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.dispatch_exception")
	defer span.End()

	return uc.processDispatch(ctx, cmd, params, logger, span)
}

func (uc *DispatchUseCase) processDispatch(
	ctx context.Context,
	cmd DispatchCommand,
	params *dispatchParams,
	logger libLog.Logger,
	span trace.Span,
) (*DispatchResult, error) {
	exception, err := uc.exceptionFinder.FindByID(ctx, cmd.ExceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load exception", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to load exception: %v", err))

		return nil, fmt.Errorf("find exception: %w", err)
	}

	payload, err := buildDispatchPayload(cmd.ExceptionID, exception, params.target)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build payload", err)

		return nil, fmt.Errorf("build payload: %w", err)
	}

	decision := services.RoutingDecision{
		Target: params.target,
		Queue:  params.queue,
	}

	result, err := uc.connector.Dispatch(ctx, cmd.ExceptionID.String(), decision, payload)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "dispatch failed", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("dispatch to %s failed: %v", params.target, err))

		return nil, fmt.Errorf("dispatch to %s: %w", params.target, err)
	}

	// Wrap audit event in a transaction to ensure reliable audit trail.
	// The external dispatch has already been sent and cannot be undone,
	// but the audit record must be persisted atomically.
	tx, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin audit transaction", err)

		return nil, fmt.Errorf("begin audit transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback() // No-op if already committed
	}()

	targetStr := string(params.target)
	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, tx, ports.AuditEvent{
		ExceptionID: cmd.ExceptionID,
		Action:      "DISPATCH",
		Actor:       params.actor,
		Notes:       fmt.Sprintf("Dispatched to %s", params.target),
		OccurredAt:  time.Now().UTC(),
		Metadata: map[string]string{
			"target":             targetStr,
			"queue":              params.queue,
			"external_reference": result.ExternalReference,
		},
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed", err)

		return nil, fmt.Errorf("publish audit: %w", err)
	}

	if err := tx.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit audit transaction", err)

		return nil, fmt.Errorf("commit audit transaction: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("exception %s dispatched to %s", cmd.ExceptionID, params.target))

	return &DispatchResult{
		ExceptionID:       cmd.ExceptionID,
		Target:            string(result.Target),
		ExternalReference: result.ExternalReference,
		Acknowledged:      result.Acknowledged,
		DispatchedAt:      time.Now().UTC(),
	}, nil
}

type dispatchPayload struct {
	ExceptionID   string  `json:"exceptionId"`
	TransactionID string  `json:"transactionId"`
	Severity      string  `json:"severity"`
	Status        string  `json:"status"`
	Reason        *string `json:"reason,omitempty"`
	Target        string  `json:"target"`
	Summary       string  `json:"summary"`
	Description   string  `json:"description"`
	AgeDays       int     `json:"ageDays"`
	AssignedTo    *string `json:"assignedTo,omitempty"`
	DueAt         *string `json:"dueAt,omitempty"`
}

func buildDispatchPayload(
	exceptionID uuid.UUID,
	exception *entities.Exception,
	target services.RoutingTarget,
) ([]byte, error) {
	ageDays := calculateAgeDays(exception.CreatedAt)

	summary := fmt.Sprintf(
		"[%s] Exception %s requires attention (age: %d days)",
		exception.Severity.String(),
		exceptionID.String()[:8],
		ageDays,
	)

	description := fmt.Sprintf(
		"Exception ID: %s\nTransaction ID: %s\nSeverity: %s\nStatus: %s\nTarget: %s\nAge: %d days",
		exceptionID,
		exception.TransactionID,
		exception.Severity.String(),
		exception.Status.String(),
		target,
		ageDays,
	)

	if exception.Reason != nil && *exception.Reason != "" {
		description = fmt.Sprintf("%s\nReason: %s", description, *exception.Reason)
	}

	if exception.AssignedTo != nil && *exception.AssignedTo != "" {
		description = fmt.Sprintf("%s\nAssigned To: %s", description, *exception.AssignedTo)
	}

	if exception.DueAt != nil {
		description = fmt.Sprintf("%s\nDue At: %s", description, exception.DueAt.Format(time.RFC3339))
	}

	payload := dispatchPayload{
		ExceptionID:   exceptionID.String(),
		TransactionID: exception.TransactionID.String(),
		Severity:      exception.Severity.String(),
		Status:        exception.Status.String(),
		Reason:        exception.Reason,
		Target:        string(target),
		Summary:       summary,
		Description:   description,
		AgeDays:       ageDays,
		AssignedTo:    exception.AssignedTo,
		DueAt:         formatOptionalTime(exception.DueAt),
	}

	return json.Marshal(payload)
}

const hoursPerDay = 24

// calculateAgeDays returns the number of days between createdAt and referenceTime.
// If referenceTime is zero, it defaults to the current time.
func calculateAgeDays(createdAt time.Time, referenceTime ...time.Time) int {
	ref := time.Now().UTC()
	if len(referenceTime) > 0 && !referenceTime[0].IsZero() {
		ref = referenceTime[0]
	}

	return int(ref.Sub(createdAt).Hours() / hoursPerDay)
}

// BulkDispatch dispatches multiple exceptions to an external system.
func (uc *DispatchUseCase) BulkDispatch(
	ctx context.Context,
	input BulkDispatchInput,
) (*BulkActionResult, error) {
	dedupedIDs, err := validateBulkIDs(input.ExceptionIDs)
	if err != nil {
		return nil, err
	}

	targetSystem := strings.TrimSpace(input.TargetSystem)
	if targetSystem == "" {
		return nil, ErrBulkTargetSystemEmpty
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.bulk_dispatch_exceptions")
	defer span.End()

	result := &BulkActionResult{
		Succeeded: make([]uuid.UUID, 0, len(dedupedIDs)),
		Failed:    make([]BulkItemFailure, 0),
	}

	queue := strings.TrimSpace(input.Queue)

	for _, exceptionID := range dedupedIDs {
		_, dispatchErr := uc.Dispatch(ctx, DispatchCommand{
			ExceptionID:  exceptionID,
			TargetSystem: targetSystem,
			Queue:        queue,
		})
		if dispatchErr != nil {
			libOpentelemetry.HandleSpanError(span, "bulk dispatch item failed", dispatchErr)

			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("bulk dispatch failed for %s: %v", exceptionID, dispatchErr))

			result.Failed = append(result.Failed, BulkItemFailure{
				ExceptionID: exceptionID,
				Error:       dispatchErr.Error(),
			})

			continue
		}

		result.Succeeded = append(result.Succeeded, exceptionID)
	}

	return result, nil
}

func formatOptionalTime(t *time.Time) *string {
	if t == nil {
		return nil
	}

	formatted := t.Format(time.RFC3339)

	return &formatted
}
