// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
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
