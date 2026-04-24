// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for exception operations.
//
// TODO(LOW) REVIEW_EXCEPTION L3: Extract repetitive HTTP error handling logic into
// shared helper functions to reduce duplication across handlers.
//
// TODO(LOW) REVIEW_EXCEPTION L7: Add edge case tests for HTTP handlers including
// malformed JSON, boundary values, and concurrent requests.
package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// Handlers provides HTTP handlers for exception operations.
//
// productionMode governs SafeError behavior (suppresses internal error
// details in client responses when true). Stored as a per-handler bool
// rather than a package-level atomic.Bool — the previous shared-global
// state coupled every test in the package to whichever test last
// constructed a handler, regardless of the production flag each test
// wanted to exercise.
type Handlers struct {
	// commandUC hosts every write operation on the exception bounded
	// context (resolution, disputes, dispatch, comments, callbacks).
	// The previously split use-case fields (exceptionUC, disputeUC,
	// dispatchUC, commentUC, callbackUC) have been merged into a single
	// grouped use case; handlers call the relevant method directly on
	// commandUC.
	commandUC         *command.ExceptionUseCase
	queryUC           *query.UseCase
	commentRepo       repositories.CommentRepository
	exceptionVerifier libHTTP.ResourceOwnershipVerifier
	disputeVerifier   libHTTP.ResourceOwnershipVerifier
	productionMode    bool
}

// NewHandlers creates a new Handlers instance with the given use cases and verifiers.
func NewHandlers(
	commandUC *command.ExceptionUseCase,
	queryUC *query.UseCase,
	commentRepo repositories.CommentRepository,
	exceptionProvider exceptionProvider,
	disputeProvider disputeProvider,
	production bool,
) (*Handlers, error) {
	if commandUC == nil {
		return nil, ErrNilExceptionUseCase
	}

	if queryUC == nil {
		return nil, ErrNilQueryUseCase
	}

	if commentRepo == nil {
		return nil, ErrNilCommentRepository
	}

	if exceptionProvider == nil {
		return nil, ErrNilExceptionProvider
	}

	if disputeProvider == nil {
		return nil, ErrNilDisputeProvider
	}

	return &Handlers{
		commandUC:         commandUC,
		queryUC:           queryUC,
		commentRepo:       commentRepo,
		exceptionVerifier: NewExceptionOwnershipVerifier(exceptionProvider),
		disputeVerifier:   NewDisputeOwnershipVerifier(disputeProvider),
		productionMode:    production,
	}, nil
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	return sharedhttp.StartHandlerSpan(c, name)
}

// The helpers below are defined as methods on *Handlers so they can read
// productionMode from the receiver. Previously they were package-level
// free functions reading a shared atomic.Bool, which coupled every test
// in the package to whichever test last constructed a handler.

func (handler *Handlers) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondError(fiberCtx *fiber.Ctx, status int, slug, message string) error {
	return sharedhttp.RespondError(fiberCtx, status, slug, message)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (handler *Handlers) badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.BadRequest(ctx, fiberCtx, span, logger, handler.productionMode, message, err)
}

func (handler *Handlers) notFound(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "not_found", message, err)
}

func (handler *Handlers) notFoundWithSlug(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	slug, message string,
	err error,
) error {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)

	return respondError(fiberCtx, fiber.StatusNotFound, slug, message)
}

func (handler *Handlers) unprocessable(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return handler.unprocessableWithSlug(ctx, fiberCtx, span, logger, "unprocessable_entity", message, err)
}

func (handler *Handlers) unprocessableWithSlug(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	slug, message string,
	err error,
) error {
	handler.logSpanError(ctx, span, logger, message, err)

	return respondError(fiberCtx, fiber.StatusUnprocessableEntity, slug, message)
}

func (handler *Handlers) exceptionNotFound(ctx context.Context, fiberCtx *fiber.Ctx, span trace.Span, logger libLog.Logger, message string, err error) error {
	return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "exception_not_found", message, err)
}

func (handler *Handlers) disputeNotFound(ctx context.Context, fiberCtx *fiber.Ctx, span trace.Span, logger libLog.Logger, message string, err error) error {
	return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "dispute_not_found", message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (handler *Handlers) internalError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.InternalError(ctx, fiberCtx, span, logger, handler.productionMode, message, err)
}

func (handler *Handlers) forbidden(ctx context.Context, fiberCtx *fiber.Ctx, span trace.Span, logger libLog.Logger, err error) error {
	const message = "access denied"

	if err == nil {
		err = fmt.Errorf("%w: %s", errForbidden, message)
	}

	libOpentelemetry.HandleSpanError(span, message, err)

	if logger != nil {
		logger.Log(ctx, libLog.LevelWarn, "access denied: "+message)
	}

	return respondError(fiberCtx, fiber.StatusForbidden, "forbidden", message)
}

type verificationErrorConfig struct {
	missingIDErr     error
	invalidIDErr     error
	invalidIDMessage string
	notFoundSlug     string
	notFoundErr      error
	notFoundMessage  string
	lookupMessage    string
}

func (handler *Handlers) handleVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
	config verificationErrorConfig,
) error {
	if errors.Is(err, config.missingIDErr) || errors.Is(err, config.invalidIDErr) {
		return handler.badRequest(ctx, fiberCtx, span, logger, config.invalidIDMessage, err)
	}

	if errors.Is(err, libHTTP.ErrTenantIDNotFound) || errors.Is(err, libHTTP.ErrInvalidTenantID) {
		handler.logSpanError(ctx, span, logger, "invalid tenant id", err)

		return respondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
	}

	if errors.Is(err, config.notFoundErr) {
		return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, config.notFoundSlug, config.notFoundMessage, err)
	}

	if errors.Is(err, libHTTP.ErrLookupFailed) {
		return handler.internalError(ctx, fiberCtx, span, logger, config.lookupMessage, err)
	}

	return handler.forbidden(ctx, fiberCtx, span, logger, err)
}

// handleExceptionVerificationError maps errors from ParseAndVerifyResourceScopedID to HTTP responses.
func (handler *Handlers) handleExceptionVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	return handler.handleVerificationError(ctx, fiberCtx, span, logger, err, verificationErrorConfig{
		missingIDErr:     ErrMissingExceptionID,
		invalidIDErr:     ErrInvalidExceptionID,
		invalidIDMessage: "invalid exception_id",
		notFoundSlug:     "exception_not_found",
		notFoundErr:      ErrExceptionNotFound,
		notFoundMessage:  "exception not found",
		lookupMessage:    "failed to verify exception access",
	})
}

// handleDisputeVerificationError maps errors from ParseAndVerifyResourceScopedID to HTTP responses.
func (handler *Handlers) handleDisputeVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	return handler.handleVerificationError(ctx, fiberCtx, span, logger, err, verificationErrorConfig{
		missingIDErr:     ErrMissingDisputeID,
		invalidIDErr:     ErrInvalidDisputeID,
		invalidIDMessage: "invalid dispute_id",
		notFoundSlug:     "dispute_not_found",
		notFoundErr:      ErrDisputeNotFound,
		notFoundMessage:  "dispute not found",
		lookupMessage:    "failed to verify dispute access",
	})
}
