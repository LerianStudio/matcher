// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/auth"
)

var (
	// ErrProtectedRouteHelperRequired indicates protected route helper is nil.
	ErrProtectedRouteHelperRequired = errors.New("protected route helper is required")
	// ErrHandlersRequired indicates handlers are nil.
	ErrHandlersRequired = errors.New("exception handlers are required")
	// ErrDispatchLimiterRequired indicates dispatch rate limiter is nil.
	ErrDispatchLimiterRequired = errors.New("dispatch rate limiter is required")
)

// RegisterRoutes registers the exception HTTP routes with dispatch rate limiting.
func RegisterRoutes(
	protected func(resource string, actions ...string) fiber.Router,
	handlers *Handlers,
	dispatchLimiter fiber.Handler,
) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handlers == nil {
		return ErrHandlersRequired
	}

	if dispatchLimiter == nil {
		return ErrDispatchLimiterRequired
	}

	// Exception query endpoints
	protected(
		auth.ResourceException,
		auth.ActionExceptionRead,
	).Get("/v1/exceptions", handlers.ListExceptions)
	protected(
		auth.ResourceException,
		auth.ActionExceptionRead,
	).Get("/v1/exceptions/:exceptionId", handlers.GetException)
	protected(
		auth.ResourceException,
		auth.ActionExceptionRead,
	).Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	// Bulk exception endpoints (registered before parameterized :exceptionId routes
	// to prevent Fiber from matching "bulk" as an :exceptionId parameter).
	protected(
		auth.ResourceException,
		auth.ActionExceptionResolve,
	).Post("/v1/exceptions/bulk/assign", handlers.BulkAssign)
	protected(
		auth.ResourceException,
		auth.ActionExceptionResolve,
	).Post("/v1/exceptions/bulk/resolve", handlers.BulkResolve)
	protected(
		auth.ResourceException,
		auth.ActionExceptionDispatch,
	).Post("/v1/exceptions/bulk/dispatch", dispatchLimiter, handlers.BulkDispatch)

	// Exception resolution endpoints
	protected(
		auth.ResourceException,
		auth.ActionExceptionResolve,
	).Post("/v1/exceptions/:exceptionId/force-match", handlers.ForceMatch)
	protected(
		auth.ResourceException,
		auth.ActionExceptionResolve,
	).Post("/v1/exceptions/:exceptionId/adjust-entry", handlers.AdjustEntry)

	// Exception dispatch endpoint (rate limited to prevent external system overload)
	protected(
		auth.ResourceException,
		auth.ActionExceptionDispatch,
	).Post("/v1/exceptions/:exceptionId/dispatch", dispatchLimiter, handlers.DispatchToExternal)

	// Callback endpoint for external system webhooks (rate limited for defense-in-depth;
	// the CallbackUseCase also has semantic per-external-system rate limiting via Redis).
	protected(
		auth.ResourceException,
		auth.ActionCallbackProcess,
	).Post("/v1/exceptions/:exceptionId/callback", dispatchLimiter, handlers.ProcessCallback)

	// Comment endpoints on exceptions
	protected(
		auth.ResourceException,
		auth.ActionExceptionRead,
	).Get("/v1/exceptions/:exceptionId/comments", handlers.ListComments)
	protected(
		auth.ResourceException,
		auth.ActionCommentWrite,
	).Post("/v1/exceptions/:exceptionId/comments", handlers.AddComment)
	protected(
		auth.ResourceException,
		auth.ActionCommentWrite,
	).Delete("/v1/exceptions/:exceptionId/comments/:commentId", handlers.DeleteComment)

	// Dispute endpoints on exceptions
	protected(
		auth.ResourceException,
		auth.ActionDisputeWrite,
	).Post("/v1/exceptions/:exceptionId/disputes", handlers.OpenDispute)

	// Dispute query endpoints
	protected(
		auth.ResourceException,
		auth.ActionDisputeRead,
	).Get("/v1/disputes", handlers.ListDisputes)
	protected(
		auth.ResourceException,
		auth.ActionDisputeRead,
	).Get("/v1/disputes/:disputeId", handlers.GetDispute)

	// Dispute operations
	protected(
		auth.ResourceException,
		auth.ActionDisputeWrite,
	).Post("/v1/disputes/:disputeId/close", handlers.CloseDispute)
	protected(
		auth.ResourceException,
		auth.ActionDisputeWrite,
	).Post("/v1/disputes/:disputeId/evidence", handlers.SubmitEvidence)

	return nil
}
