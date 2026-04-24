// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
)

const defaultFallbackTracerName = "commons.default"

// StartHandlerSpan starts a handler span using the shared default tracer fallback.
func StartHandlerSpan(fiberCtx *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	return StartHandlerSpanWithFallback(fiberCtx, name, defaultFallbackTracerName)
}

// StartHandlerSpanWithFallback starts a handler span using the provided fallback tracer name.
func StartHandlerSpanWithFallback(
	fiberCtx *fiber.Ctx,
	name, fallbackTracerName string,
) (context.Context, trace.Span, libLog.Logger) {
	if fiberCtx == nil {
		ctx := context.Background()
		tracer := otel.Tracer(fallbackTracerName)
		ctx, span := tracer.Start(ctx, name)

		return ctx, span, nil
	}

	ctx := fiberCtx.UserContext()
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if tracer == nil {
		tracer = otel.Tracer(fallbackTracerName)
	}

	ctx, span := tracer.Start(ctx, name)

	return ctx, span, logger
}

// LogSpanError records an error in the span and writes a safe log entry.
func LogSpanError(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	production bool,
	message string,
	err error,
) {
	libOpentelemetry.HandleSpanError(span, message, err)
	libLog.SafeError(logger, ctx, message, err, production)
}

// BadRequest logs the transport failure and responds with the standard invalid request payload.
func BadRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	production bool,
	message string,
	err error,
) error {
	LogSpanError(ctx, span, logger, production, message, err)

	return RespondProductError(fiberCtx, NewError(defInvalidRequest, message, nil))
}

// InternalError logs the transport failure and responds with the standard internal error payload.
func InternalError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	production bool,
	message string,
	err error,
) error {
	LogSpanError(ctx, span, logger, production, message, err)

	return RespondProductError(fiberCtx, NewError(defInternalServerError, defaultInternalErrorMessage, nil))
}
