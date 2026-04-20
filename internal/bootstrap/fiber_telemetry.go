// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	libMetrics "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry/metrics"
)

func telemetryMiddleware(
	logger libLog.Logger,
	tracer trace.Tracer,
	metricFactory *libMetrics.MetricsFactory,
) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		ctx := libOpentelemetry.ExtractHTTPContext(fiberCtx.UserContext(), fiberCtx)
		localRequestID, _ := fiberCtx.Locals(requestid.ConfigDefault.ContextKey).(string)

		requestID := strings.TrimSpace(localRequestID)

		if requestID == "" {
			requestID = strings.TrimSpace(fiberCtx.Get("X-Request-ID"))
		}

		headerID := sanitizeHeaderID(requestID)
		fiberCtx.Set("X-Request-ID", headerID)

		// Start a span for the HTTP request with semantic convention attributes
		method := fiberCtx.Method()

		var route string
		if r := fiberCtx.Route(); r != nil {
			route = r.Path
		}

		if route == "" {
			route = fiberCtx.Path()
		}

		spanName := fmt.Sprintf("%s %s", method, route)

		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		// Set HTTP semantic convention attributes (required for spanmetrics connector)
		span.SetAttributes(
			semconv.HTTPMethod(method),
			semconv.HTTPRoute(route),
			semconv.HTTPScheme(fiberCtx.Protocol()),
			semconv.HTTPTarget(fiberCtx.OriginalURL()),
			semconv.NetHostName(fiberCtx.Hostname()),
		)

		if headerID != "" {
			span.SetAttributes(attribute.String("request_id", headerID))
		}

		ctx = libCommons.ContextWithLogger(ctx, logger)
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		ctx = libCommons.ContextWithHeaderID(ctx, headerID)
		ctx = libCommons.ContextWithMetricFactory(ctx, metricFactory)
		fiberCtx.SetUserContext(ctx)

		// Execute the request handler
		err := fiberCtx.Next()

		// Set HTTP status code attribute after handler completes (required for spanmetrics)
		statusCode := fiberCtx.Response().StatusCode()
		span.SetAttributes(semconv.HTTPStatusCode(statusCode))

		// Record error on span if handler returned an error
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "request handler error", err)
		}

		// Mark span as error if status code >= 400
		if statusCode >= http.StatusBadRequest {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
		}

		return err
	}
}
