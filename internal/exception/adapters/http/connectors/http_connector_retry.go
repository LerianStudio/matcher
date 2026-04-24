// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package connectors

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
)

func (conn *HTTPConnector) executeWithRetry(
	ctx context.Context,
	client *http.Client,
	req *http.Request,
	maxRetries int,
	baseBackoff time.Duration,
) (*http.Response, error) {
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed

	bodyBytes, err := readRequestBody(ctx, req, logger)
	if err != nil {
		return nil, err
	}

	var lastErr error

	for attempt := range maxRetries {
		resp, retry, err := conn.executeAttempt(
			ctx,
			client,
			req,
			bodyBytes,
			attempt,
			maxRetries,
			logger,
		)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		if !retry {
			return nil, err
		}

		if err := sleepWithContext(ctx, retryDelay(baseBackoff, attempt)); err != nil {
			return nil, err
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrMaxRetriesExceeded, lastErr)
	}

	return nil, ErrMaxRetriesExceeded
}

func (conn *HTTPConnector) executeAttempt(
	ctx context.Context,
	client *http.Client,
	req *http.Request,
	bodyBytes []byte,
	attempt int,
	maxRetries int,
	logger libLog.Logger,
) (*http.Response, bool, error) {
	reqCopy := req.Clone(ctx)
	if bodyBytes != nil {
		reqCopy.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	resp, err := conn.doHTTPWithBreaker(client, reqCopy)
	if err != nil {
		// Circuit breaker open/half-open: fail fast, do not retry.
		if isConnectorBreakerRejection(err) {
			return nil, false, fmt.Errorf("%w: %w", ErrCircuitBreakerOpen, err)
		}

		retry := attempt < maxRetries-1

		return nil, retry, fmt.Errorf("attempt %d/%d: %w", attempt+1, maxRetries, err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, false, nil
	}

	// Read, sanitize, and truncate response body for safe inclusion in error messages.
	sanitizedBody := readResponseBody(ctx, resp, logger)

	bodyContext := ""
	if sanitizedBody != "" {
		bodyContext = ": " + sanitizedBody
	}

	if !isRetryableStatus(resp.StatusCode) {
		return nil, false, fmt.Errorf("%w: %d%s", ErrNonRetryableStatus, resp.StatusCode, bodyContext)
	}

	retryErr := fmt.Errorf("%w: attempt %d/%d status %d%s",
		ErrRetryableHTTPStatus, attempt+1, maxRetries, resp.StatusCode, bodyContext)
	logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("retryable error on attempt %d: %v", attempt+1, retryErr))

	retry := attempt < maxRetries-1

	return nil, retry, retryErr
}

// doHTTPWithBreaker executes a single HTTP call, optionally through a
// per-target circuit breaker when one is configured.
func (conn *HTTPConnector) doHTTPWithBreaker(client *http.Client, req *http.Request) (*http.Response, error) {
	if conn.breaker == nil {
		resp, err := client.Do(req) // #nosec G704 -- URL is from validated connector configuration, not user input
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrHTTPClientDo, err)
		}

		return resp, nil
	}

	serviceName := connectorBreakerName(req.URL)

	// Lazily register a breaker for this target host if not yet created.
	if _, err := conn.breaker.GetOrCreate(serviceName, circuitbreaker.HTTPServiceConfig()); err != nil {
		// If config mismatch (already created with same config), ignore; any
		// other error is unexpected — log degradation and fall through without breaker.
		if !errors.Is(err, circuitbreaker.ErrConfigMismatch) {
			logger, _, _, _ := libCommons.NewTrackingFromContext(req.Context())
			logger.Log(req.Context(), libLog.LevelWarn,
				fmt.Sprintf("circuit breaker registration failed for %s, falling back to unprotected request: %v", serviceName, err))

			span := trace.SpanFromContext(req.Context())
			libOpentelemetry.HandleSpanError(span, "circuit breaker registration failed, falling back to unprotected request", err)

			resp, doErr := client.Do(req) // #nosec G704 -- fallback without breaker
			if doErr != nil {
				return nil, fmt.Errorf("%w: %w", ErrHTTPClientDo, doErr)
			}

			return resp, nil
		}
	}

	result, err := conn.breaker.Execute(serviceName, func() (any, error) {
		resp, doErr := client.Do(req) // #nosec G704 -- URL is from validated connector configuration, not user input
		if doErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrHTTPClientDo, doErr)
		}

		// Report server errors as failures to the breaker and close the body
		// so it does not leak — the error message carries the status code for
		// diagnostic purposes.
		if resp.StatusCode >= http.StatusInternalServerError {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("%w: status %d", ErrServerError, resp.StatusCode)
		}

		return resp, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCircuitBreakerExecute, err)
	}

	resp, ok := result.(*http.Response)
	if !ok {
		return nil, ErrUnexpectedCircuitBreakerResult
	}

	return resp, nil
}

// connectorBreakerName derives a per-target circuit breaker service name from
// the request URL. Different webhook targets (different hosts) get independent
// breakers so that one failing target does not block dispatches to healthy ones.
func connectorBreakerName(u *url.URL) string {
	if u == nil {
		return connectorCircuitBreakerPrefix + "unknown"
	}

	return connectorCircuitBreakerPrefix + u.Host
}

// isConnectorBreakerRejection returns true when the error originates from the
// circuit breaker rejecting a request (open or half-open state).
func isConnectorBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
