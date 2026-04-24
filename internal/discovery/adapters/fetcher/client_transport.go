// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fetcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel/attribute"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libBackoff "github.com/LerianStudio/lib-commons/v5/commons/backoff"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// doGet performs a GET request with retry logic.
func (client *HTTPFetcherClient) doGet(ctx context.Context, requestURL string) ([]byte, error) {
	return client.doGetWithHeaders(ctx, requestURL, nil)
}

// doGetWithHeaders performs a GET request with optional extra headers and retry logic.
func (client *HTTPFetcherClient) doGetWithHeaders(ctx context.Context, requestURL string, headers map[string]string) ([]byte, error) {
	body, _, err := client.doRequestWithHeaders(ctx, http.MethodGet, requestURL, nil, true, headers)
	return body, err
}

// doPost performs a POST request without retry logic.
func (client *HTTPFetcherClient) doPost(ctx context.Context, requestURL string, body []byte) ([]byte, error) {
	respBody, _, err := client.doRequest(ctx, http.MethodPost, requestURL, body, false)
	return respBody, err
}

// doPostWithStatus performs a POST request without retry logic and also returns the
// HTTP status code so callers can distinguish semantically different success codes
// (e.g. 200 dedup vs 202 accepted).
func (client *HTTPFetcherClient) doPostWithStatus(ctx context.Context, requestURL string, body []byte) ([]byte, int, error) {
	return client.doRequest(ctx, http.MethodPost, requestURL, body, false)
}

func readBoundedBody(body io.Reader) ([]byte, error) {
	limitedReader := io.LimitReader(body, int64(maxResponseBodySize)+1)

	respBody, readErr := io.ReadAll(limitedReader)
	if readErr != nil {
		return nil, fmt.Errorf("read response body: %w", readErr)
	}

	if int64(len(respBody)) > int64(maxResponseBodySize) {
		return nil, fmt.Errorf("%w: response body exceeds %d bytes", ErrFetcherBadResponse, maxResponseBodySize)
	}

	return respBody, nil
}

func rejectEmptyOrNullBody(body []byte) error {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || trimmed == "null" {
		return fmt.Errorf("%w: null/empty payload", ErrFetcherBadResponse)
	}

	return nil
}

// maxBackoffDelay caps the exponential backoff to prevent indefinite waits
// when MaxRetries is set to a high value.
const maxBackoffDelay = 30 * time.Second

// doRequest performs an HTTP request with retry and exponential backoff.
func (client *HTTPFetcherClient) doRequest(ctx context.Context, method, requestURL string, body []byte, retryable bool) ([]byte, int, error) {
	return client.doRequestWithHeaders(ctx, method, requestURL, body, retryable, nil)
}

// doRequestWithHeaders performs an HTTP request with retry, exponential backoff, and optional extra headers.
//
//nolint:gocognit,gocyclo,cyclop // retry loop with error classification is inherently branchy; extraction done via classifyResponse.
func (client *HTTPFetcherClient) doRequestWithHeaders(ctx context.Context, method, requestURL string, body []byte, retryable bool, headers map[string]string) ([]byte, int, error) {
	if err := client.ensureReady(); err != nil {
		return nil, 0, err
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed for span

	ctx, span := tracer.Start(ctx, "fetcher.http.request")
	defer span.End()

	span.SetAttributes(
		attribute.String("http.method", method),
		attribute.String("http.url", requestURL),
		attribute.Bool("fetcher.retryable", retryable),
	)

	var lastErr error

	// retried401 tracks whether we have already retried once after a 401 response.
	// The canonical OAuth2 single-retry pattern: on the first 401, invalidate
	// cached credentials/tokens and retry once with fresh credentials. A second
	// 401 means the credentials themselves are genuinely invalid, so we stop.
	// This flag is independent of the 5xx retry counter (MaxRetries).
	retried401 := false

	attempts := 1
	if retryable {
		attempts += client.cfg.MaxRetries
	}

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := libBackoff.ExponentialWithJitter(client.cfg.RetryBaseDelay, attempt-1)
			if delay > maxBackoffDelay {
				delay = maxBackoffDelay
			}

			if err := libBackoff.WaitContext(ctx, delay); err != nil {
				return nil, 0, fmt.Errorf("request canceled: %w", err)
			}
		}

		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}

		req, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
		if err != nil {
			return nil, 0, fmt.Errorf("create request: %w", err)
		}

		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		// Apply extra headers (e.g., X-Product-Name).
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		// Inject per-tenant authentication when provider is available.
		// In single-tenant mode, m2mProvider is nil and no auth is injected.
		if err := client.injectAuth(ctx, req); err != nil {
			libOpentelemetry.HandleSpanError(span, "fetcher auth injection failed", err)
			return nil, 0, err
		}

		respBody, statusCode, err := client.doHTTPAttempt(req)
		if err != nil {
			// Circuit breaker open/half-open: fail fast without retrying.
			// Wrap with ErrFetcherUnavailable so downstream error normalization
			// (extraction_commands.go) correctly maps this to HTTP 503.
			if isBreakerRejection(err) {
				cbErr := fmt.Errorf("%w: %w: %w", sharedPorts.ErrFetcherUnavailable, ErrFetcherCircuitOpen, err)
				libOpentelemetry.HandleSpanError(span, "fetcher circuit breaker rejected request", cbErr)

				return nil, 0, cbErr
			}

			lastErr = fmt.Errorf("%w: %w", ErrFetcherUnreachable, err)
			libOpentelemetry.HandleSpanError(span, "fetcher http request failed", lastErr)

			if !retryable {
				return nil, 0, lastErr
			}

			continue
		}

		// Invalidate M2M credentials on 401 to force re-fetch from secret store.
		client.invalidateM2MOnUnauthorized(ctx, statusCode)

		// Canonical OAuth2 single-retry on 401: if this is the first 401 we've
		// seen, the cache has just been invalidated above. Retry immediately with
		// fresh credentials (no backoff needed — the delay would only slow down
		// legitimate credential rotation). This does NOT consume a 5xx retry slot:
		// decrement attempt so the for-loop's attempt++ restores it, making the
		// 401 retry "free" relative to the 5xx budget.
		//
		// Safe for POST (which is otherwise non-retryable): Fetcher may issue a
		// one-shot token that expires between acquisition and use, so a single
		// retry with a fresh token handles the token race without a user-visible
		// failure. The POST body is already buffered in `body []byte` above and
		// re-wrapped in a fresh bytes.Reader each iteration, so replay is safe.
		if statusCode == http.StatusUnauthorized && !retried401 {
			retried401 = true
			attempt-- // compensate for for-loop increment: 401 retry is outside the 5xx budget

			span.SetAttributes(attribute.Bool("fetcher.retried_401", true))

			continue
		}

		result, classifiedStatus, statusErr := classifyResponse(statusCode, respBody)
		if statusErr == nil {
			span.SetAttributes(attribute.Int("http.status_code", statusCode))

			return result, classifiedStatus, nil
		}

		libOpentelemetry.HandleSpanError(span, "fetcher classify response", statusErr)

		if statusCode >= http.StatusInternalServerError && retryable {
			lastErr = statusErr

			continue // retry on 5xx
		}

		return nil, classifiedStatus, statusErr
	}

	if retryable {
		return nil, 0, fmt.Errorf("exhausted retries: %w", lastErr)
	}

	return nil, 0, lastErr
}

// httpAttemptResult holds the outcome of a single HTTP round-trip so it can
// travel through the circuit breaker's func() (any, error) signature.
type httpAttemptResult struct {
	body       []byte
	statusCode int
}

// doHTTPAttempt executes a single HTTP round-trip, optionally through the
// circuit breaker when one is configured. It returns the response body, status
// code, and any transport-level error.
func (client *HTTPFetcherClient) doHTTPAttempt(req *http.Request) ([]byte, int, error) {
	if client.breaker == nil {
		return client.rawHTTPAttempt(req)
	}

	result, err := client.breaker.Execute(fetcherCircuitBreakerName, func() (any, error) {
		body, statusCode, httpErr := client.rawHTTPAttempt(req)
		if httpErr != nil {
			return nil, httpErr
		}

		// Report server errors as failures to the breaker so it can track them,
		// but still return the body and status so the caller can decide on retries.
		if statusCode >= http.StatusInternalServerError {
			return &httpAttemptResult{body: body, statusCode: statusCode},
				fmt.Errorf("%w: status %d", ErrFetcherServerError, statusCode)
		}

		return &httpAttemptResult{body: body, statusCode: statusCode}, nil
	})
	if err != nil {
		// If we got a result despite an error (5xx case), extract it.
		if result != nil {
			if attemptResult, ok := result.(*httpAttemptResult); ok {
				return attemptResult.body, attemptResult.statusCode, nil
			}
		}

		return nil, 0, fmt.Errorf("circuit breaker execute: %w", err)
	}

	attemptResult, ok := result.(*httpAttemptResult)
	if !ok {
		return nil, 0, fmt.Errorf("%w: unexpected circuit breaker result type", ErrFetcherBadResponse)
	}

	return attemptResult.body, attemptResult.statusCode, nil
}

// rawHTTPAttempt performs the actual HTTP call and reads the response body.
func (client *HTTPFetcherClient) rawHTTPAttempt(req *http.Request) ([]byte, int, error) {
	// The request URL is built from the configured and validated baseURL
	// (see NewHTTPFetcherClient / Validate) combined with well-known API
	// path segments — it is not constructed from untrusted user input.
	resp, err := client.httpClient.Do(req) // #nosec G704 -- URL comes from validated fetcher config, not user input
	if err != nil {
		return nil, 0, fmt.Errorf("fetcher http request: %w", err)
	}

	respBody, bodyErr := func() ([]byte, error) {
		defer func() {
			_ = resp.Body.Close()
		}()

		return readBoundedBody(resp.Body)
	}()
	if bodyErr != nil {
		return nil, 0, bodyErr
	}

	return respBody, resp.StatusCode, nil
}

// isBreakerRejection returns true when the error originates from the circuit
// breaker rejecting a request (open or half-open state), as opposed to an
// error from the wrapped HTTP call itself.
func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
