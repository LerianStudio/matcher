// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package connectors

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/backoff"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// Dispatch errors.
var (
	ErrNonRetryableStatus             = errors.New("non-retryable HTTP status")
	ErrMaxRetriesExceeded             = errors.New("max retries exceeded")
	ErrUnsupportedTarget              = errors.New("unsupported routing target")
	ErrConnectorNotConfigured         = ports.ErrConnectorNotConfigured
	ErrRetryableHTTPStatus            = errors.New("retryable HTTP status")
	ErrCircuitBreakerOpen             = errors.New("circuit breaker is open for target")
	ErrServerError                    = errors.New("server error")
	ErrUnexpectedCircuitBreakerResult = errors.New("unexpected circuit breaker result type")
	ErrHTTPClientDo                   = errors.New("http client request failed")
	ErrCircuitBreakerExecute          = errors.New("circuit breaker execute failed")
)

// Response body sanitization limits.
const (
	// maxBodyLogLength is the maximum number of characters from a response body
	// that may appear in error messages and log output after sanitization.
	maxBodyLogLength = 200

	// maxBodyReadLimit is the maximum number of bytes read from a response body
	// for error/log context before sanitization is applied.
	maxBodyReadLimit = 1024
)

// connectorCircuitBreakerPrefix is prepended to target hosts to form the
// per-target circuit breaker service name.
const connectorCircuitBreakerPrefix = "webhook-"

// HTTPConnector dispatches exceptions to external systems via HTTP.
type HTTPConnector struct {
	client                 *http.Client
	config                 ConnectorConfig
	webhookTimeoutResolver func(context.Context) time.Duration
	breaker                circuitbreaker.Manager
}

// NewHTTPConnector creates a new HTTP connector with the given configuration.
// Unless AllowPrivateIPs is set, the HTTP client uses a custom net.Dialer with a
// ControlContext hook that validates resolved IP addresses at connection time,
// mitigating DNS rebinding / TOCTOU attacks.
// The optional circuitbreaker.Manager protects outbound calls; when nil the
// connector operates without circuit-breaker protection (backward-compatible).
func NewHTTPConnector(config ConnectorConfig, breaker ...circuitbreaker.Manager) (*HTTPConnector, error) {
	transport := newSSRFSafeTransport(config.AllowPrivateIPs)

	client := &http.Client{
		Timeout:   DefaultTimeout,
		Transport: transport,
	}

	connector := &HTTPConnector{
		client: client,
		config: config,
	}

	if len(breaker) > 0 && breaker[0] != nil {
		connector.breaker = breaker[0]
	}

	return connector, nil
}

// SetWebhookTimeoutResolver injects a context-aware runtime webhook timeout source.
func (conn *HTTPConnector) SetWebhookTimeoutResolver(resolver func(context.Context) time.Duration) {
	if conn == nil {
		return
	}

	conn.webhookTimeoutResolver = resolver
}

// newSSRFSafeTransport returns an *http.Transport with a ControlContext hook
// that rejects connections to private/loopback IP addresses at dial time.
// When allowPrivate is true, the hook is omitted (for dev/test environments).
func newSSRFSafeTransport(allowPrivate bool) *http.Transport {
	if allowPrivate {
		return &http.Transport{}
	}

	dialer := &net.Dialer{
		ControlContext: func(_ context.Context, _, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("split host/port: %w", err)
			}

			ip := net.ParseIP(host)
			if ip != nil && isPrivateIP(ip) {
				return ErrPrivateIPNotAllowed
			}

			return nil
		},
	}

	return &http.Transport{
		DialContext: dialer.DialContext,
	}
}

func (conn *HTTPConnector) clientWithTimeout(timeout time.Duration) *http.Client {
	transport := conn.client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	return &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: conn.client.CheckRedirect,
		Jar:           conn.client.Jar,
	}
}

// Dispatch sends an exception to an external system based on the routing decision.
func (conn *HTTPConnector) Dispatch(
	ctx context.Context,
	exceptionID string,
	decision services.RoutingDecision,
	payload []byte,
) (ports.DispatchResult, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "http_connector.dispatch")
	defer span.End()

	switch decision.Target {
	case services.RoutingTargetManual:
		return ports.DispatchResult{
			Target:       decision.Target,
			Acknowledged: true,
		}, nil

	case services.RoutingTargetJira:
		return conn.dispatchToJira(ctx, exceptionID, decision, payload)

	case services.RoutingTargetWebhook:
		return conn.dispatchToWebhook(ctx, exceptionID, decision, payload)

	case services.RoutingTargetServiceNow:
		err := fmt.Errorf("%w: SERVICENOW", ErrUnsupportedTarget)
		libOpentelemetry.HandleSpanError(span, "unsupported target", err)

		logger.Log(ctx, libLog.LevelWarn, "ServiceNow connector not implemented: "+exceptionID)

		return ports.DispatchResult{}, err

	default:
		err := fmt.Errorf("%w: %s", ErrUnsupportedTarget, decision.Target)
		libOpentelemetry.HandleSpanError(span, "unknown target", err)

		return ports.DispatchResult{}, err
	}
}

func (conn *HTTPConnector) dispatchToJira(
	ctx context.Context,
	exceptionID string,
	decision services.RoutingDecision,
	payload []byte,
) (ports.DispatchResult, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "http_connector.dispatch_to_jira")
	defer span.End()

	if conn.config.Jira == nil {
		err := fmt.Errorf("%w: JIRA", ErrConnectorNotConfigured)
		libOpentelemetry.HandleSpanError(span, "jira not configured", err)

		return ports.DispatchResult{}, err
	}

	jiraConfig := conn.config.Jira
	issueURL := jiraConfig.BaseURL + "/rest/api/2/issue"
	client := conn.clientWithTimeout(jiraConfig.TimeoutOrDefault())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, issueURL, bytes.NewReader(payload))
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create request", err)

		return ports.DispatchResult{}, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+jiraConfig.AuthToken)
	req.Header.Set(
		"X-Idempotency-Key",
		generateIdempotencyKey(exceptionID, decision.Target, decision.Queue),
	)

	resp, err := conn.executeWithRetry(
		ctx,
		client,
		req,
		jiraConfig.MaxRetriesOrDefault(),
		jiraConfig.RetryBackoffOrDefault(),
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "jira dispatch failed", err)

		libLog.SafeError(logger, ctx, "failed to dispatch to JIRA", err, runtime.IsProductionMode())

		return ports.DispatchResult{}, fmt.Errorf("dispatch to jira: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close jira response body: %v", err))
		}
	}()

	var jiraResp struct {
		Key string `json:"key"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&jiraResp); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to decode jira response", err)

		return ports.DispatchResult{}, fmt.Errorf("decode jira response: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("dispatched exception %s to JIRA: %s", exceptionID, jiraResp.Key))

	return ports.DispatchResult{
		Target:            decision.Target,
		ExternalReference: jiraResp.Key,
		Acknowledged:      true,
	}, nil
}

func (conn *HTTPConnector) dispatchToWebhook(
	ctx context.Context,
	exceptionID string,
	decision services.RoutingDecision,
	payload []byte,
) (ports.DispatchResult, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "http_connector.dispatch_to_webhook")
	defer span.End()

	if conn.config.Webhook == nil {
		err := fmt.Errorf("%w: WEBHOOK", ErrConnectorNotConfigured)
		libOpentelemetry.HandleSpanError(span, "webhook not configured", err)

		return ports.DispatchResult{}, err
	}

	webhookConfig := conn.config.Webhook

	// SEC-27: fail closed when the deployment has opted in to signed
	// payloads but has not configured a shared secret. Without this check
	// the earlier warn-log path would silently dispatch unsigned payloads
	// — the whole point of RequireSignedPayloads is to make that
	// combination refuse to send rather than only log about it.
	if webhookConfig.RequireSignedPayloads && strings.TrimSpace(webhookConfig.SharedSecret) == "" {
		err := ErrWebhookMissingSharedSecret
		libOpentelemetry.HandleSpanError(span, "webhook missing shared secret", err)
		logger.With(
			libLog.String("exception_id", exceptionID),
			libLog.String("target", string(decision.Target)),
		).Log(ctx, libLog.LevelError, "refusing unsigned webhook dispatch: RequireSignedPayloads is true but SharedSecret is empty")

		return ports.DispatchResult{}, fmt.Errorf("dispatch to webhook: %w", err)
	}

	timeout := webhookConfig.TimeoutOrDefault()

	if conn.webhookTimeoutResolver != nil {
		if runtimeTimeout := conn.webhookTimeoutResolver(ctx); runtimeTimeout > 0 {
			timeout = runtimeTimeout
		}
	}

	client := conn.clientWithTimeout(timeout)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		webhookConfig.URL,
		bytes.NewReader(payload),
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create request", err)

		return ports.DispatchResult{}, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"X-Idempotency-Key",
		generateIdempotencyKey(exceptionID, decision.Target, decision.Queue),
	)

	if webhookConfig.SharedSecret != "" {
		signature := computeHMACSHA256(payload, webhookConfig.SharedSecret)
		req.Header.Set("X-Signature-256", signature)
	} else {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("webhook shared secret is not configured for exception %s: "+
			"payloads will be unsigned and vulnerable to spoofing", exceptionID))
	}

	resp, err := conn.executeWithRetry(
		ctx,
		client,
		req,
		webhookConfig.MaxRetriesOrDefault(),
		webhookConfig.RetryBackoffOrDefault(),
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "webhook dispatch failed", err)

		libLog.SafeError(logger, ctx, "failed to dispatch to webhook", err, runtime.IsProductionMode())

		return ports.DispatchResult{}, fmt.Errorf("dispatch to webhook: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close webhook response body: %v", err))
		}
	}()

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("dispatched exception %s to webhook", exceptionID))

	return ports.DispatchResult{
		Target:       decision.Target,
		Acknowledged: true,
	}, nil
}

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

func readRequestBody(ctx context.Context, req *http.Request, logger libLog.Logger) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}

	if err := req.Body.Close(); err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close request body: %v", err))
	}

	return bodyBytes, nil
}

// readResponseBody reads a limited amount from the response body, closes it,
// and returns a sanitized string safe for error messages and log output.
func readResponseBody(ctx context.Context, resp *http.Response, logger libLog.Logger) string {
	if resp.Body == nil {
		return ""
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close response body: %v", err))
		}
	}()

	limited := io.LimitReader(resp.Body, maxBodyReadLimit)

	bodyBytes, err := io.ReadAll(limited)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to read response body for error context: %v", err))

		return "[UNREADABLE]"
	}

	if len(bodyBytes) == 0 {
		return ""
	}

	return sanitizeBody(string(bodyBytes))
}

// sanitizeBody strips non-printable/control characters and truncates content
// to a safe length for inclusion in error messages and log output.
func sanitizeBody(raw string) string {
	sanitized := strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}

		return -1
	}, raw)

	sanitized = strings.TrimSpace(sanitized)

	if len(sanitized) > maxBodyLogLength {
		return sanitized[:maxBodyLogLength] + " [TRUNCATED]"
	}

	return sanitized
}

func retryDelay(baseBackoff time.Duration, attempt int) time.Duration {
	return backoff.ExponentialWithJitter(baseBackoff, attempt)
}

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	if err := backoff.WaitContext(ctx, duration); err != nil {
		return fmt.Errorf("sleep interrupted: %w", err)
	}

	return nil
}

func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func computeHMACSHA256(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)

	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// generateIdempotencyKey builds a deterministic key for dispatch deduplication.
// The same exception dispatched to the same target always produces the same key,
// ensuring at-most-once delivery within the Redis TTL window. Key expiration for
// retries is handled by Redis TTL, so no timestamp component is needed.
func generateIdempotencyKey(
	exceptionID string,
	target services.RoutingTarget,
	queue string,
) string {
	key := fmt.Sprintf("dispatch:%s:%s", target, exceptionID)

	if queue != "" {
		key = fmt.Sprintf("%s:%s", key, queue)
	}

	return key
}
