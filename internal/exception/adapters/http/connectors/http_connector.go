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
	"strings"
	"syscall"
	"time"
	"unicode"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/backoff"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// Dispatch errors.
var (
	ErrNonRetryableStatus     = errors.New("non-retryable HTTP status")
	ErrMaxRetriesExceeded     = errors.New("max retries exceeded")
	ErrUnsupportedTarget      = errors.New("unsupported routing target")
	ErrConnectorNotConfigured = errors.New("connector not configured for target")
	ErrRetryableHTTPStatus    = errors.New("retryable HTTP status")
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

// HTTPConnector dispatches exceptions to external systems via HTTP.
type HTTPConnector struct {
	client               *http.Client
	config               ConnectorConfig
	webhookTimeoutGetter func() time.Duration
}

// NewHTTPConnector creates a new HTTP connector with the given configuration.
// Unless AllowPrivateIPs is set, the HTTP client uses a custom net.Dialer with a
// ControlContext hook that validates resolved IP addresses at connection time,
// mitigating DNS rebinding / TOCTOU attacks.
func NewHTTPConnector(config ConnectorConfig) (*HTTPConnector, error) {
	transport := newSSRFSafeTransport(config.AllowPrivateIPs)

	client := &http.Client{
		Timeout:   DefaultTimeout,
		Transport: transport,
	}

	return &HTTPConnector{
		client: client,
		config: config,
	}, nil
}

// SetWebhookTimeoutGetter injects a live config-backed webhook timeout source.
func (conn *HTTPConnector) SetWebhookTimeoutGetter(getter func() time.Duration) {
	if conn == nil {
		return
	}

	conn.webhookTimeoutGetter = getter
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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to dispatch to JIRA: %v", err))

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
	timeout := webhookConfig.TimeoutOrDefault()
	if conn.webhookTimeoutGetter != nil {
		if runtimeTimeout := conn.webhookTimeoutGetter(); runtimeTimeout > 0 {
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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to dispatch to webhook: %v", err))

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
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed from tracking context

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

	resp, err := client.Do(reqCopy) // #nosec G704 -- URL is from validated connector configuration, not user input
	if err != nil {
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
