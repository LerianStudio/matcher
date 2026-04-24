// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package connectors

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

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
