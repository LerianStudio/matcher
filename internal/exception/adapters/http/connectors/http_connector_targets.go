// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

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
