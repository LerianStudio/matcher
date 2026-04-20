// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

// runtimeSettingsResolver resolves runtime configuration values from the
// systemplane Client with fallback to the Config defaults.
type runtimeSettingsResolver struct {
	client *systemplane.Client
}

func newRuntimeSettingsResolver(client *systemplane.Client) *runtimeSettingsResolver {
	if client == nil {
		return nil
	}

	return &runtimeSettingsResolver{client: client}
}

func (resolver *runtimeSettingsResolver) rateLimit(fallback RateLimitConfig) RateLimitConfig {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	fallback.Enabled = SystemplaneGetBool(resolver.client, "rate_limit.enabled", fallback.Enabled)
	fallback.Max = SystemplaneGetInt(resolver.client, "rate_limit.max", fallback.Max)
	fallback.ExpirySec = SystemplaneGetInt(resolver.client, "rate_limit.expiry_sec", fallback.ExpirySec)
	fallback.ExportMax = SystemplaneGetInt(resolver.client, "rate_limit.export_max", fallback.ExportMax)
	fallback.ExportExpirySec = SystemplaneGetInt(resolver.client, "rate_limit.export_expiry_sec", fallback.ExportExpirySec)
	fallback.DispatchMax = SystemplaneGetInt(resolver.client, "rate_limit.dispatch_max", fallback.DispatchMax)
	fallback.DispatchExpirySec = SystemplaneGetInt(resolver.client, "rate_limit.dispatch_expiry_sec", fallback.DispatchExpirySec)
	fallback.AdminMax = SystemplaneGetInt(resolver.client, "rate_limit.admin_max", fallback.AdminMax)
	fallback.AdminExpirySec = SystemplaneGetInt(resolver.client, "rate_limit.admin_expiry_sec", fallback.AdminExpirySec)

	return fallback
}

func (resolver *runtimeSettingsResolver) idempotencyRetryWindow(fallback time.Duration) time.Duration {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	seconds := SystemplaneGetInt(resolver.client, "idempotency.retry_window_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) idempotencySuccessTTL(fallback time.Duration) time.Duration {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	hours := SystemplaneGetInt(resolver.client, "idempotency.success_ttl_hours", int(fallback/time.Hour))
	if hours <= 0 {
		return fallback
	}

	return time.Duration(hours) * time.Hour
}

func (resolver *runtimeSettingsResolver) idempotencyHMACSecret(fallback string) string {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	return SystemplaneGetString(resolver.client, "idempotency.hmac_secret", fallback)
}

func (resolver *runtimeSettingsResolver) callbackRateLimitPerMinute(fallback int) int {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	return SystemplaneGetInt(resolver.client, "callback_rate_limit.per_minute", fallback)
}

func (resolver *runtimeSettingsResolver) webhookTimeout(fallback time.Duration) time.Duration {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	seconds := SystemplaneGetInt(resolver.client, "webhook.timeout_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	if seconds > maxWebhookTimeoutSec {
		seconds = maxWebhookTimeoutSec
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) dedupeTTL(fallback time.Duration) time.Duration {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	seconds := SystemplaneGetInt(resolver.client, "deduplication.ttl_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) exportPresignExpiry(fallback time.Duration) time.Duration {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	seconds := SystemplaneGetInt(resolver.client, "export_worker.presign_expiry_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	if seconds > maxPresignExpirySec {
		seconds = maxPresignExpirySec
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) archivalPresignExpiry(fallback time.Duration) time.Duration {
	if resolver == nil || resolver.client == nil {
		return fallback
	}

	seconds := SystemplaneGetInt(resolver.client, "archival.presign_expiry_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	if seconds > maxPresignExpirySec {
		seconds = maxPresignExpirySec
	}

	return time.Duration(seconds) * time.Second
}
