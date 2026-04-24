// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"time"
)

func schedulerInterval(cfg *Config) time.Duration {
	return cfg.SchedulerInterval()
}

func runtimeConfigOrFallback(cfg *Config, configGetter func() *Config) *Config {
	if configGetter != nil {
		if runtimeCfg := configGetter(); runtimeCfg != nil {
			return runtimeCfg
		}
	}

	return cfg
}

func resolveRuntimeDurationSetting(
	_ context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	fallbackFn func(*Config) time.Duration,
	resolverFn func(time.Duration) time.Duration,
) time.Duration {
	runtimeCfg := runtimeConfigOrFallback(cfg, configGetter)

	fallback := fallbackFn(cfg)
	if runtimeCfg != nil {
		fallback = fallbackFn(runtimeCfg)
	}

	if settingsResolver == nil {
		return fallback
	}

	return resolverFn(fallback)
}

func resolveRuntimeIntSetting(
	_ context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	fallbackFn func(*Config) int,
	resolverFn func(int) int,
) int {
	runtimeCfg := runtimeConfigOrFallback(cfg, configGetter)

	fallback := fallbackFn(cfg)
	if runtimeCfg != nil {
		fallback = fallbackFn(runtimeCfg)
	}

	if settingsResolver == nil {
		return fallback
	}

	return resolverFn(fallback)
}

func resolveRuntimeStringSetting(
	_ context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	fallbackFn func(*Config) string,
	resolverFn func(string) string,
) string {
	runtimeCfg := runtimeConfigOrFallback(cfg, configGetter)

	fallback := fallbackFn(cfg)
	if runtimeCfg != nil {
		fallback = fallbackFn(runtimeCfg)
	}

	if settingsResolver == nil {
		return fallback
	}

	return resolverFn(fallback)
}

func resolveIdempotencyRetryWindow(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return current.IdempotencyRetryWindow() },
		settingsResolver.idempotencyRetryWindow,
	)
}

func resolveIdempotencySuccessTTL(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return current.IdempotencySuccessTTL() },
		settingsResolver.idempotencySuccessTTL,
	)
}

func resolveIdempotencyHMACSecret(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) string {
	return resolveRuntimeStringSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) string { return current.Idempotency.HMACSecret },
		settingsResolver.idempotencyHMACSecret,
	)
}

func resolveCallbackRateLimit(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) int {
	return resolveRuntimeIntSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) int { return current.CallbackRateLimitPerMinute() },
		settingsResolver.callbackRateLimitPerMinute,
	)
}

func resolveWebhookTimeout(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return configuredWebhookTimeout(ctx, current) },
		settingsResolver.webhookTimeout,
	)
}

func resolveDedupeTTL(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return current.DedupeTTL() },
		settingsResolver.dedupeTTL,
	)
}

func resolveExportPresignExpiry(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return configuredExportPresignExpiry(ctx, current) },
		settingsResolver.exportPresignExpiry,
	)
}

func configuredWebhookTimeout(ctx context.Context, cfg *Config) time.Duration {
	return normalizedWebhookTimeout(ctx, cfg)
}

func configuredExportPresignExpiry(ctx context.Context, cfg *Config) time.Duration {
	return normalizedExportPresignExpiry(ctx, cfg)
}
