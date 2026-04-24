// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
)

// validateAuthConfig validates authentication configuration when auth is enabled.
func (cfg *Config) validateAuthConfig(ctx context.Context, asserter *assert.Asserter) error {
	if !cfg.Auth.Enabled {
		return nil
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Auth.Host), "PLUGIN_AUTH_ADDRESS is required when PLUGIN_AUTH_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Auth.TokenSecret), "AUTH_JWT_SECRET is required when PLUGIN_AUTH_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// validateRateLimitConfig validates rate limiting configuration.
// Export limits are always validated; global and dispatch limits only when rate limiting is enabled.
func (cfg *Config) validateRateLimitConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := cfg.validateExportRateLimitConfig(ctx, asserter); err != nil {
		return err
	}

	if !cfg.RateLimit.Enabled {
		return nil
	}

	return cfg.validateActiveRateLimitConfig(ctx, asserter)
}

// validatePositiveBounded validates that value is strictly positive and does
// not exceed max. Both failure paths wrap the assertion error with the same
// "config validation:" prefix the rate-limit checks use throughout.
func validatePositiveBounded(
	ctx context.Context,
	asserter *assert.Asserter,
	value int,
	fieldKey, positiveMsg string,
	maxValue int,
	maxMsg string,
) error {
	if err := asserter.That(ctx, value > 0, positiveMsg, fieldKey, value); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, value <= maxValue, maxMsg, fieldKey, value); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// validateExportRateLimitConfig validates export-specific rate limit bounds.
func (cfg *Config) validateExportRateLimitConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.ExportMax,
		"export_rate_limit_max",
		"EXPORT_RATE_LIMIT_MAX must be positive",
		maxRateLimitRequestsPerWindow,
		"EXPORT_RATE_LIMIT_MAX must not exceed 1000000"); err != nil {
		return err
	}

	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.ExportExpirySec,
		"export_rate_limit_expiry",
		"EXPORT_RATE_LIMIT_EXPIRY_SEC must be positive",
		maxRateLimitWindowSeconds,
		"EXPORT_RATE_LIMIT_EXPIRY_SEC must not exceed 86400"); err != nil {
		return err
	}

	return nil
}

// validateActiveRateLimitConfig validates global and dispatch rate limit bounds
// when rate limiting is enabled.
func (cfg *Config) validateActiveRateLimitConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.Max,
		"rate_limit_max",
		"RATE_LIMIT_MAX must be positive",
		maxRateLimitRequestsPerWindow,
		"RATE_LIMIT_MAX must not exceed 1000000"); err != nil {
		return err
	}

	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.ExpirySec,
		"rate_limit_expiry",
		"RATE_LIMIT_EXPIRY_SEC must be positive",
		maxRateLimitWindowSeconds,
		"RATE_LIMIT_EXPIRY_SEC must not exceed 86400"); err != nil {
		return err
	}

	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.DispatchMax,
		"dispatch_rate_limit_max",
		"DISPATCH_RATE_LIMIT_MAX must be positive",
		maxRateLimitRequestsPerWindow,
		"DISPATCH_RATE_LIMIT_MAX must not exceed 1000000"); err != nil {
		return err
	}

	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.DispatchExpirySec,
		"dispatch_rate_limit_expiry",
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC must be positive",
		maxRateLimitWindowSeconds,
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC must not exceed 86400"); err != nil {
		return err
	}

	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.AdminMax,
		"admin_rate_limit_max",
		"ADMIN_RATE_LIMIT_MAX must be positive",
		maxRateLimitRequestsPerWindow,
		"ADMIN_RATE_LIMIT_MAX must not exceed 1000000"); err != nil {
		return err
	}

	if err := validatePositiveBounded(ctx, asserter, cfg.RateLimit.AdminExpirySec,
		"admin_rate_limit_expiry",
		"ADMIN_RATE_LIMIT_EXPIRY_SEC must be positive",
		maxRateLimitWindowSeconds,
		"ADMIN_RATE_LIMIT_EXPIRY_SEC must not exceed 86400"); err != nil {
		return err
	}

	return nil
}
