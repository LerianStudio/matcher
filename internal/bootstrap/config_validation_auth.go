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

// validateExportRateLimitConfig validates export-specific rate limit bounds.
func (cfg *Config) validateExportRateLimitConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := asserter.That(ctx, cfg.RateLimit.ExportMax > 0, "EXPORT_RATE_LIMIT_MAX must be positive", "export_rate_limit_max", cfg.RateLimit.ExportMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExportMax <= maxRateLimitRequestsPerWindow,
		"EXPORT_RATE_LIMIT_MAX must not exceed 1000000",
		"export_rate_limit_max", cfg.RateLimit.ExportMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExportExpirySec > 0, "EXPORT_RATE_LIMIT_EXPIRY_SEC must be positive", "export_rate_limit_expiry", cfg.RateLimit.ExportExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExportExpirySec <= maxRateLimitWindowSeconds,
		"EXPORT_RATE_LIMIT_EXPIRY_SEC must not exceed 86400",
		"export_rate_limit_expiry", cfg.RateLimit.ExportExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// validateActiveRateLimitConfig validates global and dispatch rate limit bounds
// when rate limiting is enabled.
func (cfg *Config) validateActiveRateLimitConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := asserter.That(ctx, cfg.RateLimit.Max > 0, "RATE_LIMIT_MAX must be positive", "rate_limit_max", cfg.RateLimit.Max); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.Max <= maxRateLimitRequestsPerWindow,
		"RATE_LIMIT_MAX must not exceed 1000000",
		"rate_limit_max", cfg.RateLimit.Max); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExpirySec > 0, "RATE_LIMIT_EXPIRY_SEC must be positive", "rate_limit_expiry", cfg.RateLimit.ExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExpirySec <= maxRateLimitWindowSeconds,
		"RATE_LIMIT_EXPIRY_SEC must not exceed 86400",
		"rate_limit_expiry", cfg.RateLimit.ExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchMax > 0, "DISPATCH_RATE_LIMIT_MAX must be positive", "dispatch_rate_limit_max", cfg.RateLimit.DispatchMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchMax <= maxRateLimitRequestsPerWindow,
		"DISPATCH_RATE_LIMIT_MAX must not exceed 1000000",
		"dispatch_rate_limit_max", cfg.RateLimit.DispatchMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchExpirySec > 0, "DISPATCH_RATE_LIMIT_EXPIRY_SEC must be positive", "dispatch_rate_limit_expiry", cfg.RateLimit.DispatchExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchExpirySec <= maxRateLimitWindowSeconds,
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC must not exceed 86400",
		"dispatch_rate_limit_expiry", cfg.RateLimit.DispatchExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.AdminMax > 0, "ADMIN_RATE_LIMIT_MAX must be positive", "admin_rate_limit_max", cfg.RateLimit.AdminMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.AdminMax <= maxRateLimitRequestsPerWindow,
		"ADMIN_RATE_LIMIT_MAX must not exceed 1000000",
		"admin_rate_limit_max", cfg.RateLimit.AdminMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.AdminExpirySec > 0, "ADMIN_RATE_LIMIT_EXPIRY_SEC must be positive", "admin_rate_limit_expiry", cfg.RateLimit.AdminExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.AdminExpirySec <= maxRateLimitWindowSeconds,
		"ADMIN_RATE_LIMIT_EXPIRY_SEC must not exceed 86400",
		"admin_rate_limit_expiry", cfg.RateLimit.AdminExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}
