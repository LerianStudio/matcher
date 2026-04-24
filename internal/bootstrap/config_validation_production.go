// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
)

// validateProductionConfig validates configuration constraints specific to production environments.
func (cfg *Config) validateProductionConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := cfg.validateProductionCoreConfig(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateProductionSecurityConfig(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateProductionOptionalConfig(ctx, asserter); err != nil {
		return err
	}

	return nil
}

func (cfg *Config) validateProductionCoreConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Postgres.PrimaryPassword), "POSTGRES_PASSWORD is required in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !isWellKnownDevCredential(cfg.Postgres.PrimaryPassword),
		"POSTGRES_PASSWORD must not use a well-known development default in production",
		"password_hint", "set a strong, unique password via POSTGRES_PASSWORD env var"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, strings.TrimSpace(cfg.Server.CORSAllowedOrigins) != "" && !corsContainsWildcard(cfg.Server.CORSAllowedOrigins), "CORS_ALLOWED_ORIGINS must be restricted in production (exact \"*\" not allowed)", "cors_origins", cfg.Server.CORSAllowedOrigins); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	// When rate limiting is enabled in production, TRUSTED_PROXIES must be set
	// so Fiber resolves the correct client IP. Behind an ingress that terminates
	// TLS for us (the common prod topology), c.IP() without trusted proxies
	// returns the ingress address — so every request shares one identity and
	// the rate limiter throttles the ingress itself instead of real clients.
	if cfg.RateLimit.Enabled {
		if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Server.TrustedProxies),
			"TRUSTED_PROXIES is required in production when rate limiting is enabled",
			"rate_limit_hint", "set TRUSTED_PROXIES to the CIDR(s) of your ingress/load balancer so client IPs resolve correctly"); err != nil {
			return fmt.Errorf("production config validation: %w", err)
		}
	}

	return nil
}

func (cfg *Config) validateProductionSecurityConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := asserter.That(ctx, !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.User), "guest") && !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.Password), "guest"), "RABBITMQ credentials must be set to non-default values in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !isWellKnownDevCredential(cfg.RabbitMQ.Password),
		"RABBITMQ_PASSWORD must not use a well-known development default in production",
		"password_hint", "set a strong, unique password via RABBITMQ_PASSWORD env var"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !cfg.RabbitMQ.AllowInsecureHealthCheck, "RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK must be false in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !cfg.ObjectStorage.AllowInsecure, "OBJECT_STORAGE_ALLOW_INSECURE_ENDPOINT must be false in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateInsecureObjectStoragePolicy(ctx context.Context, asserter *assert.Asserter) error {
	if cfg == nil || !cfg.ObjectStorage.AllowInsecure {
		return nil
	}

	if err := asserter.That(
		ctx,
		isAllowedInsecureObjectStorageEnvironment(cfg.App.EnvName),
		"OBJECT_STORAGE_ALLOW_INSECURE_ENDPOINT is restricted to local/development/test environments",
		"env_name",
		cfg.App.EnvName,
	); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateProductionOptionalConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Redis.Password), "REDIS_PASSWORD is required in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := cfg.validateProductionEndpoints(ctx, asserter); err != nil {
		return err
	}

	return nil
}

// validateProductionEndpoints enforces HTTPS for service URLs in production environments.
func (cfg *Config) validateProductionEndpoints(ctx context.Context, asserter *assert.Asserter) error {
	// Multi-tenant URL must use HTTPS in production — it carries tenant
	// provisioning data and service API keys.
	if multiTenantModeEnabled(cfg) && strings.TrimSpace(cfg.Tenancy.MultiTenantURL) != "" {
		if err := requireProductionHTTPS(ctx, asserter, cfg.Tenancy.MultiTenantURL, "MULTI_TENANT_URL"); err != nil {
			return err
		}
	}

	// Fetcher URL must use HTTPS in production when fetcher is enabled.
	if cfg.Fetcher.Enabled && strings.TrimSpace(cfg.Fetcher.URL) != "" {
		if err := requireProductionHTTPS(ctx, asserter, cfg.Fetcher.URL, "FETCHER_URL"); err != nil {
			return err
		}
	}

	// Object storage endpoint must use HTTPS in production when configured.
	if strings.TrimSpace(cfg.ObjectStorage.Endpoint) != "" {
		if err := requireProductionHTTPS(ctx, asserter, cfg.ObjectStorage.Endpoint, "OBJECT_STORAGE_ENDPOINT"); err != nil {
			return err
		}
	}

	return nil
}

// requireProductionHTTPS validates that rawURL uses HTTPS. The envVar name is
// used in both the assertion message and the structured key-value pair.
func requireProductionHTTPS(ctx context.Context, asserter *assert.Asserter, rawURL, envVar string) error {
	parsed, parseErr := url.Parse(strings.TrimSpace(rawURL))
	if parseErr != nil {
		return fmt.Errorf("production config validation: invalid URL %q for %s: %w", rawURL, envVar, parseErr)
	}

	if parsed == nil {
		return fmt.Errorf("%s: %w", envVar, errURLParsedToNil)
	}

	if err := asserter.That(ctx,
		strings.EqualFold(parsed.Scheme, "https"),
		envVar+" must use HTTPS in production",
		strings.ToLower(envVar), rawURL); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

// validateArchivalConfig validates archival worker configuration.
// Retention and batch validations only run when archival is enabled because
// lib-commons.SetConfigFromEnvVars does not apply envDefault tags -- fields
// default to Go zero values when env vars are absent.
func (cfg *Config) validateArchivalConfig(ctx context.Context, asserter *assert.Asserter) error {
	if !cfg.Archival.Enabled {
		return nil
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Archival.StorageBucket), "ARCHIVAL_STORAGE_BUCKET is required when ARCHIVAL_WORKER_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.HotRetentionDays > 0, "ARCHIVAL_HOT_RETENTION_DAYS must be positive", "hot_retention_days", cfg.Archival.HotRetentionDays); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.BatchSize > 0, "ARCHIVAL_BATCH_SIZE must be positive", "batch_size", cfg.Archival.BatchSize); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.PartitionLookahead > 0, "ARCHIVAL_PARTITION_LOOKAHEAD must be positive", "partition_lookahead", cfg.Archival.PartitionLookahead); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	hotAsMonths := cfg.Archival.HotRetentionDays / 30 //nolint:mnd // 30 days per month approximation

	if err := asserter.That(ctx, cfg.Archival.WarmRetentionMonths > hotAsMonths, "ARCHIVAL_WARM_RETENTION_MONTHS must be greater than ARCHIVAL_HOT_RETENTION_DAYS / 30", "warm_months", cfg.Archival.WarmRetentionMonths, "hot_as_months", hotAsMonths); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.ColdRetentionMonths >= cfg.Archival.WarmRetentionMonths, "ARCHIVAL_COLD_RETENTION_MONTHS must be >= ARCHIVAL_WARM_RETENTION_MONTHS", "cold_months", cfg.Archival.ColdRetentionMonths, "warm_months", cfg.Archival.WarmRetentionMonths); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateReportingStorageConfig(ctx context.Context, asserter *assert.Asserter) error {
	if cfg == nil || !reportingStorageRequired(cfg) {
		return nil
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.ObjectStorage.Bucket), "OBJECT_STORAGE_BUCKET is required when export or cleanup workers are enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.ObjectStorage.Endpoint), "OBJECT_STORAGE_ENDPOINT is required when export or cleanup workers are enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}
