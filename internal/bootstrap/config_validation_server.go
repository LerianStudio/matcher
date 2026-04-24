// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
)

// validateServerConfig validates server and middleware configuration.
func (cfg *Config) validateServerConfig(ctx context.Context, asserter *assert.Asserter) error {
	if err := asserter.That(ctx, (strings.TrimSpace(cfg.Server.TLSCertFile) == "") == (strings.TrimSpace(cfg.Server.TLSKeyFile) == ""), "SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := cfg.validateAuthConfig(ctx, asserter); err != nil {
		return err
	}

	if err := asserter.That(ctx, libCommons.IsUUID(cfg.Tenancy.DefaultTenantID), "DEFAULT_TENANT_ID must be a valid UUID", "tenant_id", cfg.Tenancy.DefaultTenantID); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := cfg.validateTenancyConfig(ctx, asserter); err != nil {
		return err
	}

	if err := asserter.That(ctx, cfg.Server.BodyLimitBytes > 0, "HTTP_BODY_LIMIT_BYTES must be positive", "body_limit", cfg.Server.BodyLimitBytes); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := cfg.validateTimeoutBoundaries(ctx, asserter); err != nil {
		return err
	}

	if err := validateTrustedProxies(ctx, asserter, cfg.Server.TrustedProxies); err != nil {
		return err
	}

	if err := cfg.validateLogLevel(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateDeploymentMode(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateTelemetryConfig(ctx, asserter); err != nil {
		return err
	}

	return nil
}

func (cfg *Config) validateTimeoutBoundaries(ctx context.Context, asserter *assert.Asserter) error {
	if err := asserter.That(ctx, cfg.Postgres.ConnectTimeoutSec >= 0, "PostgresConnectTimeoutSec must be non-negative", "postgres_connect_timeout_sec", cfg.Postgres.ConnectTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Postgres.QueryTimeoutSec >= 0, "PostgresQueryTimeoutSec must be non-negative", "postgres_query_timeout_sec", cfg.Postgres.QueryTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Webhook.TimeoutSec >= 0, "WEBHOOK_TIMEOUT_SEC must be non-negative (see Config.WebhookTimeout() for runtime defaulting/capping)", "webhook_timeout_sec", cfg.Webhook.TimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Infrastructure.ConnectTimeoutSec > 0, "InfraConnectTimeoutSec must be positive", "infra_connect_timeout_sec", cfg.Infrastructure.ConnectTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateLogLevel(ctx context.Context, asserter *assert.Asserter) error {
	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
		"fatal": true,
	}

	logLevel := strings.ToLower(strings.TrimSpace(cfg.App.LogLevel))
	_, validLogLevel := validLogLevels[logLevel]

	if err := asserter.That(ctx, validLogLevel, "LOG_LEVEL must be one of: debug, info, warn, error, fatal", "log_level", cfg.App.LogLevel); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateDeploymentMode(ctx context.Context, asserter *assert.Asserter) error {
	mode := strings.ToLower(strings.TrimSpace(cfg.App.Mode))
	if mode == "" {
		return nil
	}

	validModes := map[string]bool{
		deploymentModeSaaS:  true,
		deploymentModeByoc:  true,
		deploymentModeLocal: true,
	}

	if err := asserter.That(ctx, validModes[mode], "DEPLOYMENT_MODE must be one of: saas, byoc, local", "deployment_mode", cfg.App.Mode); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateTelemetryConfig(ctx context.Context, asserter *assert.Asserter) error {
	if !cfg.Telemetry.Enabled {
		return nil
	}

	validOtelEnvs := map[string]bool{"development": true, "staging": true, "production": true}

	otelEnv := strings.ToLower(strings.TrimSpace(cfg.Telemetry.DeploymentEnv))
	_, validOtelEnv := validOtelEnvs[otelEnv]

	if err := asserter.That(ctx, validOtelEnv, "OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT must be one of: development, staging, production", "otel_env", cfg.Telemetry.DeploymentEnv); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}
