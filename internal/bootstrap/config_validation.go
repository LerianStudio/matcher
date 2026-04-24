// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// errURLParsedToNil is returned when url.Parse succeeds but yields a nil *url.URL.
var errURLParsedToNil = errors.New("production config validation: URL parsed to nil")

const (
	maxRateLimitRequestsPerWindow = 1_000_000
	maxRateLimitWindowSeconds     = 86_400
)

// wellKnownDevCredentials lists passwords that are known development defaults.
// These pass the not-empty and not-guest checks but must NEVER be used in production.
// This list is intentionally kept in source code as a safety net — if a credential
// appears here, production validation will reject it.
var wellKnownDevCredentials = []string{
	"matcher_dev_password",
	"password",
	"changeme",
	"secret",
}

// isWellKnownDevCredential returns true if the given value matches a known development default.
func isWellKnownDevCredential(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	for _, blocked := range wellKnownDevCredentials {
		if lower == blocked {
			return true
		}
	}

	return false
}

// Validate checks the configuration for required fields and production constraints.
func (cfg *Config) Validate() error {
	return cfg.validateWithContext(context.Background())
}

func (cfg *Config) validateWithContext(ctx context.Context) error {
	asserter := newConfigAsserter(ctx, "config.validate")

	if err := asserter.NotNil(ctx, cfg, "config must be provided"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if IsProductionEnvironment(cfg.App.EnvName) {
		if err := cfg.validateProductionConfig(ctx, asserter); err != nil {
			return err
		}
	}

	if err := cfg.validateServerConfig(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateRateLimitConfig(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateArchivalConfig(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateReportingStorageConfig(ctx, asserter); err != nil {
		return err
	}

	if err := cfg.validateInsecureObjectStoragePolicy(ctx, asserter); err != nil {
		return err
	}

	return nil
}

// corsContainsWildcard returns true if the comma-separated origin list contains
// an exact "*" entry. Subdomain wildcards like "https://*.example.com" are allowed.
func corsContainsWildcard(origins string) bool {
	for _, entry := range strings.Split(origins, ",") {
		if strings.TrimSpace(entry) == "*" {
			return true
		}
	}

	return false
}

func newConfigAsserter(ctx context.Context, operation string) *assert.Asserter {
	return assert.New(ctx, nil, constants.ApplicationName, operation)
}
