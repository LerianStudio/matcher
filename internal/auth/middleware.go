// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package auth provides authentication and multi-tenancy middleware for the Matcher service.
// It extracts tenant information from JWT tokens and manages schema-based tenant isolation.
package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/jwt"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

type contextKey string

// Context keys for tenant and user information.
const (
	TenantIDKey   contextKey = "tenantId"
	TenantSlugKey contextKey = "tenantSlug"
	UserIDKey     contextKey = "userId"

	DefaultTenantID   = "11111111-1111-1111-1111-111111111111"
	DefaultTenantSlug = "default"
)

// Sentinel errors for authentication failures.
var (
	ErrMissingToken       = errors.New("missing authorization token")
	ErrInvalidToken       = errors.New("invalid authorization token")
	ErrMissingTenantClaim = errors.New("missing tenant claim in token")
	ErrInvalidTenantID    = errors.New("invalid tenant ID: must be a valid UUID")
	ErrInvalidTenantSlug  = errors.New("invalid tenant slug: must not be whitespace-only")

	validSigningMethods = []string{
		jwt.AlgHS256,
		jwt.AlgHS384,
		jwt.AlgHS512,
	}
)

// defaultTenantID and defaultTenantSlug are initialized at startup via SetDefaultTenantID/SetDefaultTenantSlug.
// These values should be set once during application bootstrap and not modified during request processing.
// Tests that modify these values must use t.Cleanup to restore originals and must not use t.Parallel().
var (
	defaultTenantMu   sync.RWMutex
	defaultTenantID   = DefaultTenantID
	defaultTenantSlug = DefaultTenantSlug
)

// TenantExtractor extracts tenant information from JWT tokens in HTTP requests.
type TenantExtractor struct {
	authEnabled         bool
	requireTenantClaims bool
	defaultTenantID     string
	defaultTenantSlug   string
	tokenSecret         []byte
	isDevelopment       bool
}

// NewTenantExtractor creates a new TenantExtractor with the given configuration.
// Tenant claims (tenant_id/tenantId) in the JWT are REQUIRED only when both
// authEnabled AND multiTenantEnabled are true. In single-tenant mode (multiTenantEnabled=false),
// JWTs without tenant claims fall back to the configured defaults instead of being rejected.
// The envName parameter controls security features: X-User-ID header is only accepted
// in non-production environments (development, test, staging).
func NewTenantExtractor(
	authEnabled, multiTenantEnabled bool,
	defaultTenantID, defaultTenantSlug, tokenSecret, envName string,
) (*TenantExtractor, error) {
	ctx := context.Background()
	asserter := assert.New(ctx, nil, constants.ApplicationName, "auth.new_tenant_extractor")

	// Apply defaults
	if defaultTenantID == "" {
		defaultTenantID = DefaultTenantID
	}

	if defaultTenantSlug == "" {
		defaultTenantSlug = DefaultTenantSlug
	}

	// INVARIANT: Default tenant ID must be a valid UUID
	if err := asserter.That(
		ctx,
		libCommons.IsUUID(defaultTenantID),
		"default tenant ID must be valid UUID",
		"tenant_id", defaultTenantID,
	); err != nil {
		return nil, fmt.Errorf("invalid default tenant id: %w", err)
	}

	// INVARIANT: Default tenant slug must not be empty
	if err := asserter.NotEmpty(
		ctx,
		defaultTenantSlug,
		"default tenant slug required",
	); err != nil {
		return nil, fmt.Errorf("invalid default tenant slug: %w", err)
	}

	// INVARIANT: If auth enabled, token secret is required
	if authEnabled {
		tokenSecret = strings.TrimSpace(tokenSecret)
		if err := asserter.NotEmpty(
			ctx,
			tokenSecret,
			"token secret required when auth enabled",
		); err != nil {
			return nil, fmt.Errorf("token secret required: %w", err)
		}
	}

	// SECURITY: Only accept the X-User-ID dev header in explicit development or
	// test environments. Staging, UAT, QA, preview, and any unknown env are all
	// treated as production-adjacent because they may hold real data — allowing
	// a client to assert any user id via a plain HTTP header would be a trivial
	// impersonation vector. Match "development" or "test" case-insensitively;
	// everything else rejects.
	normalizedEnv := strings.ToLower(strings.TrimSpace(envName))
	isDev := normalizedEnv == "development" || normalizedEnv == "test"

	return &TenantExtractor{
		authEnabled:         authEnabled,
		requireTenantClaims: authEnabled && multiTenantEnabled,
		defaultTenantID:     defaultTenantID,
		defaultTenantSlug:   defaultTenantSlug,
		tokenSecret:         []byte(tokenSecret),
		isDevelopment:       isDev,
	}, nil
}
