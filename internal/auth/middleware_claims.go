// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package auth

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/jwt"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

func claimString(claims jwt.MapClaims, keys ...string) (string, bool, error) {
	for _, key := range keys {
		value, ok := claims[key]
		if !ok {
			continue
		}

		stringValue, ok := value.(string)
		if !ok {
			return "", true, ErrInvalidToken
		}

		return stringValue, true, nil
	}

	return "", false, nil
}

func expirationFromClaims(claims jwt.MapClaims) (time.Time, bool) {
	expValue, ok := claims["exp"]
	if !ok {
		return time.Time{}, false
	}

	switch value := expValue.(type) {
	case float64:
		return time.Unix(int64(value), 0), true
	case int64:
		return time.Unix(value, 0), true
	case int32:
		return time.Unix(int64(value), 0), true
	case int:
		return time.Unix(int64(value), 0), true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return time.Time{}, false
		}

		return time.Unix(parsed, 0), true
	default:
		return time.Time{}, false
	}
}

func nbfFromClaims(claims jwt.MapClaims) (time.Time, bool) {
	nbfValue, ok := claims["nbf"]
	if !ok {
		return time.Time{}, false
	}

	switch value := nbfValue.(type) {
	case float64:
		return time.Unix(int64(value), 0), true
	case int64:
		return time.Unix(value, 0), true
	case int32:
		return time.Unix(int64(value), 0), true
	case int:
		return time.Unix(int64(value), 0), true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return time.Time{}, false
		}

		return time.Unix(parsed, 0), true
	default:
		return time.Time{}, false
	}
}

func parseTokenClaims(tokenString string, tokenSecret []byte) (jwt.MapClaims, error) {
	if len(tokenSecret) == 0 {
		return nil, ErrInvalidToken
	}

	token, err := jwt.ParseAndValidate(tokenString, tokenSecret, validSigningMethods)
	if err != nil || token == nil || !token.SignatureValid {
		return nil, ErrInvalidToken
	}

	return token.Claims, nil
}

func extractTenantFromClaims(
	claims jwt.MapClaims,
	defaultTenantID, defaultTenantSlug string,
	requireTenantClaims bool,
) (tenantID, tenantSlug string, err error) {
	ctx := context.Background()
	asserter := assert.New(ctx, nil, constants.ApplicationName, "auth.extract_tenant_claims")

	tenantIDValue, hasTenantID, err := claimString(claims, "tenant_id", "tenantId")
	if err != nil {
		_ = asserter.Never(ctx, "tenant_id claim is not a string type")

		return "", "", ErrInvalidToken
	}

	tenantIDValue = strings.TrimSpace(tenantIDValue)
	hasTenantID = hasTenantID && tenantIDValue != ""

	if !hasTenantID {
		if requireTenantClaims {
			return "", "", ErrMissingTenantClaim
		}

		return defaultTenantID, defaultTenantSlug, nil
	}

	// INVARIANT: Tenant ID in JWT must be a valid UUID (security monitoring)
	if err := asserter.That(
		ctx,
		libCommons.IsUUID(tenantIDValue),
		"tenant ID in JWT must be valid UUID",
		"tenant_id", tenantIDValue,
		"claim_present", hasTenantID,
	); err != nil {
		return "", "", ErrInvalidToken
	}

	tenantID = tenantIDValue
	tenantSlug = defaultTenantSlug

	tenantSlugValue, _, err := claimString(claims, "tenant_slug", "tenantSlug")
	if err != nil {
		_ = asserter.Never(ctx, "tenant_slug claim is not a string type")

		return "", "", ErrInvalidToken
	}

	tenantSlugValue = strings.TrimSpace(tenantSlugValue)
	if tenantSlugValue != "" {
		tenantSlug = tenantSlugValue
	}

	return tenantID, tenantSlug, nil
}

func extractClaimsFromToken(
	tokenString, defaultTenantID, defaultTenantSlug string,
	tokenSecret []byte,
	requireTenantClaims bool,
) (tenantID, tenantSlug, userID string, err error) {
	ctx := context.Background()
	asserter := assert.New(ctx, nil, constants.ApplicationName, "auth.extract_claims")

	claims, err := parseTokenClaims(tokenString, tokenSecret)
	if err != nil {
		_ = asserter.Never(
			ctx,
			"JWT token parsing failed",
			"error", err.Error(),
		)

		return "", "", "", ErrInvalidToken
	}

	// INVARIANT: Token must have valid expiration
	expiration, ok := expirationFromClaims(claims)
	now := time.Now().UTC()
	isExpired := !ok || now.After(expiration)

	if err := asserter.That(
		ctx,
		!isExpired,
		"JWT token must not be expired",
		"has_expiration", ok,
		"expiration", expiration.Format(time.RFC3339),
		"current_time", now.Format(time.RFC3339),
	); err != nil {
		return "", "", "", ErrInvalidToken
	}

	// INVARIANT: Token must not be used before its nbf time
	if nbfTime, hasNBF := nbfFromClaims(claims); hasNBF {
		if now.Before(nbfTime) {
			_ = asserter.Never(ctx, "JWT token used before nbf time",
				"nbf", nbfTime.Format(time.RFC3339),
				"current_time", now.Format(time.RFC3339),
			)

			return "", "", "", ErrInvalidToken
		}
	}

	tenantID, tenantSlug, err = extractTenantFromClaims(
		claims,
		defaultTenantID,
		defaultTenantSlug,
		requireTenantClaims,
	)
	if err != nil {
		return "", "", "", err
	}

	userIDValue, hasUserID, err := claimString(claims, "sub")
	if err != nil {
		_ = asserter.Never(ctx, "JWT 'sub' claim is not a string")

		return "", "", "", ErrInvalidToken
	}

	if hasUserID {
		userID = strings.TrimSpace(userIDValue)
	}

	return tenantID, tenantSlug, userID, nil
}
