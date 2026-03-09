// Package auth provides authentication and multi-tenancy middleware for the Matcher service.
// It extracts tenant information from JWT tokens and manages schema-based tenant isolation.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/LerianStudio/lib-auth/v2/auth/middleware"
	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	"github.com/LerianStudio/lib-commons/v4/commons/jwt"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

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
// When authEnabled is true, tenant claims (tenant_id/tenantId) in the JWT are REQUIRED.
// Tokens without tenant claims will be rejected with ErrMissingTenantClaim.
// The envName parameter controls security features: X-User-ID header is only accepted
// in non-production environments (development, test, staging).
func NewTenantExtractor(
	authEnabled bool,
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

	// SECURITY: Treat empty envName as production-safe (reject X-User-ID headers).
	// Case-insensitive check for "production" to handle Production, PRODUCTION, etc.
	isDev := envName != "" && !strings.EqualFold(envName, "production")

	return &TenantExtractor{
		authEnabled:         authEnabled,
		requireTenantClaims: authEnabled,
		defaultTenantID:     defaultTenantID,
		defaultTenantSlug:   defaultTenantSlug,
		tokenSecret:         []byte(tokenSecret),
		isDevelopment:       isDev,
	}, nil
}

// SetDefaultTenantID sets the global default tenant ID used when no tenant is specified.
// An empty string resets to the compile-time default. Non-empty values must be valid UUIDs.
func SetDefaultTenantID(tenantID string) error {
	defaultTenantMu.Lock()
	defer defaultTenantMu.Unlock()

	if tenantID == "" {
		defaultTenantID = DefaultTenantID
		return nil
	}

	if !libCommons.IsUUID(tenantID) {
		return fmt.Errorf("set default tenant id %q: %w", tenantID, ErrInvalidTenantID)
	}

	defaultTenantID = tenantID

	return nil
}

// SetDefaultTenantSlug sets the global default tenant slug used when no tenant is specified.
// An empty string resets to the compile-time default. Non-empty values must not be whitespace-only.
func SetDefaultTenantSlug(tenantSlug string) error {
	defaultTenantMu.Lock()
	defer defaultTenantMu.Unlock()

	if tenantSlug == "" {
		defaultTenantSlug = DefaultTenantSlug
		return nil
	}

	if strings.TrimSpace(tenantSlug) == "" {
		return fmt.Errorf("set default tenant slug: %w", ErrInvalidTenantSlug)
	}

	defaultTenantSlug = tenantSlug

	return nil
}

func getDefaultTenantID() string {
	defaultTenantMu.RLock()
	defer defaultTenantMu.RUnlock()

	if defaultTenantID == "" {
		return DefaultTenantID
	}

	return defaultTenantID
}

// GetDefaultTenantID returns the current default tenant ID.
// This is the configured default tenant ID (from SetDefaultTenantID) or
// the compile-time constant DefaultTenantID if not configured.
func GetDefaultTenantID() string {
	return getDefaultTenantID()
}

func getDefaultTenantSlug() string {
	defaultTenantMu.RLock()
	defer defaultTenantMu.RUnlock()

	if defaultTenantSlug == "" {
		return DefaultTenantSlug
	}

	return defaultTenantSlug
}

// ExtractTenant returns a Fiber middleware that extracts tenant information from the request.
func (te *TenantExtractor) ExtractTenant() fiber.Handler {
	if te == nil {
		return func(c *fiber.Ctx) error {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"tenant extractor not initialized",
			)
		}
	}

	return func(fiberCtx *fiber.Ctx) error {
		ctx := fiberCtx.UserContext()
		if ctx == nil {
			ctx = context.Background()
		}

		_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
		ctx, span := tracer.Start(ctx, "middleware.extract_tenant")

		defer span.End()

		span.SetAttributes(attribute.Bool("auth.enabled", te.authEnabled))

		if !te.authEnabled {
			ctx = context.WithValue(ctx, TenantIDKey, te.defaultTenantID)
			ctx = context.WithValue(ctx, TenantSlugKey, te.defaultTenantSlug)

			span.SetAttributes(
				attribute.String("tenant.id", te.defaultTenantID),
				attribute.String("tenant.slug", te.defaultTenantSlug),
				attribute.String("auth.mode", "disabled"),
			)

			// SECURITY: Only allow X-User-ID header in non-production environments.
			// This prevents header spoofing attacks in production where auth is disabled.
			if te.isDevelopment {
				if userID := fiberCtx.Get("X-User-ID"); userID != "" {
					ctx = context.WithValue(ctx, UserIDKey, userID)
					span.SetAttributes(attribute.String("user.id", userID))
				}
			}

			fiberCtx.SetUserContext(ctx)

			return fiberCtx.Next()
		}

		if len(te.tokenSecret) == 0 {
			span.SetStatus(codes.Error, "token secret not configured")

			return fiber.NewError(
				fiber.StatusInternalServerError,
				"authentication service unavailable",
			)
		}

		token := libHTTP.ExtractTokenFromHeader(fiberCtx)
		if token == "" {
			libOpentelemetry.HandleSpanError(span, "missing authorization token", ErrMissingToken)

			return fiber.NewError(fiber.StatusUnauthorized, ErrMissingToken.Error())
		}

		tenantID, tenantSlug, userID, err := extractClaimsFromToken(
			token,
			te.defaultTenantID,
			te.defaultTenantSlug,
			te.tokenSecret,
			te.requireTenantClaims,
		)
		if err != nil {
			if errors.Is(err, ErrMissingTenantClaim) {
				libOpentelemetry.HandleSpanError(span, "missing tenant claim", err)

				return fiber.NewError(fiber.StatusForbidden, "tenant claim required")
			}

			libOpentelemetry.HandleSpanError(span, "invalid token", err)

			return fiber.NewError(fiber.StatusUnauthorized, ErrInvalidToken.Error())
		}

		ctx = context.WithValue(ctx, TenantIDKey, tenantID)
		ctx = context.WithValue(ctx, TenantSlugKey, tenantSlug)

		span.SetAttributes(
			attribute.String("tenant.id", tenantID),
			attribute.String("tenant.slug", tenantSlug),
			attribute.String("auth.mode", "jwt"),
		)

		if userID != "" {
			ctx = context.WithValue(ctx, UserIDKey, userID)
			span.SetAttributes(attribute.String("user.id", userID))
		}

		fiberCtx.SetUserContext(ctx)

		return fiberCtx.Next()
	}
}

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

// GetTenantID retrieves the tenant ID from context, returning the default if not set.
func GetTenantID(ctx context.Context) string {
	if ctx == nil {
		return getDefaultTenantID()
	}

	if tid, ok := ctx.Value(TenantIDKey).(string); ok && tid != "" {
		return tid
	}

	return getDefaultTenantID()
}

// GetTenantSlug retrieves the tenant slug from context, returning the default if not set.
func GetTenantSlug(ctx context.Context) string {
	if ctx == nil {
		return getDefaultTenantSlug()
	}

	if slug, ok := ctx.Value(TenantSlugKey).(string); ok && slug != "" {
		return slug
	}

	return getDefaultTenantSlug()
}

// GetUserID retrieves the user ID from context, returning empty string if not set.
func GetUserID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if uid, ok := ctx.Value(UserIDKey).(string); ok {
		return uid
	}

	return ""
}

// Authorize returns a Fiber middleware that checks authorization for the given resource and action.
func Authorize(authClient *middleware.AuthClient, resource, action string) fiber.Handler {
	if authClient == nil {
		// Emit observability for this programming error
		ctx := context.Background()
		asserter := assert.New(ctx, nil, constants.ApplicationName, "auth.authorize")
		_ = asserter.Never(ctx, "auth client not initialized", "resource", resource, "action", action)

		return func(c *fiber.Ctx) error {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"auth client not initialized",
			)
		}
	}

	return authClient.Authorize(constants.ApplicationName, resource, action)
}
