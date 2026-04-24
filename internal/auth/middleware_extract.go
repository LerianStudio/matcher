// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package auth

import (
	"context"
	"errors"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
)

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

func (te *TenantExtractor) validateTenantClaims() fiber.Handler {
	if te == nil {
		return func(c *fiber.Ctx) error {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"tenant extractor not initialized",
			)
		}
	}

	return func(fiberCtx *fiber.Ctx) error {
		if !te.authEnabled {
			return fiberCtx.Next()
		}

		if len(te.tokenSecret) == 0 {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"authentication service unavailable",
			)
		}

		token := libHTTP.ExtractTokenFromHeader(fiberCtx)
		if token == "" {
			return fiber.NewError(fiber.StatusUnauthorized, ErrMissingToken.Error())
		}

		_, _, _, err := extractClaimsFromToken(
			token,
			te.defaultTenantID,
			te.defaultTenantSlug,
			te.tokenSecret,
			te.requireTenantClaims,
		)
		if err != nil {
			if errors.Is(err, ErrMissingTenantClaim) {
				return fiber.NewError(fiber.StatusForbidden, "tenant claim required")
			}

			return fiber.NewError(fiber.StatusUnauthorized, ErrInvalidToken.Error())
		}

		return fiberCtx.Next()
	}
}
