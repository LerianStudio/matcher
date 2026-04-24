// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Sentinel errors for HTTP helpers.
var (
	ErrMissingParameter = errors.New("missing parameter")
	ErrTenantIDNotFound = errors.New("tenant ID not found in context")
)

func parseUUIDParam(c *fiber.Ctx, name string) (uuid.UUID, error) {
	value := c.Params(name)
	if value == "" {
		return uuid.Nil, fmt.Errorf("%w: %s", ErrMissingParameter, name)
	}

	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid UUID parameter %s: %w", name, err)
	}

	return id, nil
}

func tenantIDFromContext(ctx context.Context) (uuid.UUID, error) {
	tenantID := auth.GetTenantID(ctx)
	if tenantID == "" {
		return uuid.Nil, ErrTenantIDNotFound
	}

	parsed, err := uuid.Parse(tenantID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid tenant ID: %w", err)
	}

	return parsed, nil
}
