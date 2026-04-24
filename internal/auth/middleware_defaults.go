// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package auth

import (
	"fmt"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
)

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
