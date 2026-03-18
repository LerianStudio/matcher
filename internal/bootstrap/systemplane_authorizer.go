// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.Authorizer = (*MatcherAuthorizer)(nil)

// MatcherAuthorizer bridges the systemplane Authorizer port to Matcher's
// lib-auth-based authorization mechanism. When auth is disabled it permits
// all operations, matching the codebase convention where AUTH_ENABLED=false
// bypasses all authorization checks.
//
// Permission strings follow the systemplane convention:
//
//	"system/configs:read"  -> resource="system", action="configs:read"
//	"system/settings:write" -> resource="system", action="settings:write"
//
// These are mapped to Matcher's RBAC constants (auth.ResourceSystem,
// auth.ActionConfigRead, etc.) when auth is enabled.
type MatcherAuthorizer struct {
	authEnabled bool
}

// NewMatcherAuthorizer creates a new authorizer adapter.
// When authEnabled is false, Authorize always returns nil (permit all).
func NewMatcherAuthorizer(authEnabled bool) *MatcherAuthorizer {
	return &MatcherAuthorizer{authEnabled: authEnabled}
}

// permissionMap translates systemplane permission suffixes (the part after
// "system/") to action verbs that Matcher's RBAC model recognises. The keys
// are the "resource:action" tails of a fully-qualified systemplane permission
// string; the values are the canonical lib-auth action names.
var permissionMap = map[string]struct {
	resource string
	action   string
}{
	"configs:read":          {resource: auth.ResourceSystem, action: auth.ActionConfigRead},
	"configs:write":         {resource: auth.ResourceSystem, action: auth.ActionConfigWrite},
	"configs/schema:read":   {resource: auth.ResourceSystem, action: auth.ActionConfigSchemaRead},
	"configs/history:read":  {resource: auth.ResourceSystem, action: auth.ActionConfigHistoryRead},
	"configs/reload:write":  {resource: auth.ResourceSystem, action: auth.ActionConfigReloadWrite},
	"settings:read":         {resource: auth.ResourceSystem, action: auth.ActionSettingsRead},
	"settings:write":        {resource: auth.ResourceSystem, action: auth.ActionSettingsWrite},
	"settings/schema:read":  {resource: auth.ResourceSystem, action: auth.ActionSettingsSchemaRead},
	"settings/history:read": {resource: auth.ResourceSystem, action: auth.ActionSettingsHistoryRead},
	"settings/global:read":  {resource: auth.ResourceSystem, action: auth.ActionSettingsGlobalRead},
	"settings/global:write": {resource: auth.ResourceSystem, action: auth.ActionSettingsGlobalWrite},
}

// Authorize checks whether the current actor has the given permission.
// When auth is disabled, all requests are permitted (returns nil).
//
// Permission strings are expected in "system/<suffix>" format where <suffix>
// is one of the keys in permissionMap. Unknown permissions are denied to
// fail closed rather than silently granting access.
func (a *MatcherAuthorizer) Authorize(_ context.Context, permission string) error {
	if !a.authEnabled {
		return nil
	}

	// Parse "system/<suffix>" -> suffix
	suffix, ok := strings.CutPrefix(permission, "system/")
	if !ok {
		return fmt.Errorf("unrecognised permission %q: %w", permission, domain.ErrPermissionDenied)
	}

	if _, known := permissionMap[suffix]; !known {
		return fmt.Errorf("unrecognised permission suffix %q: %w", suffix, domain.ErrPermissionDenied)
	}

	// NOTE: In the current architecture, Matcher's lib-auth authorization is
	// performed at the HTTP middleware layer (ProtectedGroupWithMiddleware)
	// before requests reach the systemplane handler. By the time Authorize
	// is called, the request has already passed lib-auth's external RBAC
	// check. This method therefore validates the permission format and
	// confirms that auth is enabled, acting as a secondary gate.
	//
	// If the systemplane is ever invoked outside the HTTP middleware chain
	// (e.g., from a CLI or internal service call), this method should be
	// extended to perform a full RBAC evaluation.
	return nil
}
