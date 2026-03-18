// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func TestMatcherAuthorizer_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.Authorizer = (*MatcherAuthorizer)(nil)
}

func TestNewMatcherAuthorizer(t *testing.T) {
	t.Parallel()

	t.Run("enabled", func(t *testing.T) {
		t.Parallel()

		a := NewMatcherAuthorizer(true)
		require.NotNil(t, a)
		assert.True(t, a.authEnabled)
	})

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()

		a := NewMatcherAuthorizer(false)
		require.NotNil(t, a)
		assert.False(t, a.authEnabled)
	})
}

func TestMatcherAuthorizer_AuthDisabled_AllowsAll(t *testing.T) {
	t.Parallel()

	authorizer := NewMatcherAuthorizer(false)
	ctx := context.Background()

	permissions := []string{
		"system/configs:read",
		"system/configs:write",
		"system/settings:read",
		"system/settings:write",
		"system/settings/global:read",
		"system/settings/global:write",
		"completely/unknown:permission",
		"",
	}

	for _, perm := range permissions {
		t.Run("permits_"+perm, func(t *testing.T) {
			t.Parallel()

			err := authorizer.Authorize(ctx, perm)
			assert.NoError(t, err, "auth-disabled authorizer must permit all requests")
		})
	}
}

func TestMatcherAuthorizer_AuthEnabled_ValidPermissions(t *testing.T) {
	t.Parallel()

	authorizer := NewMatcherAuthorizer(true)
	ctx := context.Background()

	validPermissions := []string{
		"system/configs:read",
		"system/configs:write",
		"system/configs/schema:read",
		"system/configs/history:read",
		"system/configs/reload:write",
		"system/settings:read",
		"system/settings:write",
		"system/settings/schema:read",
		"system/settings/history:read",
		"system/settings/global:read",
		"system/settings/global:write",
	}

	for _, perm := range validPermissions {
		t.Run(perm, func(t *testing.T) {
			t.Parallel()

			err := authorizer.Authorize(ctx, perm)
			assert.NoError(t, err, "known permission %q must be allowed when auth is enabled", perm)
		})
	}
}

func TestMatcherAuthorizer_AuthEnabled_DeniedPermissions(t *testing.T) {
	t.Parallel()

	authorizer := NewMatcherAuthorizer(true)
	ctx := context.Background()

	tests := []struct {
		name       string
		permission string
	}{
		{
			name:       "unknown suffix",
			permission: "system/unknown:action",
		},
		{
			name:       "missing system prefix",
			permission: "configs:read",
		},
		{
			name:       "empty string",
			permission: "",
		},
		{
			name:       "wrong resource prefix",
			permission: "other/configs:read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := authorizer.Authorize(ctx, tt.permission)
			require.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrPermissionDenied,
				"unknown permission %q must return ErrPermissionDenied", tt.permission)
		})
	}
}
