//go:build unit

// Copyright 2025 Lerian Studio.

package ports

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubAuthorizer is a minimal test double for Authorizer.
type stubAuthorizer struct {
	allowed     map[string]bool
	defaultDeny bool
}

func newStubAuthorizer(allowed ...string) *stubAuthorizer {
	m := make(map[string]bool, len(allowed))
	for _, perm := range allowed {
		m[perm] = true
	}

	return &stubAuthorizer{allowed: m}
}

func (a *stubAuthorizer) Authorize(_ context.Context, permission string) error {
	if a.allowed[permission] {
		return nil
	}

	return domain.ErrPermissionDenied
}

// Compile-time interface check.
var _ Authorizer = (*stubAuthorizer)(nil)

func TestAuthorizer_CompileCheck(t *testing.T) {
	t.Parallel()

	var auth Authorizer = newStubAuthorizer()
	require.NotNil(t, auth)
}

func TestAuthorizer_Authorize_Allowed(t *testing.T) {
	t.Parallel()

	auth := newStubAuthorizer("systemplane:config:read", "systemplane:config:write")

	err := auth.Authorize(context.Background(), "systemplane:config:read")

	require.NoError(t, err)
}

func TestAuthorizer_Authorize_Denied(t *testing.T) {
	t.Parallel()

	auth := newStubAuthorizer("systemplane:config:read")

	err := auth.Authorize(context.Background(), "systemplane:config:write")

	require.ErrorIs(t, err, domain.ErrPermissionDenied)
}

func TestAuthorizer_Authorize_NoPermissions(t *testing.T) {
	t.Parallel()

	auth := newStubAuthorizer() // no permissions granted

	err := auth.Authorize(context.Background(), "anything")

	require.ErrorIs(t, err, domain.ErrPermissionDenied)
}

func TestAuthorizer_Authorize_MultiplePermissions(t *testing.T) {
	t.Parallel()

	perms := []string{
		"systemplane:config:read",
		"systemplane:config:write",
		"systemplane:settings:read",
	}
	auth := newStubAuthorizer(perms...)

	for _, perm := range perms {
		t.Run(perm, func(t *testing.T) {
			t.Parallel()

			err := auth.Authorize(context.Background(), perm)
			assert.NoError(t, err)
		})
	}

	// Unlisted permission should be denied.
	err := auth.Authorize(context.Background(), "systemplane:settings:write")
	require.ErrorIs(t, err, domain.ErrPermissionDenied)
}
