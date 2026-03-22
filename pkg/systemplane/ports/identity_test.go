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

// stubIdentityResolver is a minimal test double for IdentityResolver.
type stubIdentityResolver struct {
	actor     domain.Actor
	actorErr  error
	tenant    string
	tenantErr error
}

func (s *stubIdentityResolver) Actor(_ context.Context) (domain.Actor, error) {
	return s.actor, s.actorErr
}

func (s *stubIdentityResolver) TenantID(_ context.Context) (string, error) {
	return s.tenant, s.tenantErr
}

// Compile-time interface check.
var _ IdentityResolver = (*stubIdentityResolver)(nil)

func TestIdentityResolver_CompileCheck(t *testing.T) {
	t.Parallel()

	// This test exists to document that stubIdentityResolver satisfies the
	// IdentityResolver interface. The compile-time check above is the
	// primary assertion; this function ensures it stays in the test binary.
	var resolver IdentityResolver = &stubIdentityResolver{}
	require.NotNil(t, resolver)
}

func TestIdentityResolver_Actor_ReturnsExpectedValues(t *testing.T) {
	t.Parallel()

	resolver := &stubIdentityResolver{
		actor: domain.Actor{ID: "user-42"},
	}

	actor, err := resolver.Actor(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "user-42", actor.ID)
}

func TestIdentityResolver_Actor_ReturnsError(t *testing.T) {
	t.Parallel()

	wantErr := assert.AnError
	resolver := &stubIdentityResolver{actorErr: wantErr}

	_, err := resolver.Actor(context.Background())

	require.ErrorIs(t, err, wantErr)
}

func TestIdentityResolver_TenantID_ReturnsExpectedValues(t *testing.T) {
	t.Parallel()

	resolver := &stubIdentityResolver{tenant: "tenant-abc"}

	tenant, err := resolver.TenantID(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "tenant-abc", tenant)
}

func TestIdentityResolver_TenantID_ReturnsError(t *testing.T) {
	t.Parallel()

	wantErr := assert.AnError
	resolver := &stubIdentityResolver{tenantErr: wantErr}

	_, err := resolver.TenantID(context.Background())

	require.ErrorIs(t, err, wantErr)
}
