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

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	"github.com/LerianStudio/matcher/internal/auth"
)

func TestMatcherIdentityResolver_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.IdentityResolver = (*MatcherIdentityResolver)(nil)
}

func TestMatcherIdentityResolver_Actor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ctx        context.Context
		expectedID string
	}{
		{
			name:       "returns user ID from context",
			ctx:        context.WithValue(context.Background(), auth.UserIDKey, "user-42"),
			expectedID: "user-42",
		},
		{
			name:       "returns anonymous when no user in context",
			ctx:        context.Background(),
			expectedID: "anonymous",
		},
		{
			name:       "returns anonymous when user ID is empty string",
			ctx:        context.WithValue(context.Background(), auth.UserIDKey, ""),
			expectedID: "anonymous",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolver := &MatcherIdentityResolver{}

			actor, err := resolver.Actor(tt.ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedID, actor.ID)
		})
	}
}

func TestMatcherIdentityResolver_TenantID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{
			name:     "returns tenant ID from context",
			ctx:      context.WithValue(context.Background(), auth.TenantIDKey, "550e8400-e29b-41d4-a716-446655440000"),
			expected: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "returns default tenant when not set in context",
			ctx:      context.Background(),
			expected: auth.DefaultTenantID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolver := &MatcherIdentityResolver{}

			tenantID, err := resolver.TenantID(tt.ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, tenantID)
		})
	}
}
