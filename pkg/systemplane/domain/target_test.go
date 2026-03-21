//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTarget_ValidGlobal(t *testing.T) {
	t.Parallel()

	target, err := NewTarget(KindConfig, ScopeGlobal, "")

	require.NoError(t, err)
	assert.Equal(t, KindConfig, target.Kind)
	assert.Equal(t, ScopeGlobal, target.Scope)
	assert.Equal(t, "", target.SubjectID)
}

func TestNewTarget_ValidTenant(t *testing.T) {
	t.Parallel()

	target, err := NewTarget(KindSetting, ScopeTenant, "tenant-abc-123")

	require.NoError(t, err)
	assert.Equal(t, KindSetting, target.Kind)
	assert.Equal(t, ScopeTenant, target.Scope)
	assert.Equal(t, "tenant-abc-123", target.SubjectID)
}

func TestNewTarget_TenantScopeEmptySubjectID(t *testing.T) {
	t.Parallel()

	_, err := NewTarget(KindConfig, ScopeTenant, "")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrScopeInvalid)
}

func TestNewTarget_GlobalScopeWithSubjectID(t *testing.T) {
	t.Parallel()

	_, err := NewTarget(KindConfig, ScopeGlobal, "should-be-empty")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrScopeInvalid)
}

func TestNewTarget_InvalidKind(t *testing.T) {
	t.Parallel()

	_, err := NewTarget(Kind("bogus"), ScopeGlobal, "")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidKind)
}

func TestNewTarget_InvalidScope(t *testing.T) {
	t.Parallel()

	_, err := NewTarget(KindConfig, Scope("bogus"), "")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidScope)
}

func TestTarget_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target Target
		want   string
	}{
		{
			name:   "global config",
			target: Target{Kind: KindConfig, Scope: ScopeGlobal},
			want:   "config/global",
		},
		{
			name:   "global setting",
			target: Target{Kind: KindSetting, Scope: ScopeGlobal},
			want:   "setting/global",
		},
		{
			name:   "tenant setting",
			target: Target{Kind: KindSetting, Scope: ScopeTenant, SubjectID: "t-42"},
			want:   "setting/tenant/t-42",
		},
		{
			name:   "tenant config",
			target: Target{Kind: KindConfig, Scope: ScopeTenant, SubjectID: "uuid-here"},
			want:   "config/tenant/uuid-here",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.target.String())
		})
	}
}
