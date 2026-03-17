//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScope_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		scope Scope
		want  bool
	}{
		{name: "global is valid", scope: ScopeGlobal, want: true},
		{name: "tenant is valid", scope: ScopeTenant, want: true},
		{name: "empty is invalid", scope: Scope(""), want: false},
		{name: "unknown is invalid", scope: Scope("unknown"), want: false},
		{name: "uppercase is invalid", scope: Scope("GLOBAL"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.scope.IsValid())
		})
	}
}

func TestScope_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "global", ScopeGlobal.String())
	assert.Equal(t, "tenant", ScopeTenant.String())
}

func TestParseScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Scope
		wantErr bool
	}{
		{name: "lowercase global", input: "global", want: ScopeGlobal},
		{name: "lowercase tenant", input: "tenant", want: ScopeTenant},
		{name: "uppercase GLOBAL", input: "GLOBAL", want: ScopeGlobal},
		{name: "mixed case Tenant", input: "Tenant", want: ScopeTenant},
		{name: "with whitespace", input: "  global  ", want: ScopeGlobal},
		{name: "invalid value", input: "invalid", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseScope(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidScope)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
