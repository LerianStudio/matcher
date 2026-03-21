//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKind_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind Kind
		want bool
	}{
		{name: "config is valid", kind: KindConfig, want: true},
		{name: "setting is valid", kind: KindSetting, want: true},
		{name: "empty is invalid", kind: Kind(""), want: false},
		{name: "unknown is invalid", kind: Kind("unknown"), want: false},
		{name: "uppercase is invalid", kind: Kind("CONFIG"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.kind.IsValid())
		})
	}
}

func TestKind_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "config", KindConfig.String())
	assert.Equal(t, "setting", KindSetting.String())
}

func TestParseKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Kind
		wantErr bool
	}{
		{name: "lowercase config", input: "config", want: KindConfig},
		{name: "lowercase setting", input: "setting", want: KindSetting},
		{name: "uppercase CONFIG", input: "CONFIG", want: KindConfig},
		{name: "mixed case Setting", input: "Setting", want: KindSetting},
		{name: "with whitespace", input: "  config  ", want: KindConfig},
		{name: "invalid value", input: "invalid", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseKind(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidKind)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
