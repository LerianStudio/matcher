//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackendKind_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind BackendKind
		want bool
	}{
		{name: "postgres is valid", kind: BackendPostgres, want: true},
		{name: "mongodb is valid", kind: BackendMongoDB, want: true},
		{name: "empty is invalid", kind: BackendKind(""), want: false},
		{name: "unknown is invalid", kind: BackendKind("unknown"), want: false},
		{name: "uppercase is invalid", kind: BackendKind("POSTGRES"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.kind.IsValid())
		})
	}
}

func TestBackendKind_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "postgres", BackendPostgres.String())
	assert.Equal(t, "mongodb", BackendMongoDB.String())
}

func TestParseBackendKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    BackendKind
		wantErr bool
	}{
		{name: "lowercase postgres", input: "postgres", want: BackendPostgres},
		{name: "lowercase mongodb", input: "mongodb", want: BackendMongoDB},
		{name: "uppercase POSTGRES", input: "POSTGRES", want: BackendPostgres},
		{name: "mixed case MongoDB", input: "MongoDB", want: BackendMongoDB},
		{name: "with whitespace", input: "  postgres  ", want: BackendPostgres},
		{name: "invalid value", input: "invalid", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseBackendKind(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidBackendKind)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
