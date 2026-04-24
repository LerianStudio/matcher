// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllowInsecureObjectStorageEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *Config
		want bool
	}{
		{
			name: "nil config",
			cfg:  nil,
			want: false,
		},
		{
			name: "disabled flag",
			cfg: &Config{
				App: AppConfig{EnvName: envLocalName},
				ObjectStorage: ObjectStorageConfig{
					AllowInsecure: false,
				},
			},
			want: false,
		},
		{
			name: "local enabled",
			cfg: &Config{
				App: AppConfig{EnvName: envLocalName},
				ObjectStorage: ObjectStorageConfig{
					AllowInsecure: true,
				},
			},
			want: true,
		},
		{
			name: "production enabled",
			cfg: &Config{
				App: AppConfig{EnvName: envProduction},
				ObjectStorage: ObjectStorageConfig{
					AllowInsecure: true,
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := allowInsecureObjectStorageEndpoint(tt.cfg)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsAllowedInsecureObjectStorageEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "empty", env: "", want: false},
		{name: "development", env: defaultEnvName, want: true},
		{name: "dev", env: envDevShortName, want: true},
		{name: "local", env: envLocalName, want: true},
		{name: "test", env: envTestName, want: true},
		{name: "staging", env: "staging", want: false},
		{name: "production", env: envProduction, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := isAllowedInsecureObjectStorageEnvironment(tt.env)
			require.Equal(t, tt.want, got)
		})
	}
}
