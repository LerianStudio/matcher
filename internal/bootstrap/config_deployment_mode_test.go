// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfigValidate_DeploymentModeAccepted covers the full matrix of
// accepted and rejected DEPLOYMENT_MODE values. Accepted: saas, byoc, local,
// empty (no-op), and case-insensitive variants. Rejected: everything else.
func TestConfigValidate_DeploymentModeAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mode        string
		expectError bool
	}{
		{name: "saas_is_accepted", mode: "saas", expectError: false},
		{name: "byoc_is_accepted", mode: "byoc", expectError: false},
		{name: "local_is_accepted", mode: "local", expectError: false},
		{name: "empty_is_noop", mode: "", expectError: false},
		{name: "uppercase_saas_is_accepted", mode: "SAAS", expectError: false},
		{name: "mixed_case_byoc_is_accepted", mode: "Byoc", expectError: false},
		{name: "dev_is_rejected", mode: "dev", expectError: true},
		{name: "prod_is_rejected", mode: "prod", expectError: true},
		{name: "hybrid_is_rejected", mode: "hybrid", expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultConfig()
			cfg.App.Mode = tt.mode

			err := cfg.Validate()
			if tt.expectError {
				require.Error(t, err)
				require.ErrorContains(t, err, "DEPLOYMENT_MODE must be one of: saas, byoc, local")

				return
			}

			require.NoError(t, err)
		})
	}
}
