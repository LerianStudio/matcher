// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestInitGovernanceModule_Signature locks the signature of
// initGovernanceModule after the T-018 bootstrap split. Behaviour is covered
// end-to-end by the Service-level tests in init_test.go; this file exists so
// init_governance.go has a companion _test.go as required by scripts/check-tests.sh.
func TestInitGovernanceModule_Signature(t *testing.T) {
	t.Parallel()

	var _ func(*Routes, *sharedRepositories, sharedPorts.InfrastructureProvider, bool) error = initGovernanceModule
}
