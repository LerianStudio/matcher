// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestInitMatchingModule_Signature locks the signature of initMatchingModule
// after the T-018 bootstrap split. Behaviour is exercised by the Service-level
// tests in init_test.go (TestInitMatchingModule_* variants); this file exists
// so init_matching.go has a companion _test.go as required by
// scripts/check-tests.sh.
func TestInitMatchingModule_Signature(t *testing.T) {
	t.Parallel()

	var _ func(
		*Routes,
		sharedPorts.InfrastructureProvider,
		sharedPorts.OutboxRepository,
		*sharedRepositories,
		bool,
	) (*matchingCommand.UseCase, error) = initMatchingModule
}
