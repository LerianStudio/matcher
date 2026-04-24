// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestInitIngestionModule_Signature locks the signature of initIngestionModule
// after the T-018 bootstrap split. Behaviour is exercised by the Service-level
// tests in init_test.go; this file exists so init_ingestion.go has a companion
// _test.go as required by scripts/check-tests.sh.
func TestInitIngestionModule_Signature(t *testing.T) {
	t.Parallel()

	var _ func(
		*Config,
		func() *Config,
		*runtimeSettingsResolver,
		*Routes,
		sharedPorts.InfrastructureProvider,
		sharedPorts.OutboxRepository,
		sharedPorts.IngestionEventPublisher,
		*matchingCommand.UseCase,
		*sharedRepositories,
		bool,
	) (*ingestionCommand.UseCase, error) = initIngestionModule
}
