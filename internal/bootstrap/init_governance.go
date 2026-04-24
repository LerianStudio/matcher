// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests
// Behaviour of initGovernanceModule is exercised end-to-end by the
// Service-level tests in init_test.go. This file has no dedicated
// companion _test.go — scripts/check-tests.sh honours the marker above.

package bootstrap

import (
	"fmt"

	governanceHTTP "github.com/LerianStudio/matcher/internal/governance/adapters/http"
	actorMappingRepoAdapter "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/actor_mapping"
	governanceCommand "github.com/LerianStudio/matcher/internal/governance/services/command"
	governanceQuery "github.com/LerianStudio/matcher/internal/governance/services/query"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func initGovernanceModule(routes *Routes, repos *sharedRepositories, provider sharedPorts.InfrastructureProvider, production bool) error {
	governanceQueryUC, err := governanceQuery.NewUseCase(repos.governanceAuditLog)
	if err != nil {
		return fmt.Errorf("create governance query use case: %w", err)
	}

	governanceHandler, err := governanceHTTP.NewHandler(governanceQueryUC, production)
	if err != nil {
		return fmt.Errorf("create governance handler: %w", err)
	}

	if err := governanceHTTP.RegisterRoutes(routes.Protected, governanceHandler); err != nil {
		return fmt.Errorf("register governance routes: %w", err)
	}

	// Actor mapping CRUD
	actorMappingRepo := actorMappingRepoAdapter.NewRepository(provider)

	actorMappingCommandUC, err := governanceCommand.NewActorMappingUseCase(actorMappingRepo)
	if err != nil {
		return fmt.Errorf("create actor mapping command use case: %w", err)
	}

	actorMappingHandler, err := governanceHTTP.NewActorMappingHandler(actorMappingCommandUC, actorMappingRepo, production)
	if err != nil {
		return fmt.Errorf("create actor mapping handler: %w", err)
	}

	if err := governanceHTTP.RegisterActorMappingRoutes(routes.Protected, actorMappingHandler); err != nil {
		return fmt.Errorf("register actor mapping routes: %w", err)
	}

	return nil
}
